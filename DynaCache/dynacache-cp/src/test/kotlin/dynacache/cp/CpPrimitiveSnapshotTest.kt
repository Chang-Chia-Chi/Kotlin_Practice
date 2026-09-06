package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Command.Ttl.Precision.MILLIS
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * CP spec 10.7 at the state machine, with no Raft group: a table survives the bytes its own
 * primitive writes, and the composite's snapshot carries every one of them. Each case builds
 * state on one instance, restores it into a second instance and asks that one what it holds, so
 * a field left out of an encoding shows up as a wrong answer rather than as a byte count nobody
 * reads.
 */
class CpPrimitiveSnapshotTest {

    private val now = 1_788_656_400_001L
    private val hour = Duration.ofHours(1)

    /** [into], after this primitive's own snapshot bytes have been read into it. */
    private fun <P : CpPrimitive> P.restoredInto(into: P): P = into.also { it.restore(snapshot()) }

    @Test
    fun counters_survive_their_own_bytes() {
        val key = Key("cp:counter:c")
        val forever = Key("cp:counter:f")
        val machine = AtomicLongStateMachine()
        machine.apply(Command.Cp.LongSet(key, 7, ttl = hour), now)
        machine.apply(Command.Cp.LongSet(forever, -3), now)

        val restored = machine.restoredInto(AtomicLongStateMachine())

        assertEquals(Reply.Integer(7), restored.apply(Command.Cp.LongGet(key), now))
        assertEquals(Reply.Integer(-3), restored.apply(Command.Cp.LongGet(forever), now))
        assertEquals(Reply.Integer(hour.toMillis()), restored.apply(Command.Cp.LongTtl(key, MILLIS), now))
        assertEquals(Reply.Integer(-1), restored.apply(Command.Cp.LongTtl(forever, MILLIS), now), "no TTL, not a lost one")
    }

    /** C17: the token is state, so it has to cross the bytes with the holder and the lease. */
    @Test
    fun locks_survive_their_own_bytes() {
        val key = Key("cp:lock:l")
        val machine = FencedLockStateMachine()
        machine.apply(Command.Cp.LockTry(key, session = 3, lease = hour), now)
        machine.apply(Command.Cp.LockTry(key, session = 3, lease = hour), now)

        val restored = machine.restoredInto(FencedLockStateMachine())

        assertEquals(
            Reply.Array(listOf(Reply.Integer(3), Reply.Integer(1), Reply.Integer(hour.toMillis()), Reply.Integer(2))),
            restored.apply(Command.Cp.LockState(key), now),
            "holder, token, what is left of the lease and the reentrant hold count",
        )
    }

    @Test
    fun semaphores_survive_their_own_bytes() {
        val key = Key("cp:sem:s")
        val machine = SemaphoreStateMachine()
        machine.apply(Command.Cp.SemInit(key, 3))
        machine.apply(Command.Cp.SemAcquire(key, session = 3, permits = 2))

        val restored = machine.restoredInto(SemaphoreStateMachine())

        assertEquals(Reply.Integer(1), restored.apply(Command.Cp.SemAvailable(key)))
        assertEquals(
            Reply.Simple("OK"),
            restored.apply(Command.Cp.SemRelease(key, session = 3, permits = 2)),
            "who holds what came too, or this release would be more than the session holds",
        )
        assertEquals(Reply.Integer(3), restored.apply(Command.Cp.SemAvailable(key)))
    }

    @Test
    fun latches_survive_their_own_bytes() {
        val key = Key("cp:latch:l")
        val machine = CountDownLatchStateMachine()
        machine.apply(Command.Cp.LatchSet(key, 5))
        machine.apply(Command.Cp.LatchDown(key))

        val restored = machine.restoredInto(CountDownLatchStateMachine())

        assertEquals(Reply.Integer(4), restored.apply(Command.Cp.LatchGet(key)))
    }

    @Test
    fun references_survive_their_own_bytes() {
        val key = Key("cp:ref:r")
        val bytes = byteArrayOf(0, 127, -1)
        val machine = AtomicReferenceStateMachine()
        machine.apply(Command.Cp.RefSet(key, bytes, ttl = hour), now)

        val restored = machine.restoredInto(AtomicReferenceStateMachine())

        assertEquals(Reply.Bulk(bytes), restored.apply(Command.Cp.RefGet(key), now), "every byte, zero and sign bit alike")
        assertEquals(Reply.Integer(hour.toMillis()), restored.apply(Command.Cp.RefTtl(key, MILLIS), now))
    }

    /** CP spec 4: which sessions are alive, when each last spoke, and the id the next one gets. */
    @Test
    fun sessions_survive_their_own_bytes() {
        val registry = SessionRegistry()
        registry.apply(Command.Cp.SessionCreate(Duration.ofSeconds(15)), now)
        registry.apply(Command.Cp.SessionCreate(Duration.ofSeconds(30)), now)
        registry.close(1)

        val restored = registry.restoredInto(SessionRegistry())

        assertFalse(restored.isAlive(1), "a closed session stays closed")
        assertTrue(restored.isAlive(2))
        assertEquals(emptyList<Long>(), restored.lapsed(now + 20_000), "session 2 kept its own 30s timeout")
        assertEquals(listOf(2L), restored.lapsed(now + 31_000), "and its last heartbeat, so it lapses when it should")
        assertEquals(
            Reply.Integer(3),
            restored.apply(Command.Cp.SessionCreate(Duration.ofSeconds(15)), now),
            "ids climb from where the snapshot left off, never reissuing 2",
        )
    }

    /**
     * The composite's own round trip: every primitive's table crosses in one snapshot, log time
     * with them, and the session-close cascade still reaches across primitives afterwards (C18,
     * I15) - the restored machine has the sessions, the locks and the permits to cascade over.
     */
    @Test
    fun composite_snapshot_restores_every_primitive() {
        val lock = Key("cp:lock:l")
        val semaphore = Key("cp:sem:s")
        val primitives = Primitives()
        val session = (primitives.apply(Command.Cp.SessionCreate(Duration.ofSeconds(15))) as Reply.Integer).value
        primitives.apply(Command.Cp.LongSet(Key("cp:counter:c"), 7))
        primitives.apply(Command.Cp.LockTry(lock, session, lease = hour))
        primitives.apply(Command.Cp.SemInit(semaphore, 3))
        primitives.apply(Command.Cp.SemAcquire(semaphore, session, 2))
        primitives.apply(Command.Cp.LatchSet(Key("cp:latch:l"), 5))
        primitives.apply(Command.Cp.RefSet(Key("cp:ref:r"), "bytes".toByteArray()))
        val taken = primitives.stateMachine.state

        val restored = CpStateMachine()
        restored.installSnapshot(commitIndex = 1, chunks = listOf(taken))

        assertEquals(taken, restored.state, "log time and one table per primitive")
        assertEquals(6, taken.tables.size, "one table per primitive, none of them read by the composite")
        assertEquals(Reply.Integer(7), restored.read(Command.Cp.LongGet(Key("cp:counter:c"))))

        restored.runOperation(2, SessionClosed(taken.lastAppliedTs + 1, session))

        assertEquals(Reply.Integer(3), restored.read(Command.Cp.SemAvailable(semaphore)), "its permits came back")
        assertEquals(
            Reply.Array(listOf(Reply.Bulk(null), Reply.Integer(1), Reply.Integer(0), Reply.Integer(0))),
            restored.read(Command.Cp.LockState(lock)),
            "its lock was released, and the token it was granted stayed behind (C17)",
        )
    }
}
