package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.TimeoutException

/**
 * The four invariant tests of CP spec 10.9 under chaos, plus availability under minority and
 * majority failure (I16, I17). The chaos ones run over five seeds; a run kills the leader, kills
 * followers, restarts them (once from the file store), and partitions the transport throughout.
 */
class ChaosInvariantTest {

    @TempDir
    lateinit var dir: Path

    /** I14: every fresh acquire of a lock returns a token strictly greater than the last, through it all. */
    @ParameterizedTest
    @ValueSource(longs = [1, 2, 3, 4, 5])
    fun invariant_fencing_token_monotonic_under_chaos(seed: Long) {
        lockChaos(seed).forEach { (key, issued) ->
            assertEquals(issued.sorted(), issued, "$key tokens out of order: $issued")
            assertEquals(issued.toSet().size, issued.size, "$key repeated a token: $issued")
        }
    }

    /**
     * I13: two live sessions never hold one lock at once. The driver asserts it inline on every
     * grant and denial as the log linearizes them; a violation throws inside [lockChaos].
     */
    @ParameterizedTest
    @ValueSource(longs = [1, 2, 3, 4, 5])
    fun invariant_mutual_exclusion_under_chaos(seed: Long) {
        assertTrue(lockChaos(seed).values.any { it.isNotEmpty() }, "the run acquired at least one lock")
    }

    /**
     * One lock-chaos run per seed, shared by the fencing and mutual-exclusion tests. Mutual exclusion
     * is asserted inline as the run proceeds, so a cached result is by itself proof it held; the
     * fencing test reads the tokens it returns. Running it once keeps the class quick.
     */
    private fun lockChaos(seed: Long): Map<Key, List<Long>> = runs.getOrPut(seed) {
        ChaosDriver(seed, dir).use { it.runLockChaos(STEPS) }
    }

    /** C20: the checker accepts the counter history recorded across leader kills and restarts. */
    @ParameterizedTest
    @ValueSource(longs = [1, 2, 3, 4, 5])
    fun invariant_linearizable_ops(seed: Long) {
        ChaosDriver(seed, dir).use { driver ->
            val history = driver.counterHistory()
            assertTrue(history.size >= DOZEN, "recorded ${history.size} operations")
            assertTrue(Linearizability.check(history, CounterSpec), "the counter history is not linearizable")
        }
    }

    /**
     * C20 under its own name: every committed CP operation appears to take effect at one point
     * between its call and its return. That is exactly what [invariant_linearizable_ops] checks,
     * on one of its seeds; [checker_rejects_a_stale_read] is what keeps the check from being
     * vacuous.
     */
    @Test
    fun C20_committed_cp_operations_are_linearizable() = invariant_linearizable_ops(seed = 1)

    /** A stale read the sequential model would never produce is rejected, so the checker is not vacuous. */
    @Test
    fun checker_rejects_a_stale_read() {
        val history = listOf(
            Op(0, call = 0, ret = 1, input = CounterOp.IncrBy(1), output = 1L),
            Op(0, call = 2, ret = 3, input = CounterOp.IncrBy(1), output = 2L),
            // A read after both increments returned, yet it saw 1: no order explains it.
            Op(1, call = 4, ret = 5, input = CounterOp.Get, output = 1L),
        )
        assertFalse(Linearizability.check(history, CounterSpec))
    }

    /**
     * I15: after a session dies, nothing it held remains, even across a failover. A session takes two
     * locks and some permits, the leader is killed, and on the new leader the session's close frees
     * every one of them in the entry that ends it.
     */
    @Test
    fun invariant_session_release_complete() {
        ChaosDriver(seed = 7).use { driver ->
            val kit = driver.kit
            val lockA = Key("cp:lock:a")
            val lockB = Key("cp:lock:b")
            val sem = Key("cp:sem:s")
            val session = (driver.submit(Command.Cp.SessionCreate(Duration.ofHours(1))) as Reply.Integer).value
            driver.submit(Command.Cp.LockTry(lockA, session, Duration.ofHours(1)))
            driver.submit(Command.Cp.LockTry(lockB, session, Duration.ofHours(1)))
            driver.submit(Command.Cp.SemInit(sem, 5))
            driver.submit(Command.Cp.SemAcquire(sem, session, 3))

            kit.killMember(kit.leader().config.nodeId)
            driver.submit(Command.Cp.SessionClose(session))
            val leader = kit.leader()
            kit.awaitApplied(leader.config.nodeId, leader.node.getReport().get(REPLY_TIMEOUT_SECS, SECONDS).result.log.commitIndex)

            val state = leader.stateMachine
            assertNull(ownerOf(state, lockA), "lock a released with the session")
            assertNull(ownerOf(state, lockB), "lock b released with the session")
            assertEquals(5L, available(state, sem), "every permit back")
        }
    }

    /** I16: a minority gone (one of three) still leaves a majority, so CP operations keep succeeding. */
    @Test
    fun I16_minority_kill_keeps_cp_available() {
        CpTestKit().use { kit ->
            val leader = kit.leader().config.nodeId
            val session = (submit(kit, Command.Cp.SessionCreate(Duration.ofHours(1))) as Reply.Integer).value
            kit.killMember(kit.live().first { it != leader })

            val lock = submit(kit, Command.Cp.LockTry(Key("cp:lock:l"), session, Duration.ofHours(1))) as Reply.Array
            assertEquals(1L, (lock.items[0] as Reply.Integer).value, "granted with a majority alive")
        }
    }

    /**
     * I17: a majority gone (two of three) leaves no quorum, so a write blocks or is refused, never a
     * false success. The lone survivor's own engine must not answer a granted lock.
     */
    @Test
    fun I17_majority_kill_never_false_succeeds() {
        CpTestKit().use { kit ->
            val leader = kit.leader()
            val session = (submit(kit, Command.Cp.SessionCreate(Duration.ofHours(1))) as Reply.Integer).value
            kit.live().filter { it != leader.config.nodeId }.forEach(kit::killMember)

            val pending = kit.engine(leader.config.nodeId)
                .submit(Command.Cp.LockTry(Key("cp:lock:l"), session, Duration.ofHours(1)))
            val reply = try {
                pending.get(UNAVAILABLE_WAIT_SECS, SECONDS)
            } catch (expected: TimeoutException) {
                null
            }

            assertTrue(reply == null || (reply as Reply.Error).kind == "NOTLEADER") { "a minority answered $reply" }
            if (reply is Reply.Array) assertEquals(0L, (reply.items[0] as Reply.Integer).value, "never a granted lock")
        }
    }

    private fun submit(kit: CpTestKit, command: Command): Reply =
        kit.leaderEngine().submit(command).get(REPLY_TIMEOUT_SECS, SECONDS)

    private fun ownerOf(state: CpStateMachine, key: Key): Long? =
        ((state.read(Command.Cp.LockState(key)) as Reply.Array).items[0] as? Reply.Integer)?.value

    private fun available(state: CpStateMachine, key: Key): Long =
        (state.read(Command.Cp.SemAvailable(key)) as Reply.Integer).value

    private companion object {
        const val STEPS = 30
        const val DOZEN = 12
        const val REPLY_TIMEOUT_SECS = 10L
        const val UNAVAILABLE_WAIT_SECS = 2L

        /** One shared lock-chaos result per seed, so the two lock invariants do not each run chaos. */
        private val runs = HashMap<Long, Map<Key, List<Long>>>()
    }
}
