package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.TimeUnit.SECONDS

/**
 * What a FencedLock does when the leader that granted it dies: the lock is state the log carries,
 * so these need a real group and stay on [CpTestKit]. The lock's own semantics are
 * [FencedLockTest], which needs no log at all.
 */
class FencedLockFailoverTest {

    private val kit = CpTestKit()
    private val lock = Key("cp:lock:l")
    private val counter = Key("cp:counter:c")

    /**
     * A lock is held by a registered session (T41); these tests name theirs by number, so 1 to 8
     * exist. Registering them spends one millisecond of log time each (C19), and the clocks follow,
     * so a jump of the leader's clock below lands where it did before the registry existed.
     */
    @BeforeEach
    fun registerSessions() {
        repeat(SESSIONS) { submit(Command.Cp.SessionCreate(Duration.ofHours(1))) }
        kit.members.forEach { kit.clock(it).advance(Duration.ofMillis(SESSIONS.toLong())) }
    }

    @AfterEach
    fun tearDown() = kit.close()

    private fun submit(command: Command): Reply =
        kit.leaderEngine().submit(command).get(REPLY_TIMEOUT_SECS, SECONDS)

    private fun tryLock(session: Long, lease: Duration = LEASE) = submit(Command.Cp.LockTry(lock, session, lease))

    private fun state() = submit(Command.Cp.LockState(lock))

    private fun unowned(token: Long) = Reply.Array(listOf(Reply.Bulk(null), Reply.Integer(token), Reply.Integer(0), Reply.Integer(0)))

    private fun heldBy(session: Long, token: Long, remaining: Long, holds: Long = 1) =
        Reply.Array(listOf(Reply.Integer(session), Reply.Integer(token), Reply.Integer(remaining), Reply.Integer(holds)))

    private fun granted(token: Long) = Reply.Array(listOf(Reply.Integer(1), Reply.Integer(token)))

    private fun denied() = Reply.Array(listOf(Reply.Integer(0), Reply.Integer(0)))

    /** CP spec 10.7: a lock and a counter both outlive the leader that committed them. */
    @Test
    fun cp_leader_failover_preserves_state() {
        val old = kit.leader()
        assertEquals(granted(1), tryLock(session = 7))
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.LongSet(counter, 42)))

        kit.killMember(old.config.nodeId)
        val successor = kit.leader()

        assertNotEquals(old.config.nodeId, successor.config.nodeId)
        assertEquals(Reply.Integer(42), submit(Command.Cp.LongGet(counter)))
        assertEquals(heldBy(session = 7, token = 1, remaining = LEASE.toMillis() - 3), state())
    }

    /** I18: the new leader shows the same owner and token, and the old token still unlocks. */
    @Test
    fun I18_lock_held_across_leader_failover() {
        val old = kit.leader()
        assertEquals(granted(1), tryLock(session = 7))
        kit.killMember(old.config.nodeId)

        assertEquals(denied(), tryLock(session = 8), "still held on the successor")
        assertEquals(heldBy(session = 7, token = 1, remaining = LEASE.toMillis() - 2), state())
        assertEquals(Reply.Integer(1), submit(Command.Cp.LockUnlock(lock, session = 7, token = 1)))
        assertEquals(granted(2), tryLock(session = 8), "C17: the successor continues the token sequence")
    }

    /**
     * I19: the lease is a third spent when the leader dies. The successor's clock has not moved,
     * so log time stands where the old leader left it and the lock is still held; it is still
     * held one millisecond before the lease ends on the successor's clock, and gone at the end.
     */
    @Test
    fun I19_lease_expires_late_never_early_across_failover() {
        val old = kit.leader()
        val lease = Duration.ofSeconds(30)
        assertEquals(granted(1), tryLock(session = 7, lease = lease))
        kit.clock(old.config.nodeId).advance(lease.dividedBy(3))
        assertEquals(heldBy(session = 7, token = 1, remaining = lease.toMillis() * 2 / 3), state())

        kit.killMember(old.config.nodeId)
        val successor = kit.leader()

        assertEquals(heldBy(session = 7, token = 1, remaining = lease.toMillis() * 2 / 3 - 1), state(), "held right after the new term")
        kit.clock(successor.config.nodeId).advance(lease.minusMillis(1))
        assertEquals(heldBy(session = 7, token = 1, remaining = 1), state(), "never early")
        kit.clock(successor.config.nodeId).advance(Duration.ofMillis(1))
        assertEquals(unowned(token = 1), state(), "late by no more than the election")
    }

    /**
     * C17, I19: the old leader's clock ran 30 s ahead of its successor's. A one-second lease taken
     * there is released by the successor's idle ticks alone, one second of the successor's own
     * clock later, with no user command carrying time into the log.
     */
    @Test
    fun C17_lease_expires_after_skewed_failover() {
        val old = kit.leader()
        kit.clock(old.config.nodeId).advance(SKEW)
        assertEquals(granted(1), tryLock(session = 7, lease = Duration.ofSeconds(1)))

        kit.killMember(old.config.nodeId)
        val successor = kit.leader()
        val interval = successor.config.tickInterval
        fun idleTick(): Long {
            kit.clock(successor.config.nodeId).advance(interval)
            return successor.tick().get(REPLY_TIMEOUT_SECS, SECONDS)
        }
        val ticksInLease = (Duration.ofSeconds(1).toMillis() / interval.toMillis()).toInt()

        repeat(ticksInLease - 1) { assertNotEquals(0L, idleTick(), "every idle interval appends a tick") }
        assertEquals(7L, ((stateOnLeader(successor) as Reply.Array).items[0] as Reply.Integer).value, "held one interval short of the lease")
        assertNotEquals(0L, idleTick(), "the tick that ends the lease")
        assertEquals(unowned(token = 1), stateOnLeader(successor), "released by the tick, no user command in between")
        assertEquals(granted(2), tryLock(session = 8), "C17: the next holder gets the next token")
    }

    /** STATE as the leader's state machine sees it at its applied index, without appending an entry. */
    private fun stateOnLeader(leader: RaftRuntime): Reply =
        leader.stateMachine.read(Command.Cp.LockState(lock))

    private companion object {
        const val REPLY_TIMEOUT_SECS = 10L
        const val SESSIONS = 8
        val LEASE: Duration = Duration.ofSeconds(30)
        val SKEW: Duration = Duration.ofSeconds(30)
    }
}
