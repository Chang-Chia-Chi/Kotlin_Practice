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
 * The FencedLock (CP spec 3.1) through the CP engine's seam. Time is the leader's injected clock,
 * carried into the log by the next entry.
 */
class FencedLockTest {

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

    private fun tryLock(session: Long, ttl: Duration = LEASE) = submit(Command.Cp.LockTry(lock, session, ttl))

    private fun state() = submit(Command.Cp.LockState(lock))

    private fun unowned(token: Long) = Reply.Array(listOf(Reply.Bulk(null), Reply.Integer(token), Reply.Integer(0), Reply.Integer(0)))

    private fun heldBy(session: Long, token: Long, remaining: Long, holds: Long = 1) =
        Reply.Array(listOf(Reply.Integer(session), Reply.Integer(token), Reply.Integer(remaining), Reply.Integer(holds)))

    private fun granted(token: Long) = Reply.Array(listOf(Reply.Integer(1), Reply.Integer(token)))

    private fun denied() = Reply.Array(listOf(Reply.Integer(0), Reply.Integer(0)))

    @Test
    fun lock_try_acquire_release_roundtrip() {
        assertEquals(granted(1), tryLock(session = 7))
        // The STATE entry itself moves log time one millisecond past the TRY (C19).
        assertEquals(heldBy(session = 7, token = 1, remaining = LEASE.toMillis() - 1), state())
        assertEquals(Reply.Integer(1), submit(Command.Cp.LockUnlock(lock, session = 7, token = 1)))
        assertEquals(unowned(token = 1), state())
    }

    /** I13: two sessions race for the lock; exactly one is granted, and STATE names it. */
    @Test
    fun lock_mutual_exclusion() {
        val engine = kit.leaderEngine()
        val first = engine.submit(Command.Cp.LockTry(lock, 1, LEASE))
        val second = engine.submit(Command.Cp.LockTry(lock, 2, LEASE))

        val replies = listOf(first, second).map { it.get(REPLY_TIMEOUT_SECS, SECONDS) }

        assertEquals(1, replies.count { it == granted(1) }, "exactly one granted: $replies")
        assertEquals(1, replies.count { it == denied() }, "the other denied: $replies")
    }

    /** I14, C17: a hundred acquire/release cycles hand out strictly climbing tokens. */
    @Test
    fun lock_fencing_token_monotonic() {
        val tokens = (1..CYCLES).map { cycle ->
            val session = (cycle % 3 + 1).toLong()
            val token = ((tryLock(session) as Reply.Array).items[1] as Reply.Integer).value
            assertEquals(Reply.Integer(1), submit(Command.Cp.LockUnlock(lock, session, token)))
            token
        }

        assertEquals(tokens.sorted(), tokens, "tokens climb: $tokens")
        assertEquals(CYCLES, tokens.distinct().size, "no token issued twice: $tokens")
    }

    @Test
    fun lock_reentrant_same_session() {
        assertEquals(granted(1), tryLock(session = 7))
        assertEquals(granted(1), tryLock(session = 7), "held again with the same token")
        assertEquals(heldBy(session = 7, token = 1, remaining = LEASE.toMillis() - 2, holds = 2), state())

        assertEquals(Reply.Integer(0), submit(Command.Cp.LockUnlock(lock, 7, 1)), "one hold dropped, still held")
        assertEquals(Reply.Integer(1), submit(Command.Cp.LockUnlock(lock, 7, 1)), "released")
        assertEquals(unowned(token = 1), state())
    }

    @Test
    fun lock_unlock_wrong_session_rejected() {
        assertEquals(granted(1), tryLock(session = 7))

        assertEquals("REENTRANCE", (submit(Command.Cp.LockUnlock(lock, session = 8, token = 1)) as Reply.Error).kind)
        assertEquals(heldBy(session = 7, token = 1, remaining = LEASE.toMillis() - 2), state())
    }

    @Test
    fun lock_unlock_wrong_token_rejected() {
        assertEquals(granted(1), tryLock(session = 7))
        assertEquals(Reply.Integer(1), submit(Command.Cp.LockUnlock(lock, 7, 1)))
        assertEquals(granted(2), tryLock(session = 7))

        assertEquals("REENTRANCE", (submit(Command.Cp.LockUnlock(lock, session = 7, token = 1)) as Reply.Error).kind, "the stale token")
        assertEquals(heldBy(session = 7, token = 2, remaining = LEASE.toMillis() - 2), state())
    }

    /** CP spec 10.1: "wait 2s" is the leader's clock moving and a tick carrying the time into the log. */
    @Test
    fun lock_ttl_expires() {
        val leader = kit.leader()
        assertEquals(granted(1), tryLock(session = 7, ttl = Duration.ofSeconds(1)))

        kit.clock(leader.config.nodeId).advance(Duration.ofSeconds(2))
        leader.tick().get(REPLY_TIMEOUT_SECS, SECONDS)

        assertEquals(unowned(token = 1), state())
        assertEquals(granted(2), tryLock(session = 8), "the next holder gets the next token")
    }

    @Test
    fun lock_ttl_renew() {
        val leader = kit.leader()
        assertEquals(granted(1), tryLock(session = 7, ttl = Duration.ofSeconds(1)))
        assertEquals(Reply.Integer(1), submit(Command.Cp.LockRenew(lock, session = 7, token = 1, ttl = Duration.ofSeconds(5))))

        kit.clock(leader.config.nodeId).advance(Duration.ofSeconds(2))
        leader.tick().get(REPLY_TIMEOUT_SECS, SECONDS)

        assertEquals(heldBy(session = 7, token = 1, remaining = Duration.ofSeconds(3).toMillis()), state())
    }

    @Test
    fun lock_renew_by_non_holder_rejected() {
        assertEquals(granted(1), tryLock(session = 7, ttl = Duration.ofSeconds(1)))

        val renewed = submit(Command.Cp.LockRenew(lock, session = 8, token = 1, ttl = Duration.ofSeconds(5)))

        assertEquals("REENTRANCE", (renewed as Reply.Error).kind)
        assertEquals(heldBy(session = 7, token = 1, remaining = Duration.ofSeconds(1).toMillis() - 2), state(), "lease unchanged")
    }

    @Test
    fun lock_force_unlock_overrides() {
        assertEquals(granted(1), tryLock(session = 7))
        assertEquals(denied(), tryLock(session = 8))

        assertEquals(Reply.Simple("OK"), submit(Command.Cp.LockForceUnlock(lock)))

        assertEquals(unowned(token = 1), state())
        assertEquals(granted(2), tryLock(session = 8))
    }

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
        assertEquals(granted(1), tryLock(session = 7, ttl = lease))
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

    private companion object {
        const val REPLY_TIMEOUT_SECS = 10L
        const val CYCLES = 100
        const val SESSIONS = 8
        val LEASE: Duration = Duration.ofSeconds(30)
    }
}
