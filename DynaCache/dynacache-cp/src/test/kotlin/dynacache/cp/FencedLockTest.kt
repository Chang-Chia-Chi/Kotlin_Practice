package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * The FencedLock (CP spec 3.1) at the state machine: a lock is a pure function of the command and
 * the log time stamped on the entry carrying it, so these need no Raft group. What the lock does
 * across a leader change is [FencedLockFailoverTest]; the race two clients run for it is
 * [CpConcurrencyTest].
 */
class FencedLockTest {

    private val cp = Primitives()
    private val lock = Key("cp:lock:l")

    /**
     * A lock is held by a registered session (T41); these tests name theirs by number, so 1 to 8
     * exist. Registering them spends one millisecond of log time each (C19), and the clock follows,
     * so a jump of the clock below lands where it did before the registry existed.
     */
    @BeforeEach
    fun registerSessions() {
        repeat(SESSIONS) { cp.apply(Command.Cp.SessionCreate(Duration.ofHours(1))) }
        cp.advance(Duration.ofMillis(SESSIONS.toLong()))
    }

    private fun tryLock(session: Long, lease: Duration = LEASE) = cp.apply(Command.Cp.LockTry(lock, session, lease))

    private fun state() = cp.apply(Command.Cp.LockState(lock))

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
        assertEquals(Reply.Integer(1), cp.apply(Command.Cp.LockUnlock(lock, session = 7, token = 1)))
        assertEquals(unowned(token = 1), state())
    }

    /** I14, C17: a hundred acquire/release cycles hand out strictly climbing tokens. */
    @Test
    fun lock_fencing_token_monotonic() {
        val tokens = (1..CYCLES).map { cycle ->
            val session = (cycle % 3 + 1).toLong()
            val token = ((tryLock(session) as Reply.Array).items[1] as Reply.Integer).value
            assertEquals(Reply.Integer(1), cp.apply(Command.Cp.LockUnlock(lock, session, token)))
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

        assertEquals(Reply.Integer(0), cp.apply(Command.Cp.LockUnlock(lock, 7, 1)), "one hold dropped, still held")
        assertEquals(Reply.Integer(1), cp.apply(Command.Cp.LockUnlock(lock, 7, 1)), "released")
        assertEquals(unowned(token = 1), state())
    }

    @Test
    fun lock_unlock_wrong_session_rejected() {
        assertEquals(granted(1), tryLock(session = 7))

        assertEquals("REENTRANCE", (cp.apply(Command.Cp.LockUnlock(lock, session = 8, token = 1)) as Reply.Error).kind)
        assertEquals(heldBy(session = 7, token = 1, remaining = LEASE.toMillis() - 2), state())
    }

    @Test
    fun lock_unlock_wrong_token_rejected() {
        assertEquals(granted(1), tryLock(session = 7))
        assertEquals(Reply.Integer(1), cp.apply(Command.Cp.LockUnlock(lock, 7, 1)))
        assertEquals(granted(2), tryLock(session = 7))

        assertEquals("REENTRANCE", (cp.apply(Command.Cp.LockUnlock(lock, session = 7, token = 1)) as Reply.Error).kind, "the stale token")
        assertEquals(heldBy(session = 7, token = 2, remaining = LEASE.toMillis() - 2), state())
    }

    /** CP spec 10.1: "wait 2s" is the clock moving and a tick carrying the time into the log. */
    @Test
    fun lock_ttl_expires() {
        assertEquals(granted(1), tryLock(session = 7, lease = Duration.ofSeconds(1)))

        cp.tick(after = Duration.ofSeconds(2))

        assertEquals(unowned(token = 1), state())
        assertEquals(granted(2), tryLock(session = 8), "the next holder gets the next token")
    }

    @Test
    fun lock_ttl_renew() {
        assertEquals(granted(1), tryLock(session = 7, lease = Duration.ofSeconds(1)))
        assertEquals(Reply.Integer(1), cp.apply(Command.Cp.LockRenew(lock, session = 7, token = 1, lease = Duration.ofSeconds(5))))

        cp.tick(after = Duration.ofSeconds(2))

        assertEquals(heldBy(session = 7, token = 1, remaining = Duration.ofSeconds(3).toMillis()), state())
    }

    @Test
    fun lock_renew_by_non_holder_rejected() {
        assertEquals(granted(1), tryLock(session = 7, lease = Duration.ofSeconds(1)))

        val renewed = cp.apply(Command.Cp.LockRenew(lock, session = 8, token = 1, lease = Duration.ofSeconds(5)))

        assertEquals("REENTRANCE", (renewed as Reply.Error).kind)
        assertEquals(heldBy(session = 7, token = 1, remaining = Duration.ofSeconds(1).toMillis() - 2), state(), "lease unchanged")
    }

    @Test
    fun lock_force_unlock_overrides() {
        assertEquals(granted(1), tryLock(session = 7))
        assertEquals(denied(), tryLock(session = 8))

        assertEquals(Reply.Simple("OK"), cp.apply(Command.Cp.LockForceUnlock(lock)))

        assertEquals(unowned(token = 1), state())
        assertEquals(granted(2), tryLock(session = 8))
    }

    private companion object {
        const val CYCLES = 100
        const val SESSIONS = 8
        val LEASE: Duration = Duration.ofSeconds(30)
    }
}
