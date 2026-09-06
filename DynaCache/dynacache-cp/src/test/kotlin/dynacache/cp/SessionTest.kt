package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * Sessions (CP spec 4, 9.3) at the state machine: the registry, the heartbeat that keeps one alive
 * on log time, and the one entry that ends it and releases what it held (C18, I15). What the log
 * itself has to show — which index the close landed on, and a lapse on a successor's own ticks —
 * is [SessionLogTest].
 */
class SessionTest {

    private val cp = Primitives()
    private val lock = Key("cp:lock:l")
    private val other = Key("cp:lock:m")
    private val sem = Key("cp:sem:s")

    private fun create(timeout: Duration = TIMEOUT) = (cp.apply(Command.Cp.SessionCreate(timeout)) as Reply.Integer).value

    private fun tryLock(session: Long, key: Key = lock) = cp.apply(Command.Cp.LockTry(key, session, LEASE))

    private fun state(key: Key = lock) = cp.apply(Command.Cp.LockState(key))

    private fun unowned(token: Long) = Reply.Array(listOf(Reply.Bulk(null), Reply.Integer(token), Reply.Integer(0), Reply.Integer(0)))

    private fun granted(token: Long) = Reply.Array(listOf(Reply.Integer(1), Reply.Integer(token)))

    private fun kindOf(reply: Reply) = (reply as Reply.Error).kind

    @Test
    fun session_create_heartbeat_close() {
        assertEquals(Reply.Integer(1), cp.apply(Command.Cp.SessionCreate(TIMEOUT)))
        assertEquals(Reply.Integer(2), cp.apply(Command.Cp.SessionCreate(TIMEOUT)), "ids climb")
        assertEquals(Reply.Simple("OK"), cp.apply(Command.Cp.SessionHeartbeat(1)))
        assertEquals(Reply.Simple("OK"), cp.apply(Command.Cp.SessionClose(1)))
        assertEquals("NOSESSION", kindOf(cp.apply(Command.Cp.SessionHeartbeat(1))), "closed is gone")
    }

    /** CP spec 10.6, 6.8: a lock verb on behalf of a session that expired or never existed. */
    @Test
    fun session_op_without_session_rejected() {
        assertEquals("NOSESSION", kindOf(tryLock(session = 99)), "never created")
        assertEquals(unowned(token = 0), state(), "nothing was granted")

        val session = create()
        assertEquals(Reply.Simple("OK"), cp.apply(Command.Cp.SessionClose(session)))

        assertEquals("NOSESSION", kindOf(tryLock(session)), "closed")
        assertEquals("NOSESSION", kindOf(cp.apply(Command.Cp.LockUnlock(lock, session, token = 1))))
        assertEquals("NOSESSION", kindOf(cp.apply(Command.Cp.LockRenew(lock, session, token = 1, lease = LEASE))))
    }

    /** CP spec 10.6: "wait > timeout" is the clock moving and a tick carrying it into the log. */
    @Test
    fun session_timeout_closes() {
        val session = create(timeout = Duration.ofSeconds(1))
        assertEquals(granted(1), tryLock(session))

        cp.tick(after = Duration.ofSeconds(2))

        assertEquals(unowned(token = 1), state(), "released with the session")
        assertEquals("NOSESSION", kindOf(cp.apply(Command.Cp.SessionHeartbeat(session))), "the session is gone")
    }

    /** A heartbeat restarts the timeout from its own entry's log time, so a chatty session outlives it. */
    @Test
    fun session_heartbeat_keeps_alive() {
        val session = create(timeout = Duration.ofSeconds(1))
        assertEquals(granted(1), tryLock(session))

        cp.advance(Duration.ofMillis(600))
        assertEquals(Reply.Simple("OK"), cp.apply(Command.Cp.SessionHeartbeat(session)))
        cp.tick(after = Duration.ofMillis(600))

        assertEquals(session, ((state() as Reply.Array).items[0] as Reply.Integer).value, "still held")
    }

    /**
     * C18, I15: everything one session holds — two locks and its permits — is released by applying
     * its CLOSE, in that single entry, so no member ever sees the session half torn down.
     */
    @Test
    fun C18_close_releases_every_lock_and_permit_in_one_entry() {
        val session = create()
        assertEquals(granted(1), tryLock(session, lock))
        assertEquals(granted(1), tryLock(session, other))
        assertEquals(Reply.Simple("OK"), cp.apply(Command.Cp.SemInit(sem, permits = 5)))
        assertEquals(Reply.Integer(1), cp.apply(Command.Cp.SemAcquire(sem, session, permits = 2)))

        assertEquals(Reply.Simple("OK"), cp.apply(Command.Cp.SessionClose(session)))

        assertEquals(unowned(token = 1), state(lock))
        assertEquals(unowned(token = 1), state(other))
        assertEquals(Reply.Integer(5), cp.apply(Command.Cp.SemAvailable(sem)), "its permits came back too")
    }

    private companion object {
        val TIMEOUT: Duration = Duration.ofSeconds(15)
        val LEASE: Duration = Duration.ofSeconds(30)
    }
}
