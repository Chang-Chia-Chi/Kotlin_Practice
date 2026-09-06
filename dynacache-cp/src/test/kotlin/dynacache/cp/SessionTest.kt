package dynacache.cp

import dynacache.cluster.NodeId
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.TimeUnit.SECONDS

/**
 * Sessions (CP spec 4, 9.3) through the CP engine's seam: the registry, the heartbeat that keeps
 * one alive on log time, and the one entry that ends it and releases what it held (C18, I15).
 */
class SessionTest {

    private val kit = CpTestKit()
    private val lock = Key("cp:lock:l")
    private val other = Key("cp:lock:m")

    @AfterEach
    fun tearDown() = kit.close()

    private fun submit(command: Command): Reply =
        kit.leaderEngine().submit(command).get(REPLY_TIMEOUT_SECS, SECONDS)

    private fun create(timeout: Duration = TIMEOUT) = (submit(Command.Cp.SessionCreate(timeout)) as Reply.Integer).value

    private fun tryLock(session: Long, key: Key = lock) = submit(Command.Cp.LockTry(key, session, LEASE))

    private fun state(key: Key = lock) = submit(Command.Cp.LockState(key))

    private fun unowned(token: Long) = Reply.Array(listOf(Reply.Bulk(null), Reply.Integer(token), Reply.Integer(0), Reply.Integer(0)))

    private fun granted(token: Long) = Reply.Array(listOf(Reply.Integer(1), Reply.Integer(token)))

    private fun kindOf(reply: Reply) = (reply as Reply.Error).kind

    @Test
    fun session_create_heartbeat_close() {
        assertEquals(Reply.Integer(1), submit(Command.Cp.SessionCreate(TIMEOUT)))
        assertEquals(Reply.Integer(2), submit(Command.Cp.SessionCreate(TIMEOUT)), "ids climb")
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.SessionHeartbeat(1)))
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.SessionClose(1)))
        assertEquals("NOSESSION", kindOf(submit(Command.Cp.SessionHeartbeat(1))), "closed is gone")
    }

    /** CP spec 10.6, 6.8: a lock verb on behalf of a session that expired or never existed. */
    @Test
    fun session_op_without_session_rejected() {
        assertEquals("NOSESSION", kindOf(tryLock(session = 99)), "never created")
        assertEquals(unowned(token = 0), state(), "nothing was granted")

        val session = create()
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.SessionClose(session)))

        assertEquals("NOSESSION", kindOf(tryLock(session)), "closed")
        assertEquals("NOSESSION", kindOf(submit(Command.Cp.LockUnlock(lock, session, token = 1))))
        assertEquals("NOSESSION", kindOf(submit(Command.Cp.LockRenew(lock, session, token = 1, ttl = LEASE))))
    }

    /** CP spec 10.6: "wait > timeout" is the leader's clock moving and a tick carrying it into the log. */
    @Test
    fun session_timeout_closes() {
        val leader = kit.leader()
        val session = create(timeout = Duration.ofSeconds(1))
        assertEquals(granted(1), tryLock(session))

        kit.clock(leader.config.nodeId).advance(Duration.ofSeconds(2))
        leader.tick().get(REPLY_TIMEOUT_SECS, SECONDS)

        assertEquals(unowned(token = 1), state(), "released with the session")
        assertEquals("NOSESSION", kindOf(submit(Command.Cp.SessionHeartbeat(session))), "the session is gone")
    }

    /** A heartbeat restarts the timeout from its own entry's log time, so a chatty session outlives it. */
    @Test
    fun session_heartbeat_keeps_alive() {
        val leader = kit.leader()
        val session = create(timeout = Duration.ofSeconds(1))
        assertEquals(granted(1), tryLock(session))

        kit.clock(leader.config.nodeId).advance(Duration.ofMillis(600))
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.SessionHeartbeat(session)))
        kit.clock(leader.config.nodeId).advance(Duration.ofMillis(600))
        leader.tick().get(REPLY_TIMEOUT_SECS, SECONDS)

        assertEquals(session, ((state() as Reply.Array).items[0] as Reply.Integer).value, "still held")
    }

    /**
     * I15: the tick's completion names the index of the `SESSION_CLOSED` entry, the one right after
     * the tick; once two members have applied it, STATE on each shows no lock owned by the session.
     */
    @Test
    fun I15_no_lock_owned_after_session_closed_index() {
        val leader = kit.leader()
        val session = create(timeout = Duration.ofSeconds(1))
        assertEquals(granted(1), tryLock(session, lock))
        assertEquals(granted(1), tryLock(session, other))
        val before = commitIndex()

        kit.clock(leader.config.nodeId).advance(Duration.ofSeconds(2))
        val closedIndex = leader.tick().get(REPLY_TIMEOUT_SECS, SECONDS)

        assertEquals(before + 2, closedIndex, "the tick, then one SESSION_CLOSED")
        val members = listOf(leader.config.nodeId, kit.live().first { it != leader.config.nodeId })
        members.forEach { member ->
            kit.awaitApplied(member, closedIndex)
            assertEquals(unowned(token = 1), stateOn(member, lock), "$member")
            assertEquals(unowned(token = 1), stateOn(member, other), "$member")
        }
    }

    /** C18: two locks held by one session are released by its CLOSE, one committed entry, no partial state. */
    @Test
    fun C18_release_is_one_log_entry() {
        val session = create()
        assertEquals(granted(1), tryLock(session, lock))
        assertEquals(granted(1), tryLock(session, other))
        assertEquals(session, ((state(other) as Reply.Array).items[0] as Reply.Integer).value)
        val before = commitIndex()

        assertEquals(Reply.Simple("OK"), submit(Command.Cp.SessionClose(session)))

        assertEquals(before + 1, commitIndex(), "the CLOSE is the only entry")
        assertEquals(unowned(token = 1), state(lock))
        assertEquals(unowned(token = 1), state(other))
    }

    private fun commitIndex(): Long =
        kit.leader().node.getReport().get(REPLY_TIMEOUT_SECS, SECONDS).result.log.commitIndex

    /** STATE as [member] sees it at its own applied index; a follower cannot answer through its engine. */
    private fun stateOn(member: NodeId, key: Key): Reply =
        kit.runtime(member).stateMachine.let { it.locks.apply(Command.Cp.LockState(key), it.lastAppliedTs) }

    private companion object {
        const val REPLY_TIMEOUT_SECS = 10L
        val TIMEOUT: Duration = Duration.ofSeconds(15)
        val LEASE: Duration = Duration.ofSeconds(30)
    }
}
