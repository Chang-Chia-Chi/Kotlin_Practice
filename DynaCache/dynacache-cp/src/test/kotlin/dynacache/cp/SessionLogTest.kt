package dynacache.cp

import dynacache.cluster.NodeId
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.TimeUnit.SECONDS

/**
 * The session facts only a log can show: which index a close landed on, that every member has it
 * at that index, and that a session lapses on a successor's own idle ticks after a clock skew.
 * The session's semantics are [SessionTest], which needs no group.
 */
class SessionLogTest {

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

    /**
     * C18, I19: the old leader's clock ran 30 s ahead of its successor's. A session with a
     * one-second timeout whose heartbeat stops lapses on the successor's idle ticks alone, one
     * second of the successor's own clock later, and the lock it held is released with it.
     */
    @Test
    fun C18_session_lapses_after_skewed_failover() {
        val old = kit.leader()
        kit.clock(old.config.nodeId).advance(SKEW)
        val session = create(timeout = Duration.ofSeconds(1))
        assertEquals(granted(1), tryLock(session))

        kit.killMember(old.config.nodeId)
        val successor = kit.leader()
        val interval = successor.config.tickInterval
        fun idleTick(): Long {
            kit.clock(successor.config.nodeId).advance(interval)
            return successor.tick().get(REPLY_TIMEOUT_SECS, SECONDS)
        }
        val ticksInTimeout = (Duration.ofSeconds(1).toMillis() / interval.toMillis()).toInt()

        repeat(ticksInTimeout - 1) { assertNotEquals(0L, idleTick(), "every idle interval appends a tick") }
        assertEquals(session, ((stateOn(successor.config.nodeId, lock) as Reply.Array).items[0] as Reply.Integer).value, "held one interval short of the timeout")
        assertNotEquals(0L, idleTick(), "the tick whose SESSION_CLOSED ends the session")
        assertEquals(unowned(token = 1), stateOn(successor.config.nodeId, lock), "released by the tick's SESSION_CLOSED, no user command in between")
        assertEquals("NOSESSION", kindOf(submit(Command.Cp.SessionHeartbeat(session))), "the session is gone")
    }

    private fun commitIndex(): Long =
        kit.leader().node.getReport().get(REPLY_TIMEOUT_SECS, SECONDS).result.log.commitIndex

    /** STATE as [member] sees it at its own applied index; a follower cannot answer through its engine. */
    private fun stateOn(member: NodeId, key: Key): Reply =
        kit.runtime(member).stateMachine.read(Command.Cp.LockState(key))

    private companion object {
        const val REPLY_TIMEOUT_SECS = 10L
        val TIMEOUT: Duration = Duration.ofSeconds(15)
        val LEASE: Duration = Duration.ofSeconds(30)
        val SKEW: Duration = Duration.ofSeconds(30)
    }
}
