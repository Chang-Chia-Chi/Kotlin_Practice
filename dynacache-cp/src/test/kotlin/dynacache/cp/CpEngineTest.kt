package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.TimeoutException

/**
 * The CP engine's seam: `submit(command)` answers once the entry is committed and applied. Every
 * test drives a three-member in-process group through the leader's engine.
 */
class CpEngineTest {

    private val kit = CpTestKit()
    private val counter = Key("cp:counter:c")

    @AfterEach
    fun tearDown() = kit.close()

    private fun submit(command: Command): Reply =
        kit.leaderEngine().submit(command).get(REPLY_TIMEOUT_SECS, SECONDS)

    @Test
    fun long_set_get_roundtrip() {
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.LongSet(counter, 7)))
        assertEquals(Reply.Integer(7), submit(Command.Cp.LongGet(counter)))
    }

    @Test
    fun long_get_missing_is_nil() {
        assertEquals(Reply.Bulk(null), submit(Command.Cp.LongGet(Key("cp:counter:never-written"))))
    }

    @Test
    fun long_incr_decr() {
        assertEquals(Reply.Integer(1), submit(Command.Cp.LongIncr(counter)))
        assertEquals(Reply.Integer(2), submit(Command.Cp.LongIncr(counter)))
        assertEquals(Reply.Integer(1), submit(Command.Cp.LongDecr(counter)))
        assertEquals(Reply.Integer(11), submit(Command.Cp.LongIncrBy(counter, 10)))
        assertEquals(Reply.Integer(7), submit(Command.Cp.LongDecrBy(counter, 4)))
        assertEquals(Reply.Integer(7), submit(Command.Cp.LongGet(counter)))
    }

    @Test
    fun long_cas_success() {
        submit(Command.Cp.LongSet(counter, 5))
        assertEquals(Reply.Integer(1), submit(Command.Cp.LongCas(counter, expected = 5, new = 9)))
        assertEquals(Reply.Integer(9), submit(Command.Cp.LongGet(counter)))
    }

    @Test
    fun long_cas_failure() {
        submit(Command.Cp.LongSet(counter, 5))
        assertEquals(Reply.Integer(0), submit(Command.Cp.LongCas(counter, expected = 4, new = 9)))
        assertEquals(Reply.Integer(5), submit(Command.Cp.LongGet(counter)))
    }

    /** N increments in flight at once land in the log one after another, so the sum is exactly N. */
    @Test
    fun long_concurrent_incr_linearizable() {
        val engine = kit.leaderEngine()
        val clients = Executors.newFixedThreadPool(CLIENT_THREADS)
        try {
            val incremented = (1..CONCURRENT_INCREMENTS).map {
                CompletableFuture.supplyAsync(
                    { engine.submit(Command.Cp.LongIncr(counter)).get(REPLY_TIMEOUT_SECS, SECONDS) },
                    clients,
                )
            }
            val replies = incremented.map { it.get(REPLY_TIMEOUT_SECS, SECONDS) }

            // Every increment saw a distinct value, and together they cover 1..N exactly once.
            assertEquals(
                (1L..CONCURRENT_INCREMENTS).toSet(),
                replies.map { (it as Reply.Integer).value }.toSet(),
            )
            assertEquals(Reply.Integer(CONCURRENT_INCREMENTS), submit(Command.Cp.LongGet(counter)))
        } finally {
            clients.shutdownNow()
        }
    }

    /** I16: one of three members gone still leaves a majority, so the group keeps serving. */
    @Test
    fun cp_minority_failure_available() {
        val leader = kit.leader().config.nodeId
        submit(Command.Cp.LongSet(counter, 1))

        kit.killMember(kit.live().first { it != leader })

        assertEquals(Reply.Integer(2), submit(Command.Cp.LongIncr(counter)))
        assertEquals(Reply.Integer(2), submit(Command.Cp.LongGet(counter)))
    }

    /** I17: two of three gone leaves no majority, so an operation blocks or errors, never succeeds. */
    @Test
    fun cp_majority_failure_unavailable() {
        val leader = kit.leader()
        submit(Command.Cp.LongSet(counter, 1))
        kit.live().filter { it != leader.config.nodeId }.forEach(kit::killMember)

        val pending = kit.engine(leader.config.nodeId).submit(Command.Cp.LongIncr(counter))
        val reply = try {
            pending.get(UNAVAILABLE_WAIT_SECS, SECONDS)
        } catch (expected: TimeoutException) {
            null
        }

        // Either the entry never commits (the future is still pending) or leadership was lost;
        // an applied reply would mean a lone member decided for the group.
        assertTrue(reply == null || (reply as Reply.Error).kind == "NOTLEADER") {
            "a minority must not answer with success, got $reply"
        }
        assertNotEquals(Reply.Integer(2), reply)
    }

    /** A follower never applies on its own: it names the leader and lets the client retry (T43 forwards). */
    @Test
    fun cp_follower_answers_notleader() {
        val follower = kit.live().first { it != kit.leader().config.nodeId }

        val reply = kit.engine(follower).submit(Command.Cp.LongIncr(counter)).get(REPLY_TIMEOUT_SECS, SECONDS)

        assertEquals("NOTLEADER", (reply as Reply.Error).kind)
    }

    /** A cp: key is the CP engine's only business, and everything else is not its business. */
    @Test
    fun C16_cp_engine_rejects_a_non_cp_key() {
        val reply = submit(Command.Cp.LongIncr(Key("plain-key")))

        assertEquals("NOTCP", (reply as Reply.Error).kind)
    }

    /**
     * C21: the reply was observed, so the entry is in a majority of the members' logs. Read the
     * leader's commit index and count the members whose log already reaches it.
     */
    @Test
    fun C21_success_implies_majority_commit() {
        val leader = kit.leader()

        assertEquals(Reply.Simple("OK"), submit(Command.Cp.LongSet(counter, 42)))

        val committed = leader.node.report().log.commitIndex
        val holders = kit.live().count { kit.runtime(it).node.report().log.lastLogOrSnapshotIndex >= committed }

        assertTrue(holders >= kit.members.size / 2 + 1) { "$holders of ${kit.members.size} members hold entry $committed" }
        assertEquals(42L, leader.stateMachine.valueOf(counter))
    }

    private fun io.microraft.RaftNode.report() = getReport().get(REPLY_TIMEOUT_SECS, SECONDS).result

    private companion object {
        const val REPLY_TIMEOUT_SECS = 10L
        const val UNAVAILABLE_WAIT_SECS = 2L
        const val CONCURRENT_INCREMENTS = 50L
        const val CLIENT_THREADS = 4
    }
}
