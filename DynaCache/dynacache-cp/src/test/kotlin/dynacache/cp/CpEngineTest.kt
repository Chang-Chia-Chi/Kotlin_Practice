package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
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

    /** CP spec 10.2: the TTL runs on log time, so the leader's clock moving 2s expires a 1s counter. */
    @Test
    fun long_ttl_expires() {
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.LongSet(counter, 5, ttl = Duration.ofSeconds(1))))
        assertEquals(Reply.Integer(5), submit(Command.Cp.LongGet(counter)))

        kit.clock(kit.leader().config.nodeId).advance(Duration.ofSeconds(2))

        assertEquals(Reply.Bulk(null), submit(Command.Cp.LongGet(counter)))
    }

    /** CP spec 9.4: EXPIRE, TTL and PERSIST on a counter answer as Redis does, measured on log time. */
    @Test
    fun long_expire_ttl_persist() {
        val clock = kit.clock(kit.leader().config.nodeId)
        assertEquals(Reply.Integer(-2), submit(Command.Cp.LongTtl(counter)), "TTL of a missing counter")
        submit(Command.Cp.LongSet(counter, 5))
        assertEquals(Reply.Integer(-1), submit(Command.Cp.LongTtl(counter)), "TTL of a counter without one")
        assertEquals(Reply.Integer(0), submit(Command.Cp.LongExpire(Key("cp:counter:missing"), Duration.ofSeconds(10))))
        assertEquals(Reply.Integer(1), submit(Command.Cp.LongExpire(counter, Duration.ofSeconds(10))))

        clock.advance(Duration.ofSeconds(4))
        assertEquals(Reply.Integer(6), submit(Command.Cp.LongTtl(counter)), "seconds left, rounded as Redis rounds")
        assertEquals(Reply.Integer(1), submit(Command.Cp.LongPersist(counter)))
        assertEquals(Reply.Integer(0), submit(Command.Cp.LongPersist(counter)), "already persistent")

        clock.advance(Duration.ofSeconds(10))
        assertEquals(Reply.Integer(5), submit(Command.Cp.LongGet(counter)), "PERSIST outlived the old TTL")
    }

    /** CP spec 5: with no user traffic only a tick carries time, so the counter expires on the tick. */
    @Test
    fun ttl_tick_advances_time_when_idle() {
        val leader = kit.leader()
        submit(Command.Cp.LongSet(counter, 5, ttl = Duration.ofSeconds(1)))
        kit.clock(leader.config.nodeId).advance(Duration.ofSeconds(2))
        assertEquals(5L, leader.stateMachine.valueOf(counter), "the clock moved but log time did not")

        leader.tick().get(REPLY_TIMEOUT_SECS, SECONDS)

        assertNull(leader.stateMachine.valueOf(counter))
    }

    /**
     * C23: expiry is decided by the stamp carried in the log, so members that have applied the
     * same index agree on it. The followers' clocks never move; only the leader's does.
     */
    @Test
    fun C23_every_member_agrees_on_expiry_at_same_index() {
        val leader = kit.leader()
        submit(Command.Cp.LongSet(counter, 5, ttl = Duration.ofSeconds(1)))
        val setIndex = leader.node.report().log.commitIndex
        kit.live().forEach { member ->
            kit.awaitApplied(member, setIndex)
            assertEquals(5L, kit.runtime(member).stateMachine.valueOf(counter), "$member at index $setIndex")
        }

        kit.clock(leader.config.nodeId).advance(Duration.ofSeconds(2))
        val tickIndex = leader.tick().get(REPLY_TIMEOUT_SECS, SECONDS)

        assertTrue(tickIndex > setIndex, "the tick landed at $tickIndex, after $setIndex")
        kit.live().forEach { member ->
            kit.awaitApplied(member, tickIndex)
            assertNull(kit.runtime(member).stateMachine.valueOf(counter), "$member at index $tickIndex")
        }
    }

    /**
     * C19: the old leader's clock runs an hour ahead of its successor's. The successor's stamps
     * still climb past everything the old leader committed, so log time never turns back.
     */
    @Test
    fun C19_log_timestamps_monotonic_across_leader_change() {
        val old = kit.leader()
        kit.clock(old.config.nodeId).advance(Duration.ofHours(1))
        submit(Command.Cp.LongSet(counter, 1))
        submit(Command.Cp.LongIncr(counter))
        val lastByOld = old.stateMachine.lastAppliedTs

        kit.killMember(old.config.nodeId)
        val successor = kit.leader()
        val stamps = (1..3).map {
            submit(Command.Cp.LongIncr(counter))
            successor.stateMachine.lastAppliedTs
        }

        assertNotEquals(old.config.nodeId, successor.config.nodeId)
        assertTrue(stamps[0] > lastByOld, "the successor's first stamp ${stamps[0]} is past the old leader's $lastByOld")
        assertEquals(stamps, stamps.distinct().sorted(), "stamps climb strictly: $stamps")
        assertTrue(stamps[0] > kit.clock(successor.config.nodeId).millis(), "log time runs ahead of the successor's own clock")
    }

    private fun io.microraft.RaftNode.report() = getReport().get(REPLY_TIMEOUT_SECS, SECONDS).result

    private companion object {
        const val REPLY_TIMEOUT_SECS = 10L
        const val UNAVAILABLE_WAIT_SECS = 2L
        const val CONCURRENT_INCREMENTS = 50L
        const val CLIENT_THREADS = 4
    }
}
