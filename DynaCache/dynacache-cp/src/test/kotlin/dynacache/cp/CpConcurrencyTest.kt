package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.SECONDS

/**
 * What the log does for clients that arrive at once: it puts their commands in an order, and every
 * primitive then sees them one at a time. That is the property under test here, so each of these
 * needs a real group; the primitives' own semantics are tested at the state machine, in
 * [FencedLockTest], [SemaphoreTest], [CountDownLatchTest] and [AtomicReferenceTest].
 */
class CpConcurrencyTest {

    private val kit = CpTestKit()
    private val lock = Key("cp:lock:l")
    private val sem = Key("cp:sem:s")
    private val latch = Key("cp:latch:l")
    private val ref = Key("cp:ref:r")

    /** These tests name their sessions by number, so 1 to [SESSIONS] are registered and alive. */
    @BeforeEach
    fun registerSessions() {
        repeat(SESSIONS) { submit(Command.Cp.SessionCreate(Duration.ofHours(1))) }
    }

    @AfterEach
    fun tearDown() = kit.close()

    private fun submit(command: Command): Reply =
        kit.leaderEngine().submit(command).get(REPLY_TIMEOUT_SECS, SECONDS)

    /** I13: two sessions race for the lock; exactly one is granted, and the other is denied. */
    @Test
    fun lock_mutual_exclusion() {
        val granted = Reply.Array(listOf(Reply.Integer(1), Reply.Integer(1)))
        val denied = Reply.Array(listOf(Reply.Integer(0), Reply.Integer(0)))

        val replies = race(1..2) { client -> Command.Cp.LockTry(lock, client.toLong(), LEASE) }

        assertEquals(1, replies.count { it == granted }, "exactly one granted: $replies")
        assertEquals(1, replies.count { it == denied }, "the other denied: $replies")
    }

    /**
     * I13 under its own name: at any committed log index at most one session holds a lock key.
     * The assertion is [lock_mutual_exclusion]'s -- exactly one of two racing sessions is granted
     * and the other denied -- so this delegates rather than restating it.
     */
    @Test
    fun I13_at_most_one_session_holds_a_lock() = lock_mutual_exclusion()

    /** The log serializes the ten attempts, so exactly the three permits that exist are handed out. */
    @Test
    fun sem_concurrent_acquire_exactly_permits_succeed() {
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.SemInit(sem, permits = 3)))

        val replies = race(CONTENDERS) { client -> Command.Cp.SemAcquire(sem, client.toLong(), 1) }

        assertEquals(3, replies.count { it == Reply.Integer(1) }, "exactly the permits that exist: $replies")
        assertEquals(Reply.Integer(0), submit(Command.Cp.SemAvailable(sem)))
    }

    /** The log serializes the hundred count-downs, so none of them is lost to a race. */
    @Test
    fun latch_concurrent_down_correct_count() {
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.LatchSet(latch, count = PARTIES)))

        val replies = race(1..PARTIES) { Command.Cp.LatchDown(latch) }

        // Each count-down saw a distinct value: together they are exactly 99 down to 0.
        assertEquals((0L until PARTIES).toSet(), replies.map { (it as Reply.Integer).value }.toSet())
        assertEquals(Reply.Integer(0), submit(Command.Cp.LatchGet(latch)))
    }

    /** I21: N clients swap the same expected bytes; the log serializes them, so one wins and the rest see the winner's. */
    @Test
    fun ref_concurrent_cas_exactly_one_wins() {
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.RefSet(ref, "start".toByteArray())))
        val replies = race(CONTENDERS) { client -> Command.Cp.RefCas(ref, "start".toByteArray(), "won-by-$client".toByteArray()) }

        assertEquals(1, replies.count { it == Reply.Integer(1) }, "exactly one swap: $replies")
        val winner = replies.indexOf(Reply.Integer(1)) + CONTENDERS.first
        assertEquals(Reply.Bulk("won-by-$winner".toByteArray()), submit(Command.Cp.RefGet(ref)), "the reference holds the winner's bytes and nobody else's")
    }

    /** I21 over both CAS-carrying primitives: the counter's compare-and-set races the reference's. */
    @Test
    fun I21_concurrent_cas_exactly_one_wins() {
        val counter = Key("cp:counter:c")
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.LongSet(counter, 0)))
        assertEquals(1, race(CONTENDERS) { client -> Command.Cp.LongCas(counter, expected = 0, new = client.toLong()) }
            .count { it == Reply.Integer(1) }, "exactly one LONG_CAS")
        assertEquals(Reply.Integer(0), submit(Command.Cp.LongCas(counter, expected = 0, new = 99)), "0 is gone")

        assertEquals(Reply.Simple("OK"), submit(Command.Cp.RefSet(ref, "start".toByteArray())))
        assertEquals(1, race(CONTENDERS) { client -> Command.Cp.RefCas(ref, "start".toByteArray(), "$client".toByteArray()) }
            .count { it == Reply.Integer(1) }, "exactly one REF_CAS")
        assertEquals(Reply.Integer(0), submit(Command.Cp.RefCas(ref, "start".toByteArray(), "late".toByteArray())), "the expected bytes are gone")
    }

    /** Every [clients] contender's command submitted at once through the leader's engine. */
    private fun race(clients: IntRange, command: (Int) -> Command): List<Reply> {
        val engine = kit.leaderEngine()
        val pool = Executors.newFixedThreadPool(CLIENT_THREADS)
        return try {
            clients
                .map { client -> CompletableFuture.supplyAsync({ engine.submit(command(client)) }, pool) }
                .map { it.get(REPLY_TIMEOUT_SECS, SECONDS).get(REPLY_TIMEOUT_SECS, SECONDS) }
        } finally {
            pool.shutdownNow()
        }
    }

    private companion object {
        const val REPLY_TIMEOUT_SECS = 10L
        const val CLIENT_THREADS = 8
        const val SESSIONS = 12
        const val PARTIES = 100
        val CONTENDERS = 1..10
        val LEASE: Duration = Duration.ofSeconds(30)
    }
}
