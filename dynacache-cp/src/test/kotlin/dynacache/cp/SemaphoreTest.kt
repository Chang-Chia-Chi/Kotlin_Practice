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
 * The Semaphore (CP spec 3.3, 6.3) through the CP engine's seam: permits are counted per key and
 * owned per session, so a session's death gives its permits back (I15).
 */
class SemaphoreTest {

    private val kit = CpTestKit()
    private val sem = Key("cp:sem:s")

    /** These tests name their sessions by number, so 1 to [SESSIONS] are registered and alive. */
    @BeforeEach
    fun registerSessions() {
        repeat(SESSIONS) { submit(Command.Cp.SessionCreate(Duration.ofHours(1))) }
    }

    @AfterEach
    fun tearDown() = kit.close()

    private fun submit(command: Command): Reply =
        kit.leaderEngine().submit(command).get(REPLY_TIMEOUT_SECS, SECONDS)

    private fun available() = submit(Command.Cp.SemAvailable(sem))

    private fun acquire(session: Long, permits: Int) = submit(Command.Cp.SemAcquire(sem, session, permits))

    private fun release(session: Long, permits: Int) = submit(Command.Cp.SemRelease(sem, session, permits))

    @Test
    fun sem_init_acquire_release() {
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.SemInit(sem, permits = 5)))
        assertEquals(Reply.Integer(1), acquire(session = 1, permits = 2))
        assertEquals(Reply.Integer(3), available())
        assertEquals(Reply.Simple("OK"), release(session = 1, permits = 2))
        assertEquals(Reply.Integer(5), available())
    }

    /** Asking for more than is free changes nothing; the caller retries rather than blocking. */
    @Test
    fun sem_over_acquire_fails() {
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.SemInit(sem, permits = 3)))
        assertEquals(Reply.Integer(0), acquire(session = 1, permits = 5))
        assertEquals(Reply.Integer(3), available(), "nothing was taken")
    }

    /** A session may only give back what it took, or a semaphore would grow permits from nowhere. */
    @Test
    fun sem_over_release_rejected() {
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.SemInit(sem, permits = 5)))
        assertEquals(Reply.Integer(1), acquire(session = 1, permits = 2))

        assertEquals("ERR", (release(session = 1, permits = 3) as Reply.Error).kind)

        assertEquals(Reply.Integer(3), available(), "state unchanged")
        assertEquals(Reply.Simple("OK"), release(session = 1, permits = 2), "what it does hold is still its own")
    }

    /** I15: "wait session_timeout" is the leader's clock moving and a tick carrying it into the log. */
    @Test
    fun sem_session_death_releases() {
        val leader = kit.leader()
        val session = (submit(Command.Cp.SessionCreate(Duration.ofSeconds(1))) as Reply.Integer).value
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.SemInit(sem, permits = 5)))
        assertEquals(Reply.Integer(1), acquire(session, permits = 2))
        assertEquals(Reply.Integer(3), available())

        kit.clock(leader.config.nodeId).advance(Duration.ofSeconds(2))
        leader.tick().get(REPLY_TIMEOUT_SECS, SECONDS)

        assertEquals(Reply.Integer(5), available(), "the dead session's permits came back")
    }

    /** DRAIN takes whatever is free, so a drained semaphore hands out nothing until a release. */
    @Test
    fun sem_drain() {
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.SemInit(sem, permits = 5)))
        assertEquals(Reply.Integer(5), submit(Command.Cp.SemDrain(sem, session = 1)))
        assertEquals(Reply.Integer(0), available())
        assertEquals(Reply.Integer(0), acquire(session = 2, permits = 1), "nothing left to give")
    }

    /** Draining a semaphore nobody initialised takes nothing, and must not invent one holding nothing. */
    @Test
    fun sem_drain_of_unknown_key_leaves_it_initialisable() {
        assertEquals(Reply.Integer(0), submit(Command.Cp.SemDrain(sem, session = 1)))

        assertEquals(Reply.Simple("OK"), submit(Command.Cp.SemInit(sem, permits = 5)))
        assertEquals(Reply.Integer(5), available(), "the drain left no semaphore behind")
    }

    /** The log serializes the ten attempts, so exactly the three permits that exist are handed out. */
    @Test
    fun sem_concurrent_acquire_exactly_permits_succeed() {
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.SemInit(sem, permits = 3)))
        val engine = kit.leaderEngine()
        val clients = Executors.newFixedThreadPool(CLIENT_THREADS)
        try {
            val attempts = (1L..CONTENDERS).map { session ->
                CompletableFuture.supplyAsync({ engine.submit(Command.Cp.SemAcquire(sem, session, 1)) }, clients)
            }
            val replies = attempts.map { it.get(REPLY_TIMEOUT_SECS, SECONDS).get(REPLY_TIMEOUT_SECS, SECONDS) }

            assertEquals(3, replies.count { it == Reply.Integer(1) }, "exactly the permits that exist: $replies")
            assertEquals(Reply.Integer(0), available())
        } finally {
            clients.shutdownNow()
        }
    }

    private companion object {
        const val REPLY_TIMEOUT_SECS = 10L
        const val SESSIONS = 12
        const val CLIENT_THREADS = 8
        const val CONTENDERS = 10L
    }
}
