package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.SECONDS

/**
 * The CountDownLatch (CP spec 3.4, 6.4) through the CP engine's seam. A latch holds no session's
 * resources and has no lease, so nothing here touches a clock.
 */
class CountDownLatchTest {

    private val kit = CpTestKit()
    private val latch = Key("cp:latch:l")

    @AfterEach
    fun tearDown() = kit.close()

    private fun submit(command: Command): Reply =
        kit.leaderEngine().submit(command).get(REPLY_TIMEOUT_SECS, SECONDS)

    private fun down() = submit(Command.Cp.LatchDown(latch))

    @Test
    fun latch_set_down_get() {
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.LatchSet(latch, count = 3)))
        assertEquals(Reply.Integer(3), submit(Command.Cp.LatchGet(latch)))
        assertEquals(Reply.Integer(2), down())
        assertEquals(Reply.Integer(1), down())
        assertEquals(Reply.Integer(0), down())
    }

    /** The latch has already run out; counting down again cannot take it below zero. */
    @Test
    fun latch_down_at_zero_stays_zero() {
        assertEquals(Reply.Integer(0), down(), "a latch nobody set is already at zero")
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.LatchSet(latch, count = 1)))
        assertEquals(Reply.Integer(0), down())
        assertEquals(Reply.Integer(0), down(), "still zero")
    }

    /** Re-arming a latch parties are still waiting on would move the count under them. */
    @Test
    fun latch_reset_only_at_zero() {
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.LatchSet(latch, count = 3)))
        assertEquals(Reply.Integer(2), down())

        assertEquals("ERR", (submit(Command.Cp.LatchReset(latch, count = 5)) as Reply.Error).kind)
        assertEquals(Reply.Integer(2), submit(Command.Cp.LatchGet(latch)), "unchanged")

        assertEquals(Reply.Integer(1), down())
        assertEquals(Reply.Integer(0), down())
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.LatchReset(latch, count = 5)), "spent, so re-armable")
        assertEquals(Reply.Integer(5), submit(Command.Cp.LatchGet(latch)))
    }

    /** The log serializes the hundred count-downs, so none of them is lost to a race. */
    @Test
    fun latch_concurrent_down_correct_count() {
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.LatchSet(latch, count = PARTIES)))
        val engine = kit.leaderEngine()
        val clients = Executors.newFixedThreadPool(CLIENT_THREADS)
        try {
            val counted = (1..PARTIES).map {
                CompletableFuture.supplyAsync({ engine.submit(Command.Cp.LatchDown(latch)) }, clients)
            }
            val replies = counted.map { it.get(REPLY_TIMEOUT_SECS, SECONDS).get(REPLY_TIMEOUT_SECS, SECONDS) }

            // Each count-down saw a distinct value: together they are exactly 99 down to 0.
            assertEquals((0L until PARTIES).toSet(), replies.map { (it as Reply.Integer).value }.toSet())
            assertEquals(Reply.Integer(0), submit(Command.Cp.LatchGet(latch)))
        } finally {
            clients.shutdownNow()
        }
    }

    private companion object {
        const val REPLY_TIMEOUT_SECS = 10L
        const val PARTIES = 100
        const val CLIENT_THREADS = 8
    }
}
