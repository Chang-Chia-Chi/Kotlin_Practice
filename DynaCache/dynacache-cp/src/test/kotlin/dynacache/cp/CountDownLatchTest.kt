package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * The CountDownLatch (CP spec 3.4, 6.4) at the state machine. A latch holds no session's resources
 * and has no lease, so nothing here touches a clock. The hundred parties counting down at once are
 * [CpConcurrencyTest], which needs the log to serialize them.
 */
class CountDownLatchTest {

    private val cp = Primitives()
    private val latch = Key("cp:latch:l")

    private fun down() = cp.apply(Command.Cp.LatchDown(latch))

    @Test
    fun latch_set_down_get() {
        assertEquals(Reply.Simple("OK"), cp.apply(Command.Cp.LatchSet(latch, count = 3)))
        assertEquals(Reply.Integer(3), cp.apply(Command.Cp.LatchGet(latch)))
        assertEquals(Reply.Integer(2), down())
        assertEquals(Reply.Integer(1), down())
        assertEquals(Reply.Integer(0), down())
    }

    /** The latch has already run out; counting down again cannot take it below zero. */
    @Test
    fun latch_down_at_zero_stays_zero() {
        assertEquals(Reply.Integer(0), down(), "a latch nobody set is already at zero")
        assertEquals(Reply.Simple("OK"), cp.apply(Command.Cp.LatchSet(latch, count = 1)))
        assertEquals(Reply.Integer(0), down())
        assertEquals(Reply.Integer(0), down(), "still zero")
    }

    /** Re-arming a latch parties are still waiting on would move the count under them. */
    @Test
    fun latch_reset_only_at_zero() {
        assertEquals(Reply.Simple("OK"), cp.apply(Command.Cp.LatchSet(latch, count = 3)))
        assertEquals(Reply.Integer(2), down())

        assertEquals("ERR", (cp.apply(Command.Cp.LatchReset(latch, count = 5)) as Reply.Error).kind)
        assertEquals(Reply.Integer(2), cp.apply(Command.Cp.LatchGet(latch)), "unchanged")

        assertEquals(Reply.Integer(1), down())
        assertEquals(Reply.Integer(0), down())
        assertEquals(Reply.Simple("OK"), cp.apply(Command.Cp.LatchReset(latch, count = 5)), "spent, so re-armable")
        assertEquals(Reply.Integer(5), cp.apply(Command.Cp.LatchGet(latch)))
    }
}
