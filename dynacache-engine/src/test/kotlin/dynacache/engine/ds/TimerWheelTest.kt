package dynacache.engine.ds

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.Random

class TimerWheelTest {

    private val t0: Instant = Instant.EPOCH
    private val fired = mutableListOf<String>()

    private fun wheel(tickMillis: Long = 1000, slots: Int = 256) =
        TimerWheel<String>(t0, tickMillis, slots) { fired += it }

    private fun at(ms: Long): Instant = t0.plusMillis(ms)

    @Test
    fun wheel_fires_on_time() {
        val w = wheel()
        w.schedule("k", at(5000))

        w.advanceTo(at(4999))
        assertEquals(emptyList<String>(), fired)
        w.advanceTo(at(5000))
        assertEquals(listOf("k"), fired)
        w.advanceTo(at(60_000))
        assertEquals(listOf("k"), fired)
    }

    @Test
    fun wheel_no_early_fire() {
        val w = wheel()
        w.schedule("k", at(10_000))

        w.advanceTo(at(9999))
        assertEquals(emptyList<String>(), fired)
        w.advanceTo(at(10_000))
        assertEquals(listOf("k"), fired)
    }

    @Test
    fun wheel_cancel_prevents_fire() {
        val w = wheel()
        w.schedule("k", at(3000))
        w.schedule("other", at(3000))

        assertTrue(w.cancel("k"))
        assertFalse(w.cancel("k"))
        w.advanceTo(at(10_000))
        assertEquals(listOf("other"), fired)
    }

    @Test
    fun wheel_replace_ttl() {
        val w = wheel()
        w.schedule("k", at(2000))
        w.reschedule("k", at(8000))

        w.advanceTo(at(7999))
        assertEquals(emptyList<String>(), fired)
        w.advanceTo(at(8000))
        assertEquals(listOf("k"), fired)
        w.advanceTo(at(60_000))
        assertEquals(listOf("k"), fired)
    }

    @Test
    fun wheel_ordering() {
        val w = wheel()
        w.schedule("3s", at(3000))
        w.schedule("1s", at(1000))
        w.schedule("2s", at(2000))

        w.advanceTo(at(3000))
        assertEquals(listOf("1s", "2s", "3s"), fired)
    }

    @Test
    fun I7_fire_order_never_inverts() {
        val deadlines = HashMap<String, Long>()
        val w = wheel()
        val rnd = Random(7)
        repeat(5000) { i ->
            val ms = 1 + rnd.nextInt(200_000).toLong()
            deadlines["k$i"] = ms
            w.schedule("k$i", at(ms))
        }

        w.advanceTo(at(200_000))
        assertEquals(5000, fired.size)
        for (i in 1 until fired.size) {
            val earlier = deadlines.getValue(fired[i - 1])
            val later = deadlines.getValue(fired[i])
            assertTrue(earlier <= later, "${fired[i - 1]}@$earlier fired before ${fired[i]}@$later")
        }
    }

    @Test
    fun wheel_high_volume() {
        val n = 1_000_000
        val tick = 1000L
        val horizon = 3 * 24 * 3600 * 1000L   // past the 256 s and 65536 s levels
        val deadline = LongArray(n)
        val firedAt = LongArray(n) { -1 }
        var now = 0L
        val w = TimerWheel<Int>(t0, tick, 256) { firedAt[it] = now }
        val rnd = Random(42)
        for (i in 0 until n) {
            deadline[i] = 1 + rnd.nextLong().mod(horizon)
            w.schedule(i, at(deadline[i]))
        }

        while (now < horizon + tick) {
            now += tick
            w.advanceTo(at(now))
        }
        for (i in 0 until n) {
            val d = deadline[i]
            val f = firedAt[i]
            assertTrue(f >= d && f < d + tick, "key $i deadline $d fired at $f")
        }
    }

    @Test
    fun C7_never_fires_before_deadline() {
        // Four slots per level and a 1 ms tick: horizon 64 ms, so a 3 s spread of deadlines
        // cascades through every level and the overflow while the wheel is mid-turn.
        val tick = 1L
        val due = HashMap<Int, Long>()
        val firedAt = HashMap<Int, Long>()
        var now = 17L
        val w = TimerWheel<Int>(at(now), tick, 4) { firedAt[it] = now }
        val rnd = Random(3)
        var next = 0
        fun scheduleSome(count: Int) = repeat(count) {
            val ms = now - 5 + rnd.nextInt(3000)
            due[next] = maxOf(ms, now + tick)   // a past deadline is due on the next tick
            w.schedule(next++, at(ms))
        }

        scheduleSome(2000)
        while (now < 6000) {
            now += tick
            w.advanceTo(at(now))
            if (now < 3000 && now % 250 == 0L) scheduleSome(100)
            if (now == 500L) w.reschedule(0, at(now + 700)).also { due[0] = now + 700 }
        }

        assertEquals(due.keys, firedAt.keys)
        for ((k, d) in due) {
            val f = firedAt.getValue(k)
            assertTrue(f >= d && f < d + tick, "key $k due $d fired at $f")
        }
    }
}
