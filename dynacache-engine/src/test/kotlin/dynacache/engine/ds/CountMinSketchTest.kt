package dynacache.engine.ds

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.Random

class CountMinSketchTest {

    @Test
    fun sketch_estimate_never_underestimates() {
        val sketch = CountMinSketch(width = 1024, seed = 11)
        val draw = Random(7)
        // The truth is counted in a map the sketch never sees, so the assertion has an
        // independent source and cannot agree with the sketch by construction.
        val truth = HashMap<String, Int>()
        repeat(4_000) {
            val key = "key-${draw.nextInt(500)}"
            sketch.increment(key)
            truth[key] = (truth[key] ?: 0) + 1
        }
        truth.forEach { (key, count) ->
            val estimate = sketch.estimate(key)
            assertTrue(estimate >= count, "$key was incremented $count times but estimates $estimate")
        }
    }

    @Test
    fun sketch_ages_halves_counts() {
        val sketch = CountMinSketch(width = 1024, seed = 11)
        repeat(9) { sketch.increment("a") }
        assertEquals(9, sketch.estimate("a"), "one key alone in the sketch collides with nothing")
        sketch.halve()
        assertEquals(4, sketch.estimate("a"), "halving rounds down")
        sketch.halve()
        assertEquals(2, sketch.estimate("a"))
        sketch.halve()
        assertEquals(1, sketch.estimate("a"))
        sketch.halve()
        assertEquals(0, sketch.estimate("a"), "a key that stops being touched ages out to nothing")
    }

    @Test
    fun sketch_counters_saturate_rather_than_wrap() {
        val sketch = CountMinSketch(width = 1024, seed = 11)
        repeat(400) { sketch.increment("a") }
        assertEquals(
            CountMinSketch.MAX_COUNT,
            sketch.estimate("a"),
            "a byte counter that wrapped would read far below its ceiling",
        )
    }
}
