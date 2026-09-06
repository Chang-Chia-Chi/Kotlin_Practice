package dynacache.engine.ds

import java.util.Random

/**
 * A Count-Min Sketch: how often a key has been seen, in a fixed amount of memory that does not
 * grow with the keyspace. [depth] independent rows of [width] counters; [increment] raises one
 * counter per row and [estimate] answers with the smallest of them, so a collision can only ever
 * make the answer too high. That one-sided error is the whole point -- an eviction policy that
 * over-rates a cold key loses a little hit ratio, one that under-rates a hot key throws it away.
 *
 * Counters are bytes saturating at [MAX_COUNT] rather than Caffeine's packed 4-bit nibbles: one
 * byte per counter is four times the memory of a nibble and none of the shifting, and at
 * `depth * width` counters the whole sketch is still kilobytes. The ceiling is real, though --
 * past [MAX_COUNT] increments of one key the sketch does under-report it -- and [halve] is what
 * keeps live counts far below it in practice.
 *
 * ponytail: byte counters, 4x a nibble sketch's memory; pack two per byte if a partition ever
 * holds enough keys for the sketch to matter.
 *
 * Not thread-safe: one sketch belongs to one partition and is only ever touched from that
 * partition's own thread.
 */
class CountMinSketch(width: Int, private val depth: Int = 4, seed: Long = 0) {

    /** Rounded up to a power of two, so a row's slot is a mask rather than a modulo. */
    private val mask: Int = Integer.highestOneBit(maxOf(1, width - 1)) * 2 - 1

    /** One row per hash function, laid end to end: row `r`'s counters start at `r * (mask + 1)`. */
    private val counters = ByteArray(depth * (mask + 1))

    /**
     * One odd multiplier per row, drawn from [seed]. Odd so the multiplication is a bijection on
     * 32 bits and no row collapses two keys the other rows kept apart.
     */
    private val seeds = IntArray(depth) { Random(seed + it).nextInt() or 1 }

    /** Records one more sighting of [key]. Counters at [MAX_COUNT] stay there. */
    fun increment(key: Any) {
        for (row in 0 until depth) {
            val slot = slot(key, row)
            val count = counters[slot].toInt() and 0xFF
            if (count < MAX_COUNT) counters[slot] = (count + 1).toByte()
        }
    }

    /** How often [key] has been seen, never less than the truth while no counter has saturated. */
    fun estimate(key: Any): Int {
        var smallest = MAX_COUNT
        for (row in 0 until depth) {
            val count = counters[slot(key, row)].toInt() and 0xFF
            if (count < smallest) smallest = count
        }
        return smallest
    }

    /**
     * Ages every counter by halving it. Without this a key that was hot once stays hot forever in
     * the sketch and a newly hot key can never beat it (frequency fossilization, spec 2.7); with
     * it, an untouched key falls to zero in a bounded number of samples while a key that is still
     * being touched climbs straight back.
     */
    fun halve() {
        for (slot in counters.indices) counters[slot] = ((counters[slot].toInt() and 0xFF) ushr 1).toByte()
    }

    /**
     * [key]'s counter in [row]. The row's own multiplier makes the rows independent, and the
     * xor-shift spreads a `hashCode` whose entropy sits in its low bits over the whole word.
     */
    private fun slot(key: Any, row: Int): Int {
        val mixed = key.hashCode() * seeds[row]
        return row * (mask + 1) + ((mixed xor (mixed ushr 16)) and mask)
    }

    companion object {
        /** The ceiling of one byte counter; a count past this is reported as this. */
        const val MAX_COUNT = 255
    }
}
