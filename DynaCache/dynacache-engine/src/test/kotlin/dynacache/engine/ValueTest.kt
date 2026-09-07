package dynacache.engine

import dynacache.engine.ds.SkipList
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.Random

/**
 * The running byte total every aggregate keeps: what a value says it costs is what a from-scratch
 * recount says it costs, after every step of a seeded sequence. The total is arithmetic, and
 * arithmetic drifts the moment one mutation forgets to book its side; recounting after each step
 * is what turns that drift into a failing assertion rather than a wrong `INFO` and a partition
 * that evicts at the wrong moment (spec 2.7, 5.5, I6).
 *
 * The store's own side of the same invariant -- that its running total is the sum over its
 * entries -- is [PartitionStoreTest.store_used_bytes_equals_sum_of_entries_after_any_sequence].
 */
class ValueTest {

    /** Pushes at both ends, pops at both ends, a set in place, a removal and a trim. */
    @Test
    fun list_running_total_matches_a_recount_after_every_step() {
        val draw = Random(20260907)
        val list = Value.List()
        val items = list.items
        var longest = 0
        repeat(STEPS) { step ->
            // Pushes outweigh the rest, so the list grows over the run and the later steps are
            // taken on a list long enough that a walk would be the wrong thing to do.
            when (draw.nextInt(12)) {
                in 0..3 -> items.addFirst(element(draw))
                in 4..6 -> items.addLast(element(draw))
                7 -> if (items.isNotEmpty()) items.removeFirst()
                8 -> items.removeLastOrNull()
                // `LSET`: one element replaced in place by one of another size.
                9 -> if (items.isNotEmpty()) items[draw.nextInt(items.size)] = element(draw)
                // `LREM`: a match dropped out of the middle.
                10 -> if (items.isNotEmpty()) items.removeAt(draw.nextInt(items.size))
                // A trim: the tail dropped element by element.
                else -> repeat(minOf(items.size, draw.nextInt(5))) { items.removeLast() }
            }
            longest = maxOf(longest, items.size)
            assertEquals(recount(list), list.approximateBytes(), "step $step drifted, on ${items.size} elements")
        }
        assertTrue(longest > 100, "the sequence never grew a list worth counting: $longest at its longest")
    }

    /** Fields and scored members written, overwritten at another size, and taken out again. */
    @Test
    fun hash_and_zset_running_totals_match_a_recount_after_every_step() {
        val draw = Random(20260906)
        val hash = Value.Hash()
        val zset = Value.ZSet(SkipList(11))
        repeat(STEPS) { step ->
            when (draw.nextInt(4)) {
                0 -> hash.fields.put(name(draw), element(draw))
                1 -> hash.fields.remove(name(draw))
                2 -> zset.writeScore(draw.nextDouble(), name(draw).toByteArray())
                3 -> zset.removeMember(name(draw).toByteArray())
            }
            assertEquals(recount(hash), hash.approximateBytes(), "step $step drifted, on the hash")
            assertEquals(recount(zset), zset.approximateBytes(), "step $step drifted, on the sorted set")
        }
        assertTrue(hash.fields.size > 0, "the sequence emptied the hash and proved nothing about a full one")
        assertEquals(zset.scores.size, zset.order.size, "the two indexes still hold the same members")
    }

    /** An element of no fixed size, so a replacement books a delta rather than nothing. */
    private fun element(draw: Random) = ByteArray(draw.nextInt(40))

    /** A field or member name of no fixed length, so the name's own bytes are part of the total. */
    private fun name(draw: Random) = "f%d".format(draw.nextInt(1000))

    /**
     * What [value] costs counted from scratch, the way [Value.approximateBytes] itself counted it
     * before the total became a running one. The independent count is the whole point: comparing
     * a total against itself would agree forever.
     */
    private fun recount(value: Value): Long = when (value) {
        is Value.Str -> value.bytes.size.toLong()
        is Value.Hash -> value.fields.entries().sumOf { it.key.length + it.value.size + Value.ELEMENT_BYTES }
        is Value.List -> value.items.sumOf { it.size + Value.ELEMENT_BYTES }
        is Value.ZSet -> value.scores.entries().sumOf { it.key.length + Long.SIZE_BYTES + Value.ELEMENT_BYTES }
    }

    private companion object {
        const val STEPS = 2_000
    }
}
