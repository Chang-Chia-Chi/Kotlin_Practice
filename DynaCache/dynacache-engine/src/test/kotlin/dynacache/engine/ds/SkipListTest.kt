package dynacache.engine.ds

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.math.ln
import kotlin.random.Random

class SkipListTest {

    @Test
    fun skiplist_insert_order() {
        val list = SkipList(seed = 42)
        val data = Random(7)
        val scores = (1..500).map { data.nextDouble(-1000.0, 1000.0) }

        scores.forEachIndexed { i, score -> assertTrue(list.insert(score, member("m$i"))) }

        assertEquals(500, list.size)
        assertEquals(scores.sorted(), list.forward().map { it.score }.toList())
    }

    @Test
    fun skiplist_duplicate_score_lex_order() {
        val list = SkipList(seed = 42)
        val scrambled = listOf("delta", "ab", "a", "charlie", "alpha", "b")

        scrambled.forEach { assertTrue(list.insert(1.5, member(it))) }
        list.insert(1.5, byteArrayOf(-1))
        list.insert(1.5, byteArrayOf(1))

        val ordered = list.forward().map { text(it.member) }.toList()
        assertEquals(listOf("", "a", "ab", "alpha", "b", "charlie", "delta", "ÿ"), ordered)
        assertTrue(list.forward().all { it.score == 1.5 })
    }

    @Test
    fun skiplist_delete_preserves_order() {
        val list = SkipList(seed = 42)
        val data = Random(11)
        val entries = (1..300).map { i -> Entry(data.nextInt(50).toDouble(), member("m$i")) }
        entries.forEach { list.insert(it.score, it.member) }

        val doomed = entries.filterIndexed { i, _ -> i % 3 == 0 }
        doomed.forEach { assertTrue(list.remove(it.score, it.member)) }
        doomed.forEach { assertFalse(list.remove(it.score, it.member)) }
        assertFalse(list.remove(999.0, member("never inserted")))

        val survivors = entries - doomed.toSet()
        assertEquals(survivors.size, list.size)
        assertEquals(survivors.sortedWith(byScoreThenMember), list.forward().toList())
    }

    @Test
    fun skiplist_rank_correct() {
        val list = SkipList(seed = 42)
        val data = Random(13)
        val entries = (1..400).map { i -> Entry(data.nextInt(80).toDouble(), member("m$i")) }
        entries.forEach { list.insert(it.score, it.member) }
        val doomed = entries.filterIndexed { i, _ -> i % 5 == 0 }
        doomed.forEach { list.remove(it.score, it.member) }

        val survivors = (entries - doomed.toSet()).sortedWith(byScoreThenMember)
        survivors.forEachIndexed { rank, e -> assertEquals(rank, list.rank(e.score, e.member)) }
        assertEquals(-1, list.rank(999.0, member("never inserted")))
        assertEquals(-1, list.rank(survivors[0].score, member("never inserted")))
        assertEquals(-1, list.rank(999.0, survivors[0].member))
    }

    @Test
    fun skiplist_range_query() {
        val list = SkipList(seed = 42)
        val data = Random(17)
        val entries = (1..300).map { i -> Entry(data.nextInt(100).toDouble(), member("m$i")) }
        entries.forEach { list.insert(it.score, it.member) }
        val sorted = entries.sortedWith(byScoreThenMember)

        assertEquals(sorted.filter { it.score >= 20.0 && it.score <= 40.0 }, list.rangeByScore(20.0, 40.0))
        assertEquals(
            sorted.filter { it.score > 20.0 && it.score < 40.0 },
            list.rangeByScore(20.0, 40.0, minInclusive = false, maxInclusive = false),
        )
        assertEquals(sorted, list.rangeByScore(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY))
        assertEquals(emptyList<Entry>(), list.rangeByScore(40.0, 20.0))
        assertEquals(emptyList<Entry>(), list.rangeByScore(1000.0, 2000.0))
    }

    @Test
    fun range_by_rank_reads_positions_and_reverse_traversal_reads_them_backwards() {
        val list = SkipList(seed = 42)
        val data = Random(19)
        val entries = (1..200).map { i -> Entry(data.nextInt(40).toDouble(), member("m$i")) }
        entries.forEach { list.insert(it.score, it.member) }
        val sorted = entries.sortedWith(byScoreThenMember)

        assertEquals(sorted.subList(0, 10), list.rangeByRank(0, 9))
        assertEquals(sorted.subList(50, 61), list.rangeByRank(50, 60))
        assertEquals(sorted, list.rangeByRank(0, sorted.size - 1))
        assertEquals(sorted.subList(190, 200), list.rangeByRank(190, 500))
        assertEquals(emptyList<Entry>(), list.rangeByRank(10, 9))
        assertEquals(emptyList<Entry>(), list.rangeByRank(500, 600))
        assertEquals(sorted.asReversed(), list.backward().toList())

        list.remove(sorted.last().score, sorted.last().member)
        list.remove(sorted.first().score, sorted.first().member)
        val remaining = sorted.subList(1, sorted.size - 1)
        assertEquals(remaining.asReversed(), list.backward().toList())
        assertEquals(remaining, list.rangeByRank(0, list.size - 1))
    }

    @Test
    fun updating_a_score_moves_the_member_and_keeps_the_order() {
        val list = SkipList(seed = 42)
        listOf("a" to 1.0, "b" to 2.0, "c" to 3.0, "d" to 4.0).forEach { (m, s) -> list.insert(s, member(m)) }

        assertTrue(list.updateScore(2.0, member("b"), 10.0))
        assertEquals(listOf("a", "c", "d", "b"), list.forward().map { text(it.member) }.toList())
        assertEquals(3, list.rank(10.0, member("b")))
        assertEquals(-1, list.rank(2.0, member("b")))
        assertEquals(4, list.size)

        assertFalse(list.updateScore(99.0, member("b"), 1.0))
        assertFalse(list.updateScore(1.0, member("never inserted"), 5.0))
        assertTrue(list.updateScore(10.0, member("b"), 10.0))
        assertEquals(4, list.size)
        assertEquals(3, list.rank(10.0, member("b")))
    }

    @Test
    fun skiplist_log_n_property() {
        val n = 100_000
        val list = SkipList(seed = 42)
        val data = Random(23)
        val entries = (1..n).map { i -> Entry(data.nextDouble(0.0, 1_000_000.0), member("m$i")) }
        entries.forEach { list.insert(it.score, it.member) }
        assertEquals(n, list.size)

        val probe = Random(29)
        val searches = 2_000
        val before = list.comparisons
        repeat(searches) {
            val wanted = entries[probe.nextInt(n)]
            assertTrue(list.rank(wanted.score, wanted.member) >= 0)
        }
        val perSearch = (list.comparisons - before).toDouble() / searches

        val bound = 2 * ln(n.toDouble()) / ln(2.0)
        assertTrue(perSearch <= bound, "$perSearch comparisons per search, bound is $bound")
    }

    /**
     * Ranks come from the spans an insert and a remove maintain by hand, and the levels a list
     * grows and drops as it changes shape. Only interleaving the two exercises that, so this
     * walks a seeded storm against a plain sorted model.
     */
    @Test
    fun I3_ranks_and_order_survive_an_interleaved_insert_and_remove_storm() {
        val list = SkipList(seed = 42)
        val data = Random(31)
        val model = mutableListOf<Entry>()

        repeat(3_000) { step ->
            val entry = Entry(data.nextInt(30).toDouble(), member("m${data.nextInt(40)}"))
            val known = model.indexOfFirst { it == entry }
            if (data.nextInt(3) == 0 && model.isNotEmpty()) {
                val victim = model[data.nextInt(model.size)]
                assertTrue(list.remove(victim.score, victim.member))
                model.remove(victim)
            } else if (known < 0) {
                assertTrue(list.insert(entry.score, entry.member))
                model.add(entry)
            } else {
                assertFalse(list.insert(entry.score, entry.member))
            }

            if (step % 250 == 0) {
                model.sortWith(byScoreThenMember)
                assertEquals(model.size, list.size)
                assertEquals(model, list.forward().toList())
                assertEquals(model.asReversed(), list.backward().toList())
                model.forEachIndexed { rank, e -> assertEquals(rank, list.rank(e.score, e.member)) }
                assertEquals(model, list.rangeByRank(0, list.size - 1))
            }
        }
    }

    private fun member(text: String): ByteArray = text.toByteArray()

    private fun text(bytes: ByteArray): String = bytes.toString(Charsets.ISO_8859_1)

    private val byScoreThenMember = compareBy<Entry>({ it.score }, { text(it.member) })
}
