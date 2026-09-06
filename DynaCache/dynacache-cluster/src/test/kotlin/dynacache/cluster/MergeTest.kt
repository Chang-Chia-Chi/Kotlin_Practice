package dynacache.cluster

import dynacache.engine.Value
import dynacache.engine.ds.HashTable
import dynacache.engine.ds.SkipList
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.random.Random

/** Spec 5.3 decides which side survives; the table of spec 2.5 says how two concurrent values combine. */
class MergeTest {

    private val alpha = NodeId("alpha")
    private val bravo = NodeId("bravo")
    private val charlie = NodeId("charlie")
    private val self = NodeId("zulu")

    @Test
    fun merge_string_concurrent_tiebreak_highest_node() {
        val onAlpha = firstWrite(alpha, str("from alpha"))
        val onBravo = firstWrite(bravo, str("from bravo"))

        assertEquals(canon(str("from bravo")), canon(merge(onAlpha, onBravo, counter(onAlpha, onBravo)).value))
        assertEquals(canon(str("from bravo")), canon(merge(onBravo, onAlpha, counter(onAlpha, onBravo)).value))
    }

    @Test
    fun merge_hash_field_level() {
        val onBravo = firstWrite(bravo, hash("f1" to "x", "f2" to "bravo's"))
        val onAlpha = firstWrite(alpha, hash("f2" to "alpha's", "f3" to "y"))
        val expected = canon(hash("f1" to "x", "f2" to "bravo's", "f3" to "y"))

        assertEquals(expected, canon(merge(onBravo, onAlpha, counter(onBravo, onAlpha)).value))
        assertEquals(expected, canon(merge(onAlpha, onBravo, counter(onBravo, onAlpha)).value))
    }

    @Test
    fun merge_list_union_of_concurrent_appends() {
        // Both pushed onto [a, b]: alpha c then d, bravo e. Both tails survive, the last writer's last.
        val onAlpha = firstWrite(alpha, list("a", "b", "c", "d"))
        val onBravo = firstWrite(bravo, list("a", "b", "e"))
        val expected = canon(list("a", "b", "c", "d", "e"))

        assertEquals(expected, canon(merge(onAlpha, onBravo, counter(onAlpha, onBravo)).value))
        assertEquals(expected, canon(merge(onBravo, onAlpha, counter(onAlpha, onBravo)).value))
    }

    @Test
    fun merge_list_concurrent_pop_last_writer() {
        // One side is a prefix of the other: somebody popped, and the last writer decides.
        val popped = firstWrite(bravo, list("a"))
        val kept = firstWrite(alpha, list("a", "b"))
        assertEquals(canon(list("a")), canon(merge(kept, popped, counter(kept, popped)).value))

        val poppedEarlier = firstWrite(alpha, list("a"))
        val keptLater = firstWrite(bravo, list("a", "b"))
        assertEquals(canon(list("a", "b")), canon(merge(poppedEarlier, keptLater, counter(poppedEarlier, keptLater)).value))
    }

    @Test
    fun merge_zset_union_max_score() {
        val onAlpha = firstWrite(alpha, zset("m1" to 1.0, "m2" to 5.0))
        val onBravo = firstWrite(bravo, zset("m2" to 3.0, "m3" to 2.0))
        val expected = canon(zset("m1" to 1.0, "m3" to 2.0, "m2" to 5.0))

        assertEquals(expected, canon(merge(onAlpha, onBravo, counter(onAlpha, onBravo)).value))
        assertEquals(expected, canon(merge(onBravo, onAlpha, counter(onAlpha, onBravo)).value))
    }

    @Test
    fun merge_different_kinds_concurrent_tiebreak() {
        val onAlpha = firstWrite(alpha, str("text"))
        val onBravo = firstWrite(bravo, list("item"))

        assertEquals(canon(list("item")), canon(merge(onAlpha, onBravo, counter(onAlpha, onBravo)).value))
        assertEquals(canon(list("item")), canon(merge(onBravo, onAlpha, counter(onAlpha, onBravo)).value))
    }

    /**
     * Three concurrent first writes per type, 25 seeded triples each. Commutativity and idempotence
     * are exact. Associativity is exact for a sorted set, and holds for a list's elements and a
     * hash's field names; which value a last-writer pick lands on (a string, a contested hash
     * field, the order of list tails) depends on the pairing, because the merged version carries
     * the coordinator's dot, not the winning writer's. Every merge still dominates its inputs, so
     * two nodes that paired differently converge on their next exchange.
     */
    @Test
    fun merge_is_commutative_associative_idempotent() {
        val random = Random(29)
        for (kind in Value.Kind.entries) repeat(25) {
            val (a, b, c) = listOf(alpha, bravo, charlie).map { firstWrite(it, sample(kind, it.name, random)) }

            assertSame(a, merge(a, a, counter(a)), "idempotent $kind")

            val ab = merge(a, b, counter(a, b))
            val ba = merge(b, a, counter(a, b))
            assertEquals(canon(ab.value), canon(ba.value), "commutative $kind")
            assertEquals(ab.dvv, ba.dvv)

            val abThenC = merge(ab, c, counter(ab, c))
            val aThenBc = merge(a, merge(b, c, counter(b, c)), counter(a, merge(b, c, counter(b, c))))
            assertEquals(abThenC.dvv, aThenBc.dvv)
            if (kind == Value.Kind.STRING) {
                val written = listOf(a, b, c).map { canon(it.value) }
                assertTrue(canon(abThenC.value) in written && canon(aThenBc.value) in written)
            } else {
                assertEquals(stable(abThenC.value), stable(aThenBc.value), "associative $kind")
            }
        }
    }

    @Test
    fun merge_dominated_side_is_discarded() {
        val older = firstWrite(alpha, str("old"))
        val newer = Versioned(str("new"), Dvv(Dot(bravo, 1), mapOf(alpha to 1L)))

        assertSame(newer, merge(older, newer, counter(older, newer)))
        assertSame(newer, merge(newer, older, counter(older, newer)))
    }

    @Test
    fun merge_equal_dvvs_keep_local() {
        val local = firstWrite(alpha, str("local"))
        val remote = Versioned(str("remote"), local.dvv)
        val counter = counter(local)

        assertSame(local, merge(local, remote, counter))
        assertEquals(Dot(self, 1), counter.next(), "an equal pair spends no dot")
    }

    @Test
    fun merge_result_dvv_descends_from_both() {
        val onAlpha = Versioned(str("a"), Dvv(Dot(alpha, 3), mapOf(bravo to 1L)))
        val onBravo = Versioned(str("b"), Dvv(Dot(bravo, 2), mapOf(alpha to 2L)))

        val merged = merge(onAlpha, onBravo, counter(onAlpha, onBravo)).dvv

        assertTrue(merged.dominates(onAlpha.dvv))
        assertTrue(merged.dominates(onBravo.dvv))
        assertEquals(Dot(self, 1), merged.dot)
    }

    /** A node's first write of a key: nothing seen, so two of them on different nodes are concurrent. */
    private fun firstWrite(node: NodeId, value: Value) = Versioned(value, Dvv(Dot(node, 1), emptyMap()))

    private fun counter(vararg seen: Versioned) = DotCounter.of(self, seen.map { it.dvv })

    /** One writer's concurrent version of a key: lists share a base and push their own items after it. */
    private fun sample(kind: Value.Kind, writer: String, random: Random): Value {
        val n = 1 + random.nextInt(4)
        return when (kind) {
            Value.Kind.STRING -> str("$writer ${random.nextInt(100)}")
            Value.Kind.HASH -> hash(*Array(n) { "f${random.nextInt(5)}" to "$writer $it" })
            Value.Kind.LIST -> list("base0", "base1", *Array(n) { "$writer $it" })
            Value.Kind.ZSET -> zset(*Array(n) { "m${random.nextInt(5)}" to random.nextInt(10).toDouble() })
        }
    }

    private fun str(text: String) = Value.Str(text.toByteArray())

    private fun list(vararg items: String) = Value.List(ArrayDeque(items.map { it.toByteArray() }))

    private fun zset(vararg members: Pair<String, Double>) =
        Value.ZSet(SkipList(1)).apply { for ((member, score) in members) writeScore(score, member.toByteArray()) }

    private fun hash(vararg fields: Pair<String, String>) =
        Value.Hash(HashTable<String, ByteArray>().apply { for ((name, text) in fields) put(name, text.toByteArray()) })

    /** The part of a merged value that no pairing order can change. */
    private fun stable(value: Value): Any = when (value) {
        is Value.Hash -> value.fields.entries().map { it.key }.toSet()
        is Value.List -> value.items.map { String(it) }.sorted()
        else -> canon(value)
    }
}
