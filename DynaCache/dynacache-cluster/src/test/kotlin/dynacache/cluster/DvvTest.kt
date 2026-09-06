package dynacache.cluster

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.random.Random

/** The dotted version vector is the clock on every value (spec 2.5, 5.3); merging values is T29. */
class DvvTest {

    private val alpha = NodeId("alpha")
    private val bravo = NodeId("bravo")

    @Test
    fun dvv_dominance_detection() {
        val a = Dvv(Dot(alpha, 1), emptyMap())
        val b = Dvv(Dot(bravo, 1), mapOf(alpha to 1L))

        assertTrue(b.dominates(a))
        assertFalse(a.dominates(b))
        assertFalse(a.dominates(a))
    }

    @Test
    fun dvv_concurrent_detection() {
        val onAlpha = Dvv(Dot(alpha, 1), emptyMap())
        val onBravo = Dvv(Dot(bravo, 1), emptyMap())

        assertTrue(onAlpha.isConcurrent(onBravo))
        assertTrue(onBravo.isConcurrent(onAlpha))
        assertFalse(onAlpha.dominates(onBravo))
        assertFalse(onAlpha.isConcurrent(onAlpha))
        assertFalse(Dvv(Dot(bravo, 1), mapOf(alpha to 1L)).isConcurrent(onAlpha))
    }

    @Test
    fun dvv_merge_preserves_causality() {
        val onAlpha = Dvv(Dot(alpha, 3), mapOf(bravo to 1L))
        val onBravo = Dvv(Dot(bravo, 2), mapOf(alpha to 2L))

        val merged = onAlpha.merge(onBravo, DotCounter.of(alpha, listOf(onAlpha, onBravo)))

        assertTrue(merged.dominates(onAlpha))
        assertTrue(merged.dominates(onBravo))
        assertEquals(Dot(alpha, 4), merged.dot)
        assertEquals(mapOf(alpha to 3L, bravo to 2L), merged.context)
    }

    @Test
    fun dvv_bounded_size() {
        val nodes = listOf(alpha, bravo, NodeId("charlie"))
        val counters = nodes.associateWith { DotCounter.of(it, emptyList()) }
        val random = Random(20260906)
        // One key; each node holds its own current version and merges what clients send.
        val stored = HashMap<NodeId, Dvv>()
        val clients = arrayOfNulls<Dvv>(100)

        repeat(10_000) {
            val client = random.nextInt(clients.size)
            val node = nodes[random.nextInt(nodes.size)]
            val counter = counters.getValue(node)
            // A client writes on what it last saw, or on a fresh read from a random node.
            val seen = clients[client] ?: stored[nodes[random.nextInt(nodes.size)]]
            val written = seen?.bump(counter) ?: Dvv(counter.next(), emptyMap())
            val local = stored[node]
            stored[node] = when {
                local == null || written.dominates(local) -> written
                local.isConcurrent(written) -> local.merge(written, counter)
                else -> local
            }
            clients[client] = stored.getValue(node)
            assertTrue(written.context.size <= nodes.size, "write $it: ${written.context}")
            assertTrue(stored.getValue(node).context.size <= nodes.size, "node $node: ${stored[node]}")
        }
    }

    @Test
    fun dvv_no_counter_reuse() {
        val beforeCrash = DotCounter.of(alpha, emptyList())
        val localData = List(5) { Dvv(beforeCrash.next(), emptyMap()) } +
            Dvv(Dot(bravo, 9), mapOf(alpha to 7L))

        val restarted = DotCounter.of(alpha, localData)

        assertEquals(Dot(alpha, 8), restarted.next())
        assertEquals(Dot(bravo, 10), DotCounter.of(bravo, localData).next())
    }

    @Test
    fun I4_later_write_dominates() {
        val onAlpha = DotCounter.of(alpha, emptyList())
        val onBravo = DotCounter.of(bravo, emptyList())
        var a = Dvv(onAlpha.next(), emptyMap())

        // A causal chain hopping between nodes: every link strictly dominates all before it.
        val chain = ArrayList<Dvv>()
        repeat(20) { i ->
            a = a.bump(if (i % 2 == 0) onBravo else onAlpha)
            for (earlier in chain) {
                assertTrue(a.dominates(earlier), "$a should dominate $earlier")
                assertFalse(earlier.dominates(a))
            }
            chain.add(a)
        }
    }

    @Test
    fun C2_counter_strictly_increases() {
        val counter = DotCounter.of(alpha, listOf(Dvv(Dot(alpha, 41), emptyMap())))
        val other = DotCounter.of(bravo, emptyList())

        var previous = counter.next()
        assertEquals(Dot(alpha, 42), previous)
        repeat(1_000) {
            // Merging in versions from elsewhere never moves alpha's counter backwards.
            Dvv(previous, emptyMap()).merge(Dvv(other.next(), emptyMap()), counter)
            val next = counter.next()
            assertEquals(alpha, next.node)
            assertTrue(next.counter > previous.counter, "$next after $previous")
            previous = next
        }
    }

    @Test
    fun dvv_encoding_roundtrip() {
        val random = Random(20260906)
        val nodes = listOf(alpha, bravo, NodeId("charlie"), NodeId("node-é中"))
        val samples = listOf(Dvv(Dot(alpha, 1), emptyMap()), Dvv(Dot(alpha, Long.MAX_VALUE), mapOf(bravo to 300L))) +
            List(200) {
                val context = nodes.filter { random.nextBoolean() }.associateWith { random.nextLong(1, 1L shl 40) }
                Dvv(Dot(nodes.random(random), random.nextLong(1, 1L shl 40)), context)
            }

        for (dvv in samples) {
            val bytes = dvv.encode()
            assertEquals(dvv, Dvv.decode(bytes), dvv.toString())
            assertArrayEquals(bytes, Dvv.decode(bytes).encode(), "encoding is canonical")
        }
        // Length byte, "alpha", one byte of counter, one byte saying "no context".
        assertEquals(1 + 5 + 1 + 1, Dvv(Dot(alpha, 1), emptyMap()).encode().size)
        assertThrows(IllegalArgumentException::class.java) { Dvv.decode(samples[1].encode() + 0) }
    }
}
