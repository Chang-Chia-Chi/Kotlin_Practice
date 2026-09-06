package dynacache.cluster

import dynacache.engine.Key
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.random.Random

/** The ring places keys on nodes (spec 2.4). It never decides a partition (ADR 0001). */
class RingTest {

    private val threeNodes = setOf(NodeId("alpha"), NodeId("bravo"), NodeId("charlie"))

    private fun keys(count: Int): List<Key> =
        Random(20260906).let { random -> List(count) { Key("key:" + random.nextLong()) } }

    @Test
    fun C3_preference_list_has_n_distinct_nodes() {
        val ring = Ring.of(threeNodes)

        val preferenceList = ring.preferenceList(Key("orders:4711"), 3)

        assertEquals(3, preferenceList.size)
        assertEquals(3, preferenceList.toSet().size)
        assertEquals(threeNodes, preferenceList.toSet())
    }

    @Test
    fun `a preference list longer than the node set is refused`() {
        val ring = Ring.of(threeNodes)

        assertThrows(IllegalArgumentException::class.java) {
            ring.preferenceList(Key("orders:4711"), 4)
        }
    }

    @Test
    fun I5_same_inputs_same_ring() {
        val one = Ring.of(threeNodes)
        val other = Ring.of(threeNodes.reversed().toSet())

        assertEquals(one.nodes, other.nodes)
        for (key in keys(200)) {
            assertEquals(one.preferenceList(key, 2), other.preferenceList(key, 2), key.toString())
        }
    }

    @Test
    fun ring_determinism() {
        val rings = List(3) { Ring.of(threeNodes) }

        for (key in keys(10_000)) {
            val expected = rings[0].preferenceList(key, 3)
            assertEquals(expected, rings[1].preferenceList(key, 3), key.toString())
            assertEquals(expected, rings[2].preferenceList(key, 3), key.toString())
        }
    }

    @Test
    fun ring_hash_tag_places_keys_together() {
        val ring = Ring.of(threeNodes)
        val one = Key("{user1}.a")
        val other = Key("{user1}.b")

        assertEquals(ring.positionOf(one), ring.positionOf(other))
        assertEquals(ring.preferenceList(one, 3), ring.preferenceList(other, 3))
    }

    @Test
    fun ring_load_is_even() {
        val ring = Ring.of(threeNodes)

        val load = keys(100_000)
            .groupingBy { ring.preferenceList(it, 1).single() }
            .eachCount()

        assertEquals(threeNodes, load.keys)
        val spread = load.values.max().toDouble() / load.values.min()
        assertTrue(spread < 1.25, "coordinator load spread was $spread over $load")
    }

    @Test
    fun `a key belongs to the vnode whose range holds its position`() {
        val ring = Ring.of(threeNodes)

        for (key in keys(1_000)) {
            val vnode = ring.vnodeOf(key)
            assertTrue(vnode.holds(ring.positionOf(key)), "$vnode misses $key")
            assertEquals(ring.preferenceList(key, 1).single(), vnode.owner, key.toString())
        }
    }

    @Test
    fun `the vnode ranges tile the ring end to end`() {
        val vnodes = Ring.of(threeNodes).vnodes

        assertEquals(threeNodes.size * Ring.VNODES_PER_NODE, vnodes.size)
        assertEquals(vnodes.last().position, vnodes.first().rangeStart)
        for (i in 1 until vnodes.size) {
            assertEquals(vnodes[i - 1].position, vnodes[i].rangeStart, "gap before vnode $i")
        }
    }
}
