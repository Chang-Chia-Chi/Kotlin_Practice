package dynacache.cluster

import dynacache.cluster.proto.Envelope
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import dynacache.engine.Value
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Anti-entropy (spec 2.4) at its step function, `AntiEntropy.tick()`, through the test kit's
 * cluster: one range against one live replica per step, roots first, then the divergent keys
 * with their versions under spec 5.3 on both sides. Three nodes and N = 3, so every node
 * replicates every range and any node's cycle covers the whole ring.
 */
class AntiEntropyTest {

    private val ok = Reply.Simple("OK")

    /** Spec 6.7: one replica silently loses and rots data; one cycle on it makes every replica equal again. */
    @Test
    fun anti_entropy_heals_divergence() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 3, r = 1, scope = backgroundScope, partitionsPerNode = 2)
        val lost = Key("orders:1")
        val lostHash = Key("orders:2")
        val rotten = Key("orders:3")
        val coordinator = cluster.nodes.first()
        assertEquals(ok, cluster.writeVia(coordinator, lost, "v1".toByteArray()))
        assertEquals(Reply.Integer(2), cluster.settle(cluster.router(coordinator).submit(Command.HSet(lostHash, listOf(field("a", "1"), field("b", "2"))))))
        assertEquals(ok, cluster.writeVia(coordinator, rotten, "v3".toByteArray()))

        val victim = cluster.nodes.last()
        cluster.engine(victim).submit(Command.Del(lost)).await()
        cluster.engine(victim).submit(Command.Del(lostHash)).await()
        // Same version, different bytes: the replica's value rotted under it.
        cluster.engine(victim).submit(Command.Set(rotten, "rot".toByteArray())).await()
        assertNotEquals(cluster.readAllReplicas(rotten).values.toSet().size, 1)

        cluster.antiEntropyCycle(victim)

        assertEquals(setOf(Reply.Bulk("v1".toByteArray())), cluster.readAllReplicas(lost).values.toSet())
        assertEquals(setOf(Reply.Bulk("v3".toByteArray())), cluster.readAllReplicas(rotten).values.toSet())
        val hashes = cluster.nodes.map { fields(cluster.engine(it).submit(Command.HGetAll(lostHash)).await()) }
        assertEquals(setOf(mapOf("a" to "1", "b" to "2")), hashes.toSet())
        for (key in listOf(lost, lostHash, rotten)) {
            assertEquals(1, cluster.nodes.map { cluster.replication(it).version(key) }.toSet().size, "$key has one version cluster-wide")
        }
        cluster.close()
    }

    /**
     * One range per step, in ring order: a key lost on a replica is healed on exactly the step
     * that reaches its range and by none of the steps before it.
     */
    @Test
    fun anti_entropy_step_is_bounded() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 3, r = 1, scope = backgroundScope, partitionsPerNode = 2)
        val victim = cluster.nodes.last()
        val sync = cluster.antiEntropy(victim)
        val key = cluster.keyInRangeAfter(sync.ranges, first = 1)
        val at = sync.ranges.indexOf(cluster.ring.vnodeOf(key))
        assertEquals(ok, cluster.writeVia(cluster.nodes.first(), key, "v1".toByteArray()))
        cluster.engine(victim).submit(Command.Del(key)).await()

        repeat(at) { cluster.antiEntropyStep(victim) }
        assertEquals(at.toLong(), sync.rangesCompared.get(), "one range per step")
        assertEquals(0L, sync.keysSynced.get())
        assertEquals(Reply.Bulk(null), cluster.engine(victim).submit(Command.Get(key)).await(), "no earlier step touched $key's range")

        cluster.antiEntropyStep(victim)
        assertEquals(at + 1L, sync.rangesCompared.get())
        assertEquals(1L, sync.keysSynced.get())
        assertEquals(Reply.Bulk("v1".toByteArray()), cluster.engine(victim).submit(Command.Get(key)).await())
        cluster.close()
    }

    /** Equal replicas cost one root comparison and move no key, on either side. */
    @Test
    fun anti_entropy_noop_when_equal() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 3, r = 1, scope = backgroundScope, partitionsPerNode = 2)
        val node = cluster.nodes.first()
        val sync = cluster.antiEntropy(node)
        val key = cluster.keyInRangeAfter(sync.ranges, first = 0)
        assertEquals(ok, cluster.writeVia(node, key, "v1".toByteArray()))
        val versions = cluster.nodes.map { cluster.replication(it).version(key) }

        repeat(sync.ranges.indexOf(cluster.ring.vnodeOf(key)) + 1) { cluster.antiEntropyStep(node) }

        assertEquals(sync.ranges.indexOf(cluster.ring.vnodeOf(key)) + 1L, sync.rangesCompared.get())
        assertEquals(0L, cluster.nodes.sumOf { cluster.antiEntropy(it).keysSynced.get() })
        assertEquals(versions, cluster.nodes.map { cluster.replication(it).version(key) })
        assertEquals(setOf(Reply.Bulk("v1".toByteArray())), cluster.readAllReplicas(key).values.toSet())
        cluster.close()
    }

    /**
     * Spec 5.3's concurrent case: two replicas hold sibling versions of one key; after the
     * step both hold the type merge (T29: the last writer's string) under a version descending
     * from both. The third replica holds the second sibling too, so whichever peer the step
     * picks, it is a merge and not an install.
     */
    @Test
    fun anti_entropy_merges_concurrent_siblings() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 3, r = 1, scope = backgroundScope, partitionsPerNode = 2)
        val (one, two, three) = cluster.nodes
        val sync = cluster.antiEntropy(one)
        val key = cluster.keyInRangeAfter(sync.ranges, first = 0)
        val a = Dvv(Dot(one, 1), emptyMap())
        val b = Dvv(Dot(two, 1), emptyMap())
        cluster.seed(one, key, "a".toByteArray(), a)
        cluster.seed(two, key, "b".toByteArray(), b)
        cluster.seed(three, key, "b".toByteArray(), b)

        repeat(sync.ranges.indexOf(cluster.ring.vnodeOf(key)) + 1) { cluster.antiEntropyStep(one) }

        val merged = cluster.replication(one).version(key)!!
        assertEquals(true, merged.dominates(a) && merged.dominates(b), "$merged descends from both siblings")
        assertEquals(Reply.Bulk("b".toByteArray()), cluster.engine(one).submit(Command.Get(key)).await(), "the last writer's string")
        assertEquals(2, cluster.nodes.count { cluster.replication(it).version(key) == merged }, "the peer holds the same merged version")
        assertEquals(1L, sync.keysSynced.get())
        cluster.close()
    }

    /**
     * T84: one key of a large range rotted, and the exchange costs the descent rather than the
     * range. The step carries one node's children per level of the tree and the leaves of the
     * single subtree that differs, and names the one key; before T84 the same step carried all
     * 300 leaves on the peer's first reply.
     */
    @Test
    fun a_single_divergent_key_costs_a_descent_not_the_range() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 3, r = 1, scope = backgroundScope, partitionsPerNode = 2)
        val victim = cluster.nodes.last()
        val keys = cluster.fill(cluster.antiEntropy(victim).ranges.first(), count = 300)
        // Same version, different bytes: one leaf of the range differs and no position shifts.
        cluster.engine(victim).submit(Command.Set(keys.first(), "rot".toByteArray())).await()

        val carried = cluster.carry(victim)

        val height = MerkleTree.heightOf(keys.size)
        assertEquals(4, height, "300 keys under a fan-out of 16 stand four levels deep")
        assertEquals(height + 1, carried.requests, "the root, one request per level under it, the leaves, the key sync")
        assertTrue(carried.requests <= AntiEntropy.REQUESTS_PER_STEP, "${carried.requests} requests is inside the step's budget")
        assertEquals(MerkleTree.FANOUT * (height - 2), carried.hashes, "one node's children at each level and nothing else")
        assertEquals(MerkleTree.FANOUT, carried.leaves, "the leaves of the one subtree that differed, not the range's ${keys.size}")
        assertEquals(listOf(keys.first()), carried.synced, "the one key that really differs")
        assertEquals(1L, cluster.antiEntropy(victim).keysSynced.get())
        cluster.close()
    }

    /** Roots that agree still cost one comparison, however many keys stand under them. */
    @Test
    fun a_matching_range_still_costs_one_comparison() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 3, r = 1, scope = backgroundScope, partitionsPerNode = 2)
        val node = cluster.nodes.last()
        cluster.fill(cluster.antiEntropy(node).ranges.first(), count = 300)

        val carried = cluster.carry(node)

        assertEquals(1, carried.requests, "the root, and nothing opened under it")
        assertEquals(0, carried.hashes)
        assertEquals(0, carried.leaves)
        assertEquals(1L, cluster.antiEntropy(node).rangesCompared.get())
        assertEquals(0L, cluster.antiEntropy(node).keysSynced.get())
        cluster.close()
    }

    /** What one anti-entropy step put on the wire: the counts T84 is about. */
    private class Carried(val requests: Int, val hashes: Int, val leaves: Int, val synced: List<Key>)

    /** One step on [node], against the first range it walks, and what it cost the network. */
    private suspend fun InProcessCluster.carry(node: NodeId): Carried {
        val before = network.sent.size
        antiEntropyStep(node)
        val fresh = network.sent.drop(before)
        return Carried(
            requests = fresh.count { it.bodyCase in requests },
            hashes = fresh.sumOf { it.merkleLevelReply.hashCount },
            leaves = fresh.sumOf { it.merkleLevelReply.leafCount },
            synced = fresh.flatMap { it.keySync.keyList }.map { Key(it.toByteArray()) },
        )
    }

    private val requests = setOf(Envelope.BodyCase.MERKLE_ROOT, Envelope.BodyCase.MERKLE_LEVEL, Envelope.BodyCase.KEY_SYNC)

    /**
     * [count] keys of [range], each installed on every replica of it under one version, so the
     * replicas start the step agreeing. They share one hash tag, which is what puts a whole
     * range's worth of keys in one vnode of a 384-vnode ring (C12); under one tag the suffix
     * decides key order, so the keys come back in leaf order and the first sits at position 0.
     */
    private suspend fun InProcessCluster.fill(range: Vnode, count: Int): List<Key> {
        val tag = generateSequence(0) { it + 1 }.first { ring.vnodeOf(Key("{tag$it}")) == range }
        val keys = (0 until count).map { Key("{tag$tag}:%04d".format(it)) }
        val replicas = ring.preferenceList(range, n)
        for ((at, key) in keys.withIndex()) {
            val dvv = Dvv(Dot(nodes.first(), at + 1L), emptyMap())
            for (node in replicas) store(node).install(key, Value.Str("v$at".toByteArray()), null, dvv).await()
        }
        return keys
    }

    /** The first key `k1, k2, ...` whose range is at index [first] or later in [ranges]. */
    private fun InProcessCluster.keyInRangeAfter(ranges: List<Vnode>, first: Int): Key =
        generateSequence(1) { it + 1 }.map { Key("k$it") }.first { ranges.indexOf(ring.vnodeOf(it)) >= first }

    private fun field(name: String, value: String) = name.toByteArray() to value.toByteArray()

    /** An `HGETALL` reply as a map, since field order is the table's and not the test's business. */
    private fun fields(reply: Reply): Map<String, String> =
        (reply as Reply.Array).items.map { String((it as Reply.Bulk).bytes!!) }.chunked(2).associate { it[0] to it[1] }
}
