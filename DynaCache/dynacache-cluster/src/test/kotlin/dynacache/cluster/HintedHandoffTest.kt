package dynacache.cluster

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import dynacache.engine.testkit.MutableClock
import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

/**
 * Sloppy quorum and hinted handoff at the `CommandEngine` seam through the test kit's cluster
 * (spec 2.4, 5.1 step 7, C5, I9): a write whose replica is dead still reaches W through the
 * next healthy node on the ring, which holds the write as a hint and hands it back when the
 * replica returns.
 */
class HintedHandoffTest {

    private val key = Key("orders:4711")

    /**
     * Four nodes, N = 3, W = 3: the coordinator needs both other replicas. One is dead, so W
     * forms only if the fourth node stands in for it and its ack counts.
     */
    @Test
    fun sloppy_quorum_reaches_w_with_one_dead_node() = runTest {
        val cluster = InProcessCluster(nodeCount = 4, n = 3, w = 3, r = 1, scope = backgroundScope)
        val (coordinator, _, dead) = cluster.ring.preferenceList(key, 3)
        cluster.network.kill(dead)
        cluster.membership.set(dead, MemberState.DEAD)

        assertEquals(Reply.Simple("OK"), cluster.writeVia(coordinator, key, "v1".toByteArray()))
        assertEquals(Reply.Bulk(null), cluster.engine(dead).submit(Command.Get(key)).await())
        val standIn = cluster.ring.preferenceList(key, 4).last()
        assertEquals(1, cluster.replication(standIn).hintCount, "the next node on the ring holds the hint")
        cluster.close()
    }

    /**
     * Spec 6.7: one replica is cut off and declared dead, K keys it replicates are written,
     * the network heals, gossip sees it alive, and the hints drain into it.
     */
    @Test
    fun hinted_handoff_replays() = runTest {
        val cluster = InProcessCluster(nodeCount = 4, n = 3, w = 2, r = 2, scope = backgroundScope)
        val victim = cluster.nodes.last()
        val keys = cluster.keysReplicatedOn(victim, count = 5)
        cluster.partitionAway(victim)

        keys.forEachIndexed { i, key ->
            assertEquals(Reply.Simple("OK"), cluster.writeVia(cluster.nodes.first(), key, "v$i".toByteArray()))
        }
        keys.forEach { assertEquals(Reply.Bulk(null), cluster.engine(victim).submit(Command.Get(it)).await(), "$it before the heal") }

        cluster.rejoin(victim)
        keys.forEachIndexed { i, key ->
            assertEquals(Reply.Bulk("v$i".toByteArray()), cluster.engine(victim).submit(Command.Get(key)).await(), "$key after the heal")
        }
        cluster.close()
    }

    @Test
    fun hint_deleted_after_ack() = runTest {
        val cluster = InProcessCluster(nodeCount = 4, n = 3, w = 2, r = 2, scope = backgroundScope)
        val victim = cluster.nodes.last()
        val key = cluster.keysReplicatedOn(victim, count = 1).single()
        cluster.partitionAway(victim)
        assertEquals(Reply.Simple("OK"), cluster.writeVia(cluster.nodes.first(), key, "v1".toByteArray()))
        val holder = cluster.ring.preferenceList(key, 4).last()
        assertEquals(1, cluster.replication(holder).hintCount)

        cluster.rejoin(victim)
        assertEquals(0, cluster.replication(holder).hintCount, "the hint is gone once $victim acked it")
        assertEquals(Reply.Bulk("v1".toByteArray()), cluster.engine(victim).submit(Command.Get(key)).await())
        cluster.close()
    }

    /** C5: what the returned node holds is the write itself: value, version and TTL as one instant. */
    @Test
    fun C5_hint_carries_full_write() = runTest {
        val cluster = InProcessCluster(nodeCount = 4, n = 3, w = 2, r = 2, scope = backgroundScope)
        val victim = cluster.nodes.last()
        val key = cluster.keysReplicatedOn(victim, count = 1).single()
        val coordinator = cluster.ring.preferenceList(key, 3).first()
        cluster.partitionAway(victim)
        val set = Command.Set(key, "ttl'd".toByteArray(), ttl = Duration.ofSeconds(10))
        assertEquals(Reply.Simple("OK"), cluster.settle(cluster.router(cluster.nodes.first()).submit(set)))

        cluster.rejoin(victim)
        assertEquals(Reply.Bulk("ttl'd".toByteArray()), cluster.engine(victim).submit(Command.Get(key)).await())
        assertEquals(cluster.replication(coordinator).version(key), cluster.replication(victim).version(key))
        assertEquals(Reply.Integer(10_000), cluster.engine(victim).submit(Command.Ttl(key, Command.Ttl.Precision.MILLIS)).await())
        cluster.close()
    }

    /**
     * I9: K keys of mixed types and TTLs written while one replica is away; after it rejoins
     * and the hints drain, it matches a replica that never left, key by key, version included.
     */
    @Test
    fun I9_rejoined_node_matches_reference_replica() = runTest {
        val cluster = InProcessCluster(nodeCount = 4, n = 3, w = 2, r = 2, scope = backgroundScope)
        val victim = cluster.nodes.last()
        val keys = cluster.keysReplicatedOn(victim, count = 8)
        val writes = keys.mapIndexed { i, key ->
            when (i % 4) {
                0 -> Command.Set(key, "plain$i".toByteArray())
                1 -> Command.Set(key, "expiring$i".toByteArray(), ttl = Duration.ofSeconds(i + 1L))
                2 -> Command.HSet(key, listOf("f1".toByteArray() to "a$i".toByteArray(), "f2".toByteArray() to "b".toByteArray()))
                else -> Command.IncrBy(key, i.toLong())
            }
        }
        cluster.partitionAway(victim)
        writes.forEach { write -> cluster.settle(cluster.router(cluster.nodes.first()).submit(write)) }

        cluster.rejoin(victim)
        for ((key, write) in keys.zip(writes)) {
            val reference = cluster.ring.preferenceList(key, 3).first()
            val read = if (write is Command.HSet) Command.HGetAll(key) else Command.Get(key)
            assertEquals(cluster.engine(reference).submit(read).await(), cluster.engine(victim).submit(read).await(), "$key value")
            val ttl = Command.Ttl(key, Command.Ttl.Precision.MILLIS)
            assertEquals(cluster.engine(reference).submit(ttl).await(), cluster.engine(victim).submit(ttl).await(), "$key TTL")
            assertEquals(cluster.replication(reference).version(key), cluster.replication(victim).version(key), "$key version")
        }
        cluster.close()
    }

    /** A hint whose TTL ran out while its target was away is dropped, not replayed. */
    @Test
    fun expired_hint_is_not_replayed() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val cluster = InProcessCluster(nodeCount = 4, n = 3, w = 2, r = 2, scope = backgroundScope, clock = clock)
        val victim = cluster.nodes.last()
        val key = cluster.keysReplicatedOn(victim, count = 1).single()
        val holder = cluster.ring.preferenceList(key, 4).last()
        cluster.partitionAway(victim)
        val set = Command.Set(key, "short-lived".toByteArray(), ttl = Duration.ofSeconds(10))
        assertEquals(Reply.Simple("OK"), cluster.settle(cluster.router(cluster.nodes.first()).submit(set)))
        assertEquals(1, cluster.replication(holder).hintCount)

        clock.now = Instant.EPOCH.plusSeconds(11)
        cluster.rejoin(victim)
        assertEquals(0, cluster.replication(holder).hintCount, "an expired hint is dropped")
        assertNull(cluster.replication(victim).version(key), "$victim never received the write")
        cluster.close()
    }

    /** The first [count] keys `k1, k2, ...` that [node] replicates without coordinating. */
    private fun InProcessCluster.keysReplicatedOn(node: NodeId, count: Int): List<Key> =
        generateSequence(1) { it + 1 }.map { Key("k$it") }.filter { node in ring.preferenceList(it, n).drop(1) }.take(count).toList()

    /** Cuts [node] off from the rest and lets gossip declare it dead. */
    private fun InProcessCluster.partitionAway(node: NodeId) {
        network.networkPartition(listOf((nodes - node).toSet(), setOf(node)))
        membership.set(node, MemberState.DEAD)
    }

    /** Heals the network, lets gossip see [node] alive again, and drains the hints held for it. */
    private suspend fun InProcessCluster.rejoin(node: NodeId) {
        network.heal()
        membership.set(node, MemberState.ALIVE, incarnation = 1)
        drainHints()
    }
}
