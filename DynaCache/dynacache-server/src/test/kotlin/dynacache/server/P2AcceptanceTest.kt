package dynacache.server

import dynacache.cluster.HostPort
import dynacache.cluster.NodeId
import dynacache.cluster.ReplicationConfig
import dynacache.cluster.Ring
import dynacache.engine.Key
import dynacache.engine.Reply
import dynacache.engine.testkit.MutableClock
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.milliseconds

/**
 * Spec 9's cluster half, up to "kill node 2": three nodes in one JVM on ephemeral RESP and gRPC
 * ports, real gossip over real sockets, N=3 W=2 R=2, driven by Jedis unmodified (C8). The
 * client never learns that a cluster exists -- it writes through one node, reads through
 * another, and neither knows nor cares which node coordinates a key.
 *
 * Every node reads one clock the test owns (plan rule 1.5), so a TTL that crosses the quorum is
 * measured against a "now" this test sets rather than against the machine's. SWIM is the one
 * thing left on real time: it counts its own gossip periods and reads no clock at all, so the
 * one wait here is a bounded poll for its coroutine and there is no sleep anywhere.
 */
class P2AcceptanceTest {

    private val ids = List(3) { NodeId("node-${it + 1}") }

    /** The one clock all three nodes read; it never moves, so no deadline here arrives by itself. */
    private val clock = MutableClock(Instant.parse("2026-09-06T00:00:00Z"))

    /** The same ring every node builds, so the test can say which node coordinates a key. */
    private val ring = Ring.of(ids.toSet())

    /**
     * Spec 9's `foo`, unless the ring happens to give it to the node this test kills: every
     * assertion after the kill is about a key whose coordinator is still up, because a dead
     * coordinator makes its own keys unavailable until T25's sloppy quorum has somewhere to put
     * them (T22 deviation 7, four nodes needed for a substitute).
     */
    private val surviving = if (coordinatorOf("foo") != ids[1]) "foo" else keyCoordinatedBy(ids[0])

    /** One map all three nodes read at send time: ephemeral ports are only known once bound. */
    private val addresses = ConcurrentHashMap<NodeId, HostPort>()

    private val nodes = ids.map { id ->
        ClusterNode(
            self = id,
            nodes = ids.toSet(),
            addresses = addresses,
            respPort = 0,
            grpcPort = 0,
            config = ReplicationConfig(n = 3, w = 2, r = 2),
            partitionCount = 4,
            clock = clock,
            // Fast enough that a death is detected inside a test's patience, slow enough to gossip.
            gossipPeriod = 100.milliseconds,
        )
    }

    @BeforeEach
    fun startCluster() {
        nodes.forEach { addresses[it.self] = HostPort("127.0.0.1", it.grpcPort) }
        nodes.forEach { it.start() }
    }

    @AfterEach
    fun stopCluster() {
        nodes.forEach { runCatching { it.close() } }
    }

    @Test
    fun P2_acceptance_three_nodes_quorum_and_minority_failure() {
        Jedis("127.0.0.1", nodes[0].respPort).use { one ->
            Jedis("127.0.0.1", nodes[2].respPort).use { three ->
                theWholeClusterIsKnown(one)
                writtenThroughOneReadThroughThree(one, three)
                aKeyThisContactDoesNotCoordinate(one, three)
                oneBatchNeedsOneCoordinator(one)
                nodes[1].close()
                gossipSeesTheDeadNode(one)
                readsAndWritesStillSucceed(one, three)
                whatTheDeadNodeCoordinated(one)
            }
        }
    }

    /** `INFO`'s `# Cluster` section: who this node is and what it believes about the others. */
    private fun theWholeClusterIsKnown(redis: Jedis) {
        val fields = clusterSection(redis)
        assertEquals("node-1", fields["cluster_my_id"])
        assertEquals("3", fields["cluster_known_nodes"])
        assertEquals("0", fields["cluster_hints_pending"])
        ids.forEach { assertEquals("alive,0", fields["member_$it"], "$it was not alive in $fields") }
    }

    /** Spec 9's first commands, but written through one node and read back through another. */
    private fun writtenThroughOneReadThroughThree(one: Jedis, three: Jedis) {
        assertEquals("OK", one.set(surviving, "bar", SetParams.setParams().ex(60)))
        assertEquals("bar", three.get(surviving))
        val remaining = three.ttl(surviving)
        assertTrue(remaining in 1L..60L, "the TTL did not cross the quorum: $remaining")
    }

    /**
     * The forward (spec 5.1 steps 2 and 3): a key node-1 does not coordinate, written through
     * node-1 anyway. The contact spells the command back onto the wire and the coordinator's own
     * parser reads it, so the client sees a plain `+OK` and never learns a hop happened.
     */
    private fun aKeyThisContactDoesNotCoordinate(one: Jedis, three: Jedis) {
        val forwarded = keyCoordinatedBy(ids[1])
        assertNotEquals(ids[0], coordinatorOf(forwarded), "the point of the key is that node-1 forwards it")
        assertEquals("OK", one.set(forwarded, "v"))
        assertEquals("v", three.get(forwarded))
        assertEquals(1L, three.del(forwarded))
    }

    /**
     * What a `MULTI` through the cluster does today (T22 deviation 5, recorded not fixed): a
     * batch runs on the coordinator of its keys and nowhere else. Keys sharing a hash tag share
     * a coordinator, so the batch commits through that node; through any other node `EXEC` is an
     * error naming the coordinator, because a batch is a caller's block and cannot be forwarded.
     */
    private fun oneBatchNeedsOneCoordinator(one: Jedis) {
        val tag = "{acct}"
        val coordinator = nodes.single { it.self == coordinatorOf(tag) }
        RespClient(coordinator.respPort).use { onCoordinator ->
            assertEquals(Reply.Simple("OK"), onCoordinator.call("MULTI"))
            assertEquals(Reply.Simple("QUEUED"), onCoordinator.call("SET", "$tag.balance", "100"))
            assertEquals(Reply.Simple("QUEUED"), onCoordinator.call("SET", "$tag.owner", "alice"))
            assertEquals(Reply.Array(listOf(Reply.Simple("OK"), Reply.Simple("OK"))), onCoordinator.call("EXEC"))
        }
        assertEquals("100", one.get("$tag.balance"))

        val elsewhere = nodes.first { it.self != coordinator.self }
        RespClient(elsewhere.respPort).use { offCoordinator ->
            assertEquals(Reply.Simple("OK"), offCoordinator.call("MULTI"))
            assertEquals(Reply.Simple("QUEUED"), offCoordinator.call("SET", "$tag.balance", "200"))
            val refused = offCoordinator.call("EXEC")
            assertTrue(
                refused is Reply.Error && refused.message.contains("coordinated by"),
                "a batch off its coordinator answered $refused",
            )
        }
        assertEquals("100", one.get("$tag.balance"))
    }

    /**
     * Real gossip over real sockets: node-1 hears nothing from node-2 and buries it. The one wait
     * this tier keeps (plan rule 1.7): SWIM counts gossip periods on its own coroutine and reads
     * no clock, so there is no clock a test could advance to bring the burial forward. The wait is
     * for that coroutine, bounded, and it polls rather than sleeps.
     */
    private fun gossipSeesTheDeadNode(one: Jedis) {
        val giveUpAt = System.nanoTime() + Duration.ofSeconds(30).toNanos()
        var fields = clusterSection(one)
        while (fields["member_${ids[1]}"]?.startsWith("dead") != true) {
            assertTrue(System.nanoTime() < giveUpAt, "node-2 was still ${fields["member_${ids[1]}"]} after 30s")
            fields = clusterSection(one)
        }
        assertEquals("alive,0", fields["member_${ids[2]}"], "gossip buried a node that is up")
    }

    /** Minority failure, spec 9's "kill node 2": two of three nodes still make a quorum. */
    private fun readsAndWritesStillSucceed(one: Jedis, three: Jedis) {
        assertEquals("bar", three.get(surviving), "a value written before the failure was lost")
        assertEquals("OK", one.set(surviving, "after"))
        assertEquals("after", three.get(surviving))

        val onThree = keyCoordinatedBy(ids[2])
        assertEquals("OK", one.set(onThree, "forwarded past the dead node"))
        assertEquals("forwarded past the dead node", three.get(onThree))
    }

    /**
     * The other half of the minority failure, and the proof that a contact really forwards: a key
     * node-2 coordinates has no coordinator left, so node-1 forwards into the dark and answers the
     * forward timeout. With N=3 on three nodes every node holds every key, so this is the only
     * thing a client can see that local execution could not have produced.
     *
     * Recorded, not fixed. Spec 5.1 step 7 hands a dead coordinator's keys to the next node
     * clockwise; T25's sloppy quorum needs a fourth node to have one, and the router picks the
     * preference list's first node whether or not gossip has buried it (T22 deviation 7).
     */
    private fun whatTheDeadNodeCoordinated(one: Jedis) {
        val orphaned = keyCoordinatedBy(ids[1])
        RespClient(nodes[0].respPort).use { client ->
            val refused = client.call("SET", orphaned, "v")
            assertTrue(
                refused is Reply.Error && refused.message.contains("forward timeout"),
                "a key the dead node coordinated answered $refused",
            )
        }
        assertEquals("after", one.get(surviving), "the node stayed usable after the forward timed out")
    }

    // ---- the ring, so the test can name a key by the node that coordinates it -----------------

    private fun coordinatorOf(key: String): NodeId =
        ring.preferenceList(Key(key.toByteArray(Charsets.ISO_8859_1)), 3).first()

    private fun keyCoordinatedBy(node: NodeId): String =
        generateSequence(0) { it + 1 }.map { "routed-$it" }.first { coordinatorOf(it) == node }

    /** `INFO`'s cluster section as a map, read the way any client reads `field:value` lines. */
    private fun clusterSection(redis: Jedis): Map<String, String> =
        redis.info()
            .lineSequence()
            .dropWhile { !it.startsWith("# Cluster") }
            .filter { it.contains(':') && !it.startsWith("#") }
            .associate { it.substringBefore(':').trim() to it.substringAfter(':').trim() }

}

/** One command out, one reply in: the test kit's client, used where Jedis hides the reply shape. */
private fun RespClient.call(vararg words: String): Reply {
    send(*words)
    return read()
}
