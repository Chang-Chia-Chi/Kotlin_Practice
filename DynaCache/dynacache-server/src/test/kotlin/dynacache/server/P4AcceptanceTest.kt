package dynacache.server

import dynacache.cluster.HostPort
import dynacache.cluster.NodeId
import dynacache.cluster.ReplicationConfig
import dynacache.engine.EvictionPolicy
import dynacache.engine.persist.FsyncPolicy
import dynacache.engine.testkit.MutableClock
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.milliseconds

/**
 * Spec 9's persistence half, on T24's three-node harness: the cluster is killed outright and
 * comes back warm, takes a Chandy-Lamport snapshot under traffic and is restored from it, fires
 * a TTL through the server's own scheduler, and evicts cold keys under a memory threshold.
 *
 * The seam is the socket, as it is in P1 and P2: every assertion is on a value Jedis handed
 * back. The one thing a client cannot ask for is a snapshot, so the trigger is a method on
 * [ClusterNode] (see [ClusterNode.snapshot]) rather than an admin verb -- an operator's control,
 * not a client's.
 *
 * Every generation of nodes reads one clock the test owns (plan rule 1.5), so a TTL falls due
 * because this test moved time and not because the machine took a while: a restart costs no clock
 * time at all, which is what makes the restored deadline a real assertion. The one wait left is
 * for the snapshot set's own threads, which no clock advance can bring forward.
 */
class P4AcceptanceTest {

    @TempDir
    lateinit var tmp: Path

    private val ids = List(3) { NodeId("node-${it + 1}") }

    /** The one clock every generation of nodes reads, surviving the restarts as the data dirs do. */
    private val clock = MutableClock(Instant.parse("2026-09-06T00:00:00Z"))

    /** One map every generation of nodes reads at send time; a restart overwrites its own row. */
    private val addresses = ConcurrentHashMap<NodeId, HostPort>()

    private var nodes: List<ClusterNode> = emptyList()

    /** One directory per node, surviving the restart: this is what "the same data dir" means. */
    private val dataDirs by lazy { ids.map { tmp.resolve("data-$it") } }

    /** The snapshot set's root, shared by the three nodes as `<root>/<id>/<node>/`. */
    private val snapshotDir by lazy { tmp.resolve("snapshots") }

    @AfterEach
    fun stopWhateverIsUp() = stopCluster()

    @Test
    fun P4_acceptance_success_signal() {
        startCluster(dataDirs)
        aMixedKeyspaceThroughJedis()
        stopCluster()

        startCluster(dataDirs)
        everyKeyCameBack()
        aTtlFiresOnAClusterNode()

        aSnapshotWhileJedisKeepsWriting()
        stopCluster()

        startCluster()
        nodes.forEach { it.restoreSnapshot(SET) }
        readsReturnSnapshotTimeValues()
        stopCluster()

        underMemoryPressureColdKeysGo()
    }

    // ---- (a) kill all three, restart on the same dirs -----------------------------------------

    /**
     * Spec 9's keyspace, one type per key, written through node-1 and read back through node-3 so
     * the quorum carries every type and not just the strings P2 wrote. The clock does not move
     * while this runs, so the assertion after the restart knows exactly what is left of the TTL.
     */
    private fun aMixedKeyspaceThroughJedis() {
        Jedis("127.0.0.1", nodes[0].respPort).use { one ->
            assertEquals("OK", one.set("sess:1", "bar", SetParams.setParams().ex(60)))
            assertEquals(2L, one.hset("user:1", mapOf("name" to "alice", "city" to "berlin")))
            assertEquals(3L, one.rpush("queue:1", "a", "b", "c"))
            assertEquals(1L, one.zadd("leaderboard", 100.0, "alice"))
            assertEquals(1L, one.zadd("leaderboard", 200.0, "bob"))
            // Its deadline has to survive the restart, or it never falls due again (spec 5.4).
            assertEquals("OK", one.set("blink", "gone", SetParams.setParams().px(300)))
        }
    }

    /** The whole keyspace, through a node that is not the one it was written through. */
    private fun everyKeyCameBack() {
        Jedis("127.0.0.1", nodes[2].respPort).use { three ->
            assertEquals("bar", three.get("sess:1"))
            // No clock time has passed since the write, so the whole minute is still there: a TTL
            // the restart rounded, shortened or dropped could not report exactly what was set.
            assertEquals(60L, three.ttl("sess:1"), "the TTL did not survive the restart")
            assertEquals(mapOf("name" to "alice", "city" to "berlin"), three.hgetAll("user:1"))
            assertEquals(listOf("a", "b", "c"), three.lrange("queue:1", 0, -1))
            val ranked = three.zrangeWithScores("leaderboard", 0, -1)
            assertEquals(listOf("alice", "bob"), ranked.map { it.element })
            assertEquals(listOf(100.0, 200.0), ranked.map { it.score })
            // The restart cost no clock time at all, so `blink` is certainly still alive when the
            // node comes back, and what follows is the deadline arriving rather than the key having
            // gone already: a TTL the RDB dropped would leave the key here for ever.
            assertEquals("gone", three.get("blink"), "the restarted node lost a key inside its TTL")
            clock.advance(Duration.ofMillis(301))
            assertNull(three.get("blink"), "the restarted node did not let a restored TTL fall due")
        }
        assertTrue(Files.exists(dataDirs[0].resolve("dump.rdb")), "node-1 wrote no snapshot at shutdown")
    }

    // ---- (b) a consistent cut under traffic, and the cluster restored from it ------------------

    /**
     * Spec 9's "trigger a Chandy-Lamport snapshot while traffic is running". A second client
     * writes as fast as the cluster will take it for the whole of the cut, so the markers really
     * do overtake writes in flight; the snapshot is complete when every node says its channels
     * closed. A restore owes nothing about the traffic keys and everything about the two keys
     * written either side of the cut, so only those two are asserted.
     */
    private fun aSnapshotWhileJedisKeepsWriting() {
        Jedis("127.0.0.1", nodes[0].respPort).use { one ->
            assertEquals("OK", one.set(BEFORE, "at the cut"))
        }
        val traffic = Traffic(nodes[1].respPort).apply { start() }
        nodes[0].snapshot(SET)
        awaitUntil("the snapshot completed on every node") { nodes.all { it.snapshotComplete(SET) } }
        traffic.stopWriting()
        traffic.join()
        assertNull(traffic.failure, "the writer failed during the snapshot: ${traffic.failure}")
        assertTrue(traffic.written > 0, "no traffic was running while the snapshot was taken")

        // After the cut, so no part of the set may hold it: not a state file, not a channel log.
        Jedis("127.0.0.1", nodes[2].respPort).use { three ->
            assertEquals("OK", three.set(AFTER, "later"))
        }
    }

    /** I12 at the socket: the fresh cluster holds the cut, and nothing that happened after it. */
    private fun readsReturnSnapshotTimeValues() {
        for (node in nodes) {
            Jedis("127.0.0.1", node.respPort).use { redis ->
                assertEquals("at the cut", redis.get(BEFORE), "${node.self} lost the value at the cut")
                assertNull(redis.get(AFTER), "${node.self} restored a write made after the snapshot")
            }
        }
    }

    // ---- (c) a TTL that falls due on a cluster node ---------------------------------------------

    /**
     * Spec 9's "TTLs fire on time via the timer wheel", on a cluster node, written through one and
     * read back through another so the deadline crosses the quorum. The socket cannot tell the
     * wheel's deletion from the lazy check a read makes on the way past -- both are the client
     * seeing nil -- so what this asserts is the client-visible half; the wheel itself is T09's
     * business at the engine tier. The deadline arrives when this test moves the clock the nodes
     * read, so nothing here waits for the scheduler to come round.
     */
    private fun aTtlFiresOnAClusterNode() {
        Jedis("127.0.0.1", nodes[0].respPort).use { one ->
            assertEquals("OK", one.set("tick:fires", "v", SetParams.setParams().px(200)))
            assertEquals("v", one.get("tick:fires"))
        }
        clock.advance(Duration.ofMillis(201))
        Jedis("127.0.0.1", nodes[2].respPort).use { three ->
            assertNull(three.get("tick:fires"), "the TTL did not fire")
            assertEquals(-2L, three.ttl("tick:fires"))
        }
    }

    // ---- (d) memory pressure and W-TinyLFU -----------------------------------------------------

    /**
     * Spec 9's last line: "under memory pressure, W-TinyLFU evicts cold keys, keeps hot ones". One
     * node, because eviction is a node's own decision (spec 5.5) and three replicas of every key
     * would only ask the same question three times. The workload is Redis's own worst case: a
     * flood of keys asked for once, with a small set asked for over and over between the batches.
     */
    private fun underMemoryPressureColdKeysGo() {
        val one = ids[0]
        val node = ClusterNode(
            self = one,
            nodes = setOf(one),
            addresses = mapOf(one to HostPort("127.0.0.1", 0)),
            respPort = 0,
            grpcPort = 0,
            config = ReplicationConfig(n = 1, w = 1, r = 1),
            partitionCount = 4,
            clock = clock,
            maxMemoryBytes = LIMIT,
            policy = EvictionPolicy.W_TINYLFU,
        )
        node.start()
        try {
            Jedis("127.0.0.1", node.respPort).use { redis ->
                val payload = "x".repeat(1024)
                val hot = List(12) { "hot:$it" }
                hot.forEach { assertEquals("OK", redis.set(it, payload)) }
                repeat(12) { round ->
                    repeat(100) { assertEquals("OK", redis.set("cold:${round * 100 + it}", payload)) }
                    hot.forEach { redis.get(it) }
                }
                val fields = infoOf(redis)
                assertEquals("w-tinylfu", fields["maxmemory_policy"], "the node's policy never reached its engine")
                val used = fields.getValue("used_memory").toLong()
                assertTrue(used <= LIMIT, "the node held $used bytes against a $LIMIT threshold")
                assertEquals(hot, hot.filter { redis.get(it) != null }, "the cold flood pushed a hot key out")
            }
        } finally {
            node.close()
        }
    }

    /** `INFO` as the `field:value` map any client reads it as. */
    private fun infoOf(redis: Jedis): Map<String, String> =
        redis.info()
            .lineSequence()
            .filter { it.contains(':') && !it.startsWith("#") }
            .associate { it.substringBefore(':').trim() to it.substringAfter(':').trim() }

    // ---- the harness --------------------------------------------------------------------------

    /**
     * A generation of three nodes on fresh ephemeral ports, told each other's before they start.
     * [dirs] is null for a cluster that persists nothing, which is what a restore from a snapshot
     * set needs: the fresh nodes must hold what the snapshot says and nothing else.
     */
    private fun startCluster(dirs: List<Path>? = null) {
        nodes = ids.mapIndexed { index, id ->
            ClusterNode(
                self = id,
                nodes = ids.toSet(),
                addresses = addresses,
                respPort = 0,
                grpcPort = 0,
                config = ReplicationConfig(n = 3, w = 2, r = 2),
                partitionCount = 4,
                clock = clock,
                gossipPeriod = 100.milliseconds,
                dataDir = dirs?.get(index),
                fsync = FsyncPolicy.ALWAYS,
                snapshotDir = snapshotDir,
            )
        }
        nodes.forEach { addresses[it.self] = HostPort("127.0.0.1", it.grpcPort) }
        nodes.forEach { it.start() }
    }

    private fun stopCluster() {
        nodes.forEach { runCatching { it.close() } }
        nodes = emptyList()
    }

    /**
     * The one wait this tier keeps (plan rule 1.7). A snapshot set completes when every node's
     * markers have overtaken the envelopes in flight on the gRPC transport, which is that
     * transport's threads and the nodes' router coroutines doing the work; no clock is consulted
     * anywhere on that path, so there is nothing a test could advance to bring it forward. Bounded,
     * and a poll rather than a sleep.
     */
    private fun awaitUntil(what: String, within: Duration = Duration.ofSeconds(20), done: () -> Boolean) {
        val giveUpAt = System.nanoTime() + within.toNanos()
        while (!done()) assertTrue(System.nanoTime() < giveUpAt, "$what: not within $within")
    }

    private companion object {
        const val SET = "s1"
        const val BEFORE = "cut:before"
        const val AFTER = "cut:after"

        /** Small enough that 1.2 MB of cold keys is real pressure, large enough to hold the hot set. */
        const val LIMIT = 256L * 1024
    }
}

/**
 * The traffic the snapshot is taken under: one more Jedis client on its own thread, writing as
 * fast as the cluster will take it until it is told to stop. A write that fails is kept rather
 * than swallowed, because a snapshot that stalls the write path would show up exactly there.
 */
private class Traffic(private val port: Int) : Thread("p4-traffic") {

    @Volatile
    private var running = true

    @Volatile
    var written = 0
        private set

    @Volatile
    var failure: Throwable? = null
        private set

    override fun run() {
        try {
            Jedis("127.0.0.1", port).use { redis ->
                while (running) {
                    redis.set("traffic:$written", "v")
                    written++
                }
            }
        } catch (failed: Throwable) {
            failure = failed
        }
    }

    fun stopWriting() {
        running = false
    }
}
