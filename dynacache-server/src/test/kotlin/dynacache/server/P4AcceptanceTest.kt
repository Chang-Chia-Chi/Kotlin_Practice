package dynacache.server

import dynacache.cluster.HostPort
import dynacache.cluster.NodeId
import dynacache.cluster.ReplicationConfig
import dynacache.engine.EvictionPolicy
import dynacache.engine.persist.FsyncPolicy
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
 * Real time, so every wait is a poll to a deadline and there is no sleep anywhere.
 */
class P4AcceptanceTest {

    @TempDir
    lateinit var tmp: Path

    private val ids = List(3) { NodeId("node-${it + 1}") }

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
        val writtenAt = aMixedKeyspaceThroughJedis()
        stopCluster()

        startCluster(dataDirs)
        everyKeyCameBack(writtenAt)
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
     * the quorum carries every type and not just the strings P2 wrote. Answers the instant the
     * TTL'd key was written, so the assertion after the restart can say what is left of it.
     */
    private fun aMixedKeyspaceThroughJedis(): Long {
        Jedis("127.0.0.1", nodes[0].respPort).use { one ->
            assertEquals("OK", one.set("sess:1", "bar", SetParams.setParams().ex(60)))
            assertEquals(2L, one.hset("user:1", mapOf("name" to "alice", "city" to "berlin")))
            assertEquals(3L, one.rpush("queue:1", "a", "b", "c"))
            assertEquals(1L, one.zadd("leaderboard", 100.0, "alice"))
            assertEquals(1L, one.zadd("leaderboard", 200.0, "bob"))
            // Its deadline has to survive the restart, or it never falls due again (spec 5.4).
            assertEquals("OK", one.set("blink", "gone", SetParams.setParams().px(300)))
            return System.nanoTime()
        }
    }

    /** The whole keyspace, through a node that is not the one it was written through. */
    private fun everyKeyCameBack(writtenAt: Long) {
        Jedis("127.0.0.1", nodes[2].respPort).use { three ->
            assertEquals("bar", three.get("sess:1"))
            val spent = Duration.ofNanos(System.nanoTime() - writtenAt).seconds
            assertTrue(three.ttl("sess:1") in 1L..(60L - spent), "the TTL did not survive the restart")
            assertEquals(mapOf("name" to "alice", "city" to "berlin"), three.hgetAll("user:1"))
            assertEquals(listOf("a", "b", "c"), three.lrange("queue:1", 0, -1))
            val ranked = three.zrangeWithScores("leaderboard", 0, -1)
            assertEquals(listOf("alice", "bob"), ranked.map { it.element })
            assertEquals(listOf(100.0, 200.0), ranked.map { it.score })
            // The restart is often quicker than 300 ms, so this is the deadline arriving rather
            // than the key being gone already: a TTL dropped by the RDB would never fall due.
            awaitUntil("the restarted node let a restored TTL fall due") { three.get("blink") == null }
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

    // ---- (c) a TTL fired by the server's own scheduler -----------------------------------------

    /**
     * Spec 9's "TTLs fire on time via the timer wheel", on a cluster node: the server's scheduler
     * is the one thread that advances the wheel, and here it is the real one on a real clock. The
     * socket cannot tell the wheel's deletion from the lazy check a read makes on the way past --
     * both are the client seeing nil -- so what this asserts is the client-visible half; the wheel
     * itself is T09's business at the engine tier.
     */
    private fun aTtlFiresOnAClusterNode() {
        Jedis("127.0.0.1", nodes[0].respPort).use { one ->
            assertEquals("OK", one.set("tick:fires", "v", SetParams.setParams().px(200)))
            assertEquals("v", one.get("tick:fires"))
        }
        Jedis("127.0.0.1", nodes[2].respPort).use { three ->
            awaitUntil("the TTL fired", Duration.ofSeconds(10)) { three.get("tick:fires") == null }
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

    /** Real time, so every wait is a poll to a deadline and there is no sleep anywhere. */
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
