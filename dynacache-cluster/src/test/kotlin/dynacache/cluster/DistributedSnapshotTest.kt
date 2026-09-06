package dynacache.cluster

import dynacache.cluster.proto.Envelope
import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import dynacache.engine.persist.SnapshotEngine
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.io.path.exists
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * Chandy-Lamport distributed snapshots (spec 2.8, C10, I12) at the coordinator's seam,
 * `initiate` and `restoreFrom`, through the test kit's cluster. Every value is a causality
 * tag: write `i` completes (its quorum formed) before write `i + 1` is issued, so `i`
 * happened-before `i + 1`, and a consistent cut holding tag `i + 1` anywhere holds tag `i`.
 */
class DistributedSnapshotTest {

    @TempDir
    lateinit var dir: Path

    private val clock: Clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC)
    private val ok = Reply.Simple("OK")

    private val keys = (1..3).map { Key("k$it") }

    /**
     * Write 1 is everywhere before the snapshot starts. Write 2 is applied on its coordinator
     * and its Replicates are in flight, held two rounds, when a node that is not that
     * coordinator initiates snapshot [id]: the markers overtake the Replicates on the other
     * channels, so write 2 reaches its replicas after their states were recorded and only
     * the channel logs can hold it. Answers write 2's coordinator once every node is complete.
     */
    private suspend fun InProcessCluster.snapshotWithWriteInFlight(id: String): NodeId {
        assertEquals(ok, writeVia(nodes[0], keys[0], "1".toByteArray()))
        drainMessages()
        network.delay(rounds = 2..2, seed = 1)
        val coordinator = ring.preferenceList(keys[1], 3).first()
        val second = router(coordinator).submit(Command.Set(keys[1], "2".toByteArray()))
        untilInFlight()
        network.delay(rounds = 0..0, seed = 1)
        snapshot((nodes - coordinator).first()).initiate(id)
        drainMessages()
        assertEquals(ok, second.await())
        nodes.forEach { assertTrue(snapshot(it).complete(id), "$it complete") }
        return coordinator
    }

    @Test
    fun chandy_lamport_consistent_cut() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope, snapshotDir = dir)
        val coordinator = cluster.snapshotWithWriteInFlight("s1")
        // Write 3 is issued after the snapshot completed.
        assertEquals(ok, cluster.writeVia(cluster.nodes[2], keys[2], "3".toByteArray()))

        val parts = recorded(dir.resolve("s1"))
        val tags = parts.values.flatMap { it.state + it.channels.values.flatten() }.toSet()
        assertFalse(3 in tags, "a write after the snapshot is not in it: $parts")
        for (tag in tags) assertTrue(tags.containsAll((1 until tag).toList()), "tag $tag without its predecessors: $parts")
        // Write 2 was sent by its coordinator inside the cut (it is in the coordinator's state),
        // so every receiver holds it inside the cut too: applied in its state or in flight on
        // its channel from the coordinator. At least one receiver has it only in flight, or
        // the scenario recorded nothing.
        assertTrue(2 in parts.getValue(coordinator).state, "$parts")
        for (node in cluster.nodes - coordinator) {
            val part = parts.getValue(node)
            assertTrue(2 in part.state || 2 in part.channels[coordinator].orEmpty(), "$node lost write 2: $parts")
        }
        assertTrue(cluster.nodes.any { 2 !in parts.getValue(it).state }, "nothing was in flight: $parts")
        cluster.close()
    }

    /** One node's part of a snapshot set as causality tags: its state and each recorded channel. */
    private data class Part(val state: Set<Int>, val channels: Map<NodeId, Set<Int>>)

    @Test
    fun chandy_lamport_restorable() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope, snapshotDir = dir)
        cluster.snapshotWithWriteInFlight("s1")
        assertEquals(ok, cluster.writeVia(cluster.nodes[2], keys[0], "9".toByteArray()))
        assertEquals(ok, cluster.writeVia(cluster.nodes[2], keys[2], "3".toByteArray()))
        cluster.close()

        val restored = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope, snapshotDir = dir)
        restored.nodes.forEach { restored.snapshot(it).restoreFrom(dir, "s1") }
        restored.drainMessages()
        for (node in restored.nodes) {
            assertEquals(Reply.Bulk("1".toByteArray()), restored.readVia(node, keys[0]), "k1 via $node")
            assertEquals(Reply.Bulk("2".toByteArray()), restored.readVia(node, keys[1]), "k2 (in flight at the cut) via $node")
            assertEquals(Reply.Bulk(null), restored.readVia(node, keys[2]), "k3 (after the cut) via $node")
        }
        // The replay is what put write 2 on the replicas whose state was recorded before it arrived.
        assertEquals(List(3) { Reply.Bulk("2".toByteArray()) }, restored.readAllReplicas(keys[1]).values.toList())
        restored.close()
    }

    /**
     * A node is dead when the snapshot starts, so its channels never close. The write path
     * stays open while the survivors wait; at the deadline every survivor deletes the
     * snapshot set and the engines hold exactly what they held.
     */
    @Test
    fun chandy_lamport_timeout_aborts() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope, snapshotDir = dir)
        val coordinators = keys.take(2).map { cluster.ring.preferenceList(it, 1).single() }
        val victim = (cluster.nodes - coordinators.toSet()).first()
        val survivors = cluster.nodes - victim
        assertEquals(ok, cluster.writeVia(survivors[0], keys[0], "1".toByteArray()))
        cluster.network.kill(victim)

        cluster.snapshot(survivors[0]).initiate("s1")
        cluster.drainMessages()
        assertTrue(dir.resolve("s1").exists(), "the survivors recorded their parts")
        assertEquals(ok, cluster.writeVia(survivors[1], keys[1], "2".toByteArray()), "the write path is open during a snapshot")
        survivors.forEach { assertFalse(cluster.snapshot(it).complete("s1"), "$it waits for $victim") }

        advanceTimeBy(31.seconds)
        runCurrent()
        assertFalse(dir.resolve("s1").exists(), "an aborted snapshot leaves no files")
        survivors.forEach { assertFalse(cluster.snapshot(it).complete("s1"), "$it aborted") }
        for (node in survivors) {
            assertEquals(Reply.Bulk("1".toByteArray()), cluster.readVia(node, keys[0]), "k1 via $node")
            assertEquals(Reply.Bulk("2".toByteArray()), cluster.readVia(node, keys[1]), "k2 via $node")
        }
        cluster.close()
    }

    @Test
    fun C10_marker_on_every_channel() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope, snapshotDir = dir)
        assertEquals(ok, cluster.writeVia(cluster.nodes[0], Key("k1"), "1".toByteArray()))

        cluster.snapshot(cluster.nodes[1]).initiate("s1")
        cluster.drainMessages()

        val markers = cluster.network.sent.filter { it.hasMarker() }.groupingBy { it.from to it.to }.eachCount()
        val channels = cluster.nodes.flatMap { from -> (cluster.nodes - from).map { from.name to it.name } }
        assertEquals(channels.associateWith { 1 }, markers)
        cluster.nodes.forEach { assertTrue(cluster.snapshot(it).complete("s1"), "$it complete") }
        cluster.close()
    }

    /** Take snapshot, keep writing (overwrite, delete, create), restore: every read is snapshot-time. */
    @Test
    fun I12_reads_after_restore_return_snapshot_time_values() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope, snapshotDir = dir)
        assertEquals(ok, cluster.writeVia(cluster.nodes[0], keys[0], "1".toByteArray()))
        assertEquals(ok, cluster.writeVia(cluster.nodes[1], keys[1], "2".toByteArray()))
        cluster.snapshot(cluster.nodes[2]).initiate("s1")
        cluster.drainMessages()
        assertEquals(ok, cluster.writeVia(cluster.nodes[0], keys[0], "9".toByteArray()))
        assertEquals(Reply.Integer(1), cluster.settle(cluster.router(cluster.nodes[1]).submit(Command.Del(keys[1]))))
        assertEquals(ok, cluster.writeVia(cluster.nodes[2], keys[2], "3".toByteArray()))
        cluster.close()

        val restored = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope, snapshotDir = dir)
        restored.nodes.forEach { restored.snapshot(it).restoreFrom(dir, "s1") }
        restored.drainMessages()
        for (node in restored.nodes) {
            assertEquals(Reply.Bulk("1".toByteArray()), restored.readVia(node, keys[0]), "k1 (overwritten after) via $node")
            assertEquals(Reply.Bulk("2".toByteArray()), restored.readVia(node, keys[1]), "k2 (deleted after) via $node")
            assertEquals(Reply.Bulk(null), restored.readVia(node, keys[2]), "k3 (created after) via $node")
        }
        restored.close()
    }

    /** Runs the cluster's coroutines and engines until something is in flight on the network. */
    private suspend fun InProcessCluster.untilInFlight() {
        repeat(100) {
            if (network.inFlight) return
            yield()
            nodes.forEach { engine(it).submit(Command.DbSize).get() }
            yield()
        }
        error("nothing was sent")
    }

    /** Every node's part of the snapshot set under [snapshot], read back from its files. */
    private fun recorded(snapshot: Path): Map<NodeId, Part> = snapshot.listDirectoryEntries().associate { node ->
        val scratch = ApEngine(1, clock)
        SnapshotEngine(scratch, node, clock).restore()
        val keys = scratch.submit(Command.Keys("*".toByteArray())).get() as Reply.Array
        val state = keys.items.map { key ->
            val value = scratch.submit(Command.Get(Key((key as Reply.Bulk).bytes!!))).get() as Reply.Bulk
            value.bytes!!.decodeToString().toInt()
        }
        scratch.close()
        val channels = node.listDirectoryEntries("from-*.log").associate { log ->
            val tags = Files.newInputStream(log).use { input ->
                generateSequence { Envelope.parseDelimitedFrom(input) }
                    .filter { it.hasReplicate() }
                    .map { it.replicate.getToken(2).toStringUtf8().toInt() }
                    .toSet()
            }
            NodeId(log.name.removeSurrounding("from-", ".log")) to tags
        }
        NodeId(node.name) to Part(state.toSet(), channels)
    }
}
