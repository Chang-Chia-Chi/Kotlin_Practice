package dynacache.cluster

import com.google.protobuf.ByteString
import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Replicate
import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import dynacache.engine.persist.CommandCodec
import dynacache.engine.persist.SnapshotEngine
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.io.path.exists
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
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

    /**
     * The initiator's cut against its own demux (spec 2.8 step 1 before step 2). On a real node
     * `initiate` runs on the node's scope while the router's inbound loop runs the demux, so here
     * the initiator runs on [Dispatchers.Default] and a [Gate] clock parks it at the state
     * save's first clock reading, before any partition's view is taken. A Replicate handed to
     * the demux there is restored with its effect applied once: it is in the state or on the
     * channel log, never both. With the channels opened before the cut (T36) it was in both and
     * the restored node read 2.
     */
    @Test
    fun I12_write_during_the_cut_is_restored_once() = runTest {
        val gate = Gate()
        val node = Lone(gate, backgroundScope)
        val initiate = launch(Dispatchers.Default) { node.snapshot.initiate("s1") }
        assertTrue(gate.blocked.await(5, SECONDS), "the initiator is inside its state save")
        val delivery = launch { node.demux(incr) }
        runCurrent()
        gate.release.countDown()
        delivery.join()
        initiate.join()
        assertEquals(one, node.engine.submit(Command.Get(counted)).await(), "the live node applied the write once")
        node.close()

        val restored = Lone(clock, backgroundScope)
        restored.snapshot.restoreFrom(dir, "s1")
        assertEquals(one, restored.engine.submit(Command.Get(counted)).await(), "restored once: from the state or the channel log, not both")
        restored.close()
    }

    /**
     * While the state is being cut no channel takes an envelope; the one handed to the demux
     * during the cut is on its channel afterwards and not in the state.
     */
    @Test
    fun C10_state_is_cut_before_any_channel_opens() = runTest {
        val gate = Gate()
        val node = Lone(gate, backgroundScope)
        val initiate = launch(Dispatchers.Default) { node.snapshot.initiate("s1") }
        assertTrue(gate.blocked.await(5, SECONDS), "the initiator is inside its state save")
        val delivery = launch { node.demux(incr) }
        runCurrent()
        val logs = dir.resolve("s1").resolve(self.name).listDirectoryEntries("from-*.log")
        assertEquals(emptyList<Path>(), logs, "no channel is open while the state is being cut")
        gate.release.countDown()
        delivery.join()
        initiate.join()
        node.close()
        val part = recorded(dir.resolve("s1")).getValue(self)
        assertEquals(Part(emptySet(), mapOf(peer to setOf(1))), part, "delivered during the cut: on the channel, not in the state")
    }

    private val self = NodeId("node-1")
    private val peer = NodeId("node-2")
    private val counted = Key("n")
    private val one = Reply.Bulk("1".toByteArray())

    /** What the peer replicates during the cut: not idempotent, so a second application shows. */
    private val incr: Envelope = Envelope.newBuilder().setFrom(peer.name).setTo(self.name)
        .setReplicate(Replicate.newBuilder().setId(1).setCommand(ByteString.copyFrom(CommandCodec.frame(Command.IncrBy(counted, 1)))))
        .build()

    /**
     * The initiator alone, one silent peer on its network, and its demux in the router's shape:
     * the snapshot hook first, then the replicated command on the engine. The peer never sends
     * its marker back, so the part waits without a deadline: under `runTest` a finite one fires
     * as soon as the test idles on the initiator's thread and deletes the set under the test.
     */
    private inner class Lone(snapshotClock: Clock, scope: CoroutineScope) {
        val engine = ApEngine(1, clock)
        val snapshot = DistributedSnapshot(
            self, listOf(peer), engine, InMemoryTransport().endpoint(self), dir, snapshotClock,
            demux = ::demux, scope = scope, deadline = Duration.INFINITE,
        )

        suspend fun demux(envelope: Envelope) {
            if (snapshot.receive(envelope) || !envelope.hasReplicate()) return
            engine.submit(CommandCodec.unframe(envelope.replicate.command.toByteArray()).single()).await()
        }

        fun close() = engine.close()
    }

    /** A clock whose first reading parks its caller until [release]; every reading is the epoch. */
    private class Gate : Clock() {
        val blocked = CountDownLatch(1)
        val release = CountDownLatch(1)
        private val first = AtomicBoolean(true)

        override fun instant(): Instant {
            if (first.compareAndSet(true, false)) {
                blocked.countDown()
                check(release.await(5, SECONDS)) { "the gate was never released" }
            }
            return Instant.EPOCH
        }

        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId): Clock = this
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

    /** The write's tag: the value a cluster `SET` carries, or the delta of the lone harness's [incr]. */
    private fun tag(replicate: Replicate): Int = when (val write = CommandCodec.unframe(replicate.command.toByteArray()).single()) {
        is Command.Set -> write.value.decodeToString().toInt()
        is Command.IncrBy -> write.delta.toInt()
        else -> error("no tag in $write")
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
                    .map { tag(it.replicate) }
                    .toSet()
            }
            NodeId(log.name.removeSurrounding("from-", ".log")) to tags
        }
        NodeId(node.name) to Part(state.toSet(), channels)
    }
}
