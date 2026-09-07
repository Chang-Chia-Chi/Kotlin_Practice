package dynacache.cluster

import com.google.protobuf.ByteString
import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Marker
import dynacache.cluster.proto.Replicate
import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import dynacache.engine.persist.CommandCodec
import dynacache.engine.persist.FileSnapshotParts
import dynacache.engine.persist.FsyncPolicy
import dynacache.engine.persist.SnapshotEngine
import dynacache.engine.persist.SnapshotParts
import java.io.IOException
import java.nio.file.Path
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.io.path.createDirectories
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

        val parts = recorded("s1")
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
        restored.nodes.forEach { restored.snapshot(it).restoreFrom("s1") }
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
        restored.nodes.forEach { restored.snapshot(it).restoreFrom("s1") }
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
        restored.snapshot.restoreFrom("s1")
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
        val logs = dir.resolve("s1").resolve(self.name).listDirectoryEntries("from-*.wal")
        assertEquals(emptyList<Path>(), logs, "no channel is open while the state is being cut")
        gate.release.countDown()
        delivery.join()
        initiate.join()
        node.close()
        val part = recorded("s1").getValue(self)
        assertEquals(Part(emptySet(), mapOf(peer to setOf(1))), part, "delivered during the cut: on the channel, not in the state")
    }

    /**
     * A node with a data directory and a snapshot directory, wired as `clusterMain` wires every
     * persisting node. Its peer never answers the marker, so the set is aborted at the deadline
     * and deleted whole; the writes acked between the cut and the abort are on the node's own
     * log, which lives under the data directory and never inside the part (spec 2.8 recovery).
     */
    @Test
    fun C14_writes_after_the_cut_survive_an_aborted_snapshot_set() = runTest {
        val data = dir.resolve("data")
        val node = Lone(clock, backgroundScope, dataDir = data, deadline = 30.seconds)
        assertEquals(ok, node.engine.submit(Command.Set(counted, "1".toByteArray())).await())
        node.snapshot.initiate("s1")
        assertEquals(ok, node.engine.submit(Command.Set(counted, "2".toByteArray())).await())
        assertEquals(ok, node.engine.submit(Command.Set(Key("after"), "3".toByteArray())).await())

        advanceTimeBy(31.seconds)
        runCurrent()
        assertFalse(dir.resolve("s1").exists(), "the set was aborted at the deadline")
        node.close()

        val restarted = Lone(clock, backgroundScope, dataDir = data)
        assertEquals(Reply.Bulk("2".toByteArray()), restarted.engine.submit(Command.Get(counted)).await(), "acked after the cut")
        assertEquals(Reply.Bulk("3".toByteArray()), restarted.engine.submit(Command.Get(Key("after"))).await(), "acked after the cut")
        restarted.close()
    }

    /**
     * A marker whose snapshot id the part adapter refuses, arriving on a real channel. The
     * inbound loop has no per-envelope catch, so the marker is consumed and dropped rather than
     * thrown on: the id never becomes a path, no part is started or recorded for it, and the
     * node goes on to take the snapshot the next marker asks for and to serve its clients.
     */
    @Test
    fun marker_with_an_unusable_id_is_dropped_and_the_node_lives() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope, snapshotDir = dir)
        assertEquals(ok, cluster.writeVia(cluster.nodes[0], keys[0], "1".toByteArray()))
        val target = cluster.nodes[0]
        val sender = cluster.nodes[1]
        val crafted = Envelope.newBuilder().setFrom(sender.name).setTo(target.name)
            .setMarker(Marker.newBuilder().setSnapshotId("../x")).build()

        cluster.transport(sender).send(target, crafted)
        cluster.drainMessages()

        assertFalse(dir.parent.resolve("x").exists(), "the id never became a path outside the snapshot directory")
        assertEquals(emptyList<Path>(), dir.listDirectoryEntries(), "the refused id recorded nothing")
        assertFalse(cluster.snapshot(target).complete("../x"), "no part was started for the refused id")

        cluster.snapshot(sender).initiate("s1")
        cluster.drainMessages()
        cluster.nodes.forEach { assertTrue(cluster.snapshot(it).complete("s1"), "$it complete") }
        assertEquals(Reply.Bulk("1".toByteArray()), cluster.readVia(target, keys[0]), "the node still answers its clients")
        cluster.close()
    }

    /**
     * A restore names a set this node has no part of, which is what an operator's typo looks
     * like. Before this ticket the missing state file restored as an empty state, so the typo
     * emptied a live node without a word; now the ask fails and every key reads as before (I12).
     */
    @Test
    fun restore_of_a_missing_id_is_an_error_and_changes_nothing() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope, snapshotDir = dir)
        assertEquals(ok, cluster.writeVia(cluster.nodes[0], keys[0], "1".toByteArray()))
        cluster.snapshot(cluster.nodes[0]).initiate("s1")
        cluster.drainMessages()

        for (node in cluster.nodes) {
            val failure = failureOf { cluster.snapshot(node).restoreFrom("s2") }
            assertTrue(failure is IllegalArgumentException, "restoring a set $node has no part of: $failure")
        }

        assertEquals(Reply.Bulk("1".toByteArray()), cluster.readVia(cluster.nodes[0], keys[0]), "the refused restore cleared nothing")
        assertEquals(List(3) { Reply.Bulk("1".toByteArray()) }, cluster.readAllReplicas(keys[0]).values.toList())
        cluster.close()
    }

    /**
     * The part is on disk but its channels are still recording: the state is cut and the
     * envelopes that were in flight at the cut are still arriving, so restoring it now would
     * drop them (I12). A part is restorable once it is complete, and this one is not.
     */
    @Test
    fun restore_of_an_incomplete_part_is_an_error() = runTest {
        val node = Lone(clock, backgroundScope)
        assertEquals(ok, node.engine.submit(Command.Set(counted, "1".toByteArray())).await())
        node.snapshot.initiate("s1")
        assertTrue(dir.resolve("s1").resolve(self.name).resolve("dump.rdb").exists(), "the state is cut")
        assertFalse(node.snapshot.complete("s1"), "$peer never closed its channel")

        val failure = failureOf { node.snapshot.restoreFrom("s1") }
        assertTrue(failure is IllegalArgumentException, "restoring a part still recording: $failure")

        assertEquals(one, node.engine.submit(Command.Get(counted)).await(), "the engine is untouched")
        node.close()
    }

    /**
     * One node of a live cluster cannot write its part: its `dump.rdb.tmp` is a directory, so
     * the state save's first write fails with an ordinary `IOException`. That node abandons the
     * set and goes on reading its inbound channel. Before this ticket the exception left the cut,
     * crossed an inbound loop with no per-envelope catch (T68) and ended the loop, so one node's
     * disk problem stopped it answering for the third of the keyspace it coordinates (T75).
     */
    @Test
    fun a_cut_that_cannot_write_abandons_the_set_and_the_node_lives() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope, snapshotDir = dir)
        assertEquals(ok, cluster.writeVia(cluster.nodes[0], keys[0], "1".toByteArray()))
        val victim = cluster.nodes[0]
        val initiator = cluster.nodes[1]
        dir.resolve("s1").resolve(victim.name).resolve("dump.rdb.tmp").createDirectories()

        cluster.snapshot(initiator).initiate("s1")
        cluster.drainMessages()

        assertFalse(cluster.snapshot(victim).complete("s1"), "$victim has no part of the set")
        assertFalse(dir.resolve("s1").resolve(victim.name).exists(), "$victim recorded no part")
        assertEquals(1, cluster.snapshot(victim).abandoned, "the abandoned set is what INFO reports")
        assertEquals(ok, cluster.writeVia(victim, keys[1], "2".toByteArray()), "$victim still takes a write")
        assertEquals(Reply.Bulk("1".toByteArray()), cluster.readVia(victim, keys[0]), "$victim still answers a read")
        cluster.close()
    }

    /**
     * The cut fails part way, with the part's directory already made. What is left is deleted
     * whole, so no later restore can read the abandoned set as complete, and the id is refused
     * afterwards the way an aborted one is: an operator naming it does not empty the node.
     */
    @Test
    fun a_failed_cut_leaves_no_half_written_part() = runTest {
        val node = Lone(clock, backgroundScope)
        assertEquals(ok, node.engine.submit(Command.Set(counted, "1".toByteArray())).await())
        dir.resolve("s1").resolve(self.name).resolve("dump.rdb.tmp").createDirectories()

        node.demux(marker("s1"))

        assertEquals(emptyList<Path>(), dir.listDirectoryEntries(), "the abandoned set left nothing behind")
        val failure = failureOf { node.snapshot.restoreFrom("s1") }
        assertTrue(failure is IllegalArgumentException, "an abandoned set is not restorable: $failure")
        assertEquals(one, node.engine.submit(Command.Get(counted)).await(), "the engine is untouched")
        assertEquals(1, node.snapshot.abandoned)
        node.close()
    }

    /**
     * The channel log is the cut's other file: a set whose recording fails is abandoned too, and
     * the envelope that could not be recorded still gets its ordinary handling. Recording is
     * beside the write path, so a disk that refuses a snapshot never refuses a client.
     */
    @Test
    fun a_channel_that_cannot_be_recorded_abandons_the_set_and_the_envelope_is_handled() = runTest {
        val node = Lone(clock, backgroundScope, wrap = { parts ->
            object : SnapshotParts by parts {
                override fun record(id: String, channel: String, bytes: ByteArray): Unit =
                    throw IOException("no space left on device")
            }
        })
        node.snapshot.initiate("s1")
        assertTrue(dir.resolve("s1").resolve(self.name).exists(), "the state was cut")

        node.demux(incr)

        assertEquals(one, node.engine.submit(Command.Get(counted)).await(), "the envelope was handled")
        assertFalse(dir.resolve("s1").exists(), "the set this node could not record on is gone")
        assertEquals(1, node.snapshot.abandoned)
        node.close()
    }

    /**
     * The boundary this ticket's policy draws, by exception type and not by position: an
     * `IOException` out of the part adapter is the environment and the set is abandoned;
     * anything else is a bug in this build and reaches the inbound loop, which has no catch of
     * its own on purpose (T68). Swallowing both would answer a broken build with a node that
     * quietly records nothing.
     */
    @Test
    fun a_bug_in_the_cut_is_not_swallowed() = runTest {
        val node = Lone(clock, backgroundScope, wrap = { parts ->
            object : SnapshotParts by parts {
                override fun cut(id: String): Unit = throw IllegalStateException("a bug in this build")
            }
        })

        val failure = failureOf { node.snapshot.receive(marker("s1")) }

        assertTrue(failure is IllegalStateException, "a programming error reaches the loop: $failure")
        assertEquals(0, node.snapshot.abandoned, "a bug is not an abandoned set")
        node.close()
    }

    /** What a suspending call threw, or null: `assertThrows` takes no suspending lambda. */
    private suspend fun failureOf(block: suspend () -> Unit): Throwable? = runCatching { block() }.exceptionOrNull()

    private val self = NodeId("node-1")
    private val peer = NodeId("node-2")
    private val counted = Key("n")
    private val one = Reply.Bulk("1".toByteArray())

    /** The peer's marker for set [id], as it arrives on this node's channel from it. */
    private fun marker(id: String): Envelope = Envelope.newBuilder().setFrom(peer.name).setTo(self.name)
        .setMarker(Marker.newBuilder().setSnapshotId(id)).build()

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
    private inner class Lone(
        snapshotClock: Clock,
        scope: CoroutineScope,
        /** The node's own RDB and log (spec 2.8), restored at construction; null persists nothing. */
        dataDir: Path? = null,
        deadline: Duration = Duration.INFINITE,
        /** The part adapter this node is given, wrapped: how a test makes its storage refuse. */
        wrap: (SnapshotParts) -> SnapshotParts = { it },
    ) {
        val engine = ApEngine(1, clock)
        init {
            dataDir?.let { SnapshotEngine(engine, it.createDirectories(), clock, fsync = FsyncPolicy.NEVER).restore() }
        }
        val snapshot = DistributedSnapshot(
            self, listOf(peer), InMemoryTransport().endpoint(self),
            wrap(FileSnapshotParts(dir, self.name, engine, snapshotClock)),
            demux = ::demux, scope = scope, deadline = deadline,
        )

        suspend fun demux(envelope: Envelope) {
            if (snapshot.receive(envelope) || !envelope.hasReplicate()) return
            engine.submit(CommandCodec.unframe(envelope.replicate.command.toByteArray()).single()).await()
        }

        /** The crash of spec 2.8: nothing is saved on the way out, so recovery is the log alone. */
        fun close() {
            engine.wal?.close()
            engine.close()
        }
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

    /**
     * Every node's part of snapshot set [id]: its state and its channels through the persist
     * adapter, the set's own directory listed for the parts. Which channels a part recorded is
     * a fact only the files hold -- a peer whose envelopes were recorded need not have a part
     * of its own -- so the log names are read here and the records through [SnapshotParts].
     */
    private fun recorded(id: String): Map<NodeId, Part> = dir.resolve(id).listDirectoryEntries().associate { partDir ->
        val node = NodeId(partDir.name)
        val scratch = ApEngine(1, clock)
        val parts = FileSnapshotParts(dir, node.name, scratch, clock)
        parts.restore(id)
        val keys = scratch.submit(Command.Keys("*".toByteArray())).get() as Reply.Array
        val state = keys.items.map { key ->
            val value = scratch.submit(Command.Get(Key((key as Reply.Bulk).bytes!!))).get() as Reply.Bulk
            value.bytes!!.decodeToString().toInt()
        }
        scratch.close()
        val channels = partDir.listDirectoryEntries("from-*.wal").associate { log ->
            val peer = NodeId(log.name.removeSurrounding("from-", ".wal"))
            peer to parts.replay(id, peer.name).map { Envelope.parseFrom(it) }
                .filter { it.hasReplicate() }
                .map { tag(it.replicate) }
                .toSet()
        }
        node to Part(state.toSet(), channels)
    }
}
