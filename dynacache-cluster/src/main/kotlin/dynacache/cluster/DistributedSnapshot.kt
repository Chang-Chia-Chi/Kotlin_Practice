package dynacache.cluster

import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Marker
import dynacache.engine.ApEngine
import dynacache.engine.persist.SnapshotEngine
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE
import java.time.Clock
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

/**
 * One node's part of a Chandy-Lamport distributed snapshot (spec 2.8 steps 1 to 5). A
 * **channel** is one peer's envelopes to this node; there is one per peer, in order (the
 * `Transport` seam's promise). This node's part of snapshot `id` lives under
 * `<dir>/<id>/<self>/`: its state as `dump.rdb` (the T32 snapshot) and, per channel, the
 * envelopes that arrived between this node recording its state and that channel's marker, as
 * `from-<peer>.log` (length-delimited protobuf, appended in arrival order).
 *
 * [initiate] is step 1, [receive] steps 2 and 3 fed by the router's demux, [complete] step 4
 * for this node; the whole snapshot is complete when every node's part is. Recording happens
 * beside normal handling, so a snapshot never blocks the write path; the engine is only read.
 * [restoreFrom] is the way back: the state into the engine, then every recorded envelope
 * through [demux] as if it had just arrived, so a replicated write in flight at the cut lands.
 *
 * A node's part still open at [deadline] after it started is aborted: the whole set under
 * `<dir>/<id>` goes, the engine is untouched, and a later marker for that id is ignored.
 *
 * @param demux the node's inbound handler (`Router.receive`), for the replay.
 * @param scope the node's lifecycle scope; the deadline timer lives on it.
 * @param deadline how long this node waits for its channels to close (spec 2.8, default 30s).
 */
class DistributedSnapshot(
    private val self: NodeId,
    private val peers: Collection<NodeId>,
    private val engine: ApEngine,
    private val transport: Transport,
    private val dir: Path,
    private val clock: Clock,
    private val demux: suspend (Envelope) -> Unit,
    private val scope: CoroutineScope,
    private val deadline: Duration = 30.seconds,
) {

    /**
     * The channels of one snapshot still being recorded; empty once every marker arrived.
     * The demux touches it inline and the deadline timer from its own coroutine.
     */
    private val open = ConcurrentHashMap<String, MutableSet<NodeId>>()
    private val aborted = ConcurrentHashMap.newKeySet<String>()

    /** Step 1: this node records its state and sends a marker on every outgoing channel. */
    suspend fun initiate(id: String) = start(id)

    /** Step 4 for this node: a marker arrived on every incoming channel. */
    fun complete(id: String): Boolean = open[id]?.isEmpty() == true

    /**
     * The demux hook, ahead of every other handler: a marker is consumed here (steps 2 and 3)
     * and the answer is true; anything else is recorded on every open channel it arrived on
     * and handed back for its usual handling.
     */
    suspend fun receive(envelope: Envelope): Boolean {
        val from = NodeId(envelope.from)
        if (!envelope.hasMarker()) {
            for ((id, channels) in open) if (from in channels) record(id, from, envelope)
            return false
        }
        val id = envelope.marker.snapshotId
        if (id in aborted) return true
        if (!open.containsKey(id)) start(id)
        open[id]?.remove(from)
        return true
    }

    /**
     * Loads this node's state from snapshot [id] under [from] (I12) and re-delivers its
     * recorded channels, each in arrival order. Channels are replayed one after another: the
     * cut ordered nothing across them, so no order between them is owed.
     */
    suspend fun restoreFrom(from: Path, id: String) {
        val mine = part(from, id)
        SnapshotEngine(engine, mine, clock).restore()
        for (peer in peers) {
            val log = mine.resolve("from-${peer.name}.log")
            if (!Files.exists(log)) continue
            val envelopes = Files.newInputStream(log).use { input ->
                generateSequence { Envelope.parseDelimitedFrom(input) }.toList()
            }
            for (envelope in envelopes) demux(envelope)
        }
    }

    private suspend fun start(id: String) {
        require(!open.containsKey(id)) { "snapshot $id already started on $self" }
        open[id] = peers.toMutableSet()
        val mine = part(dir, id)
        Files.createDirectories(mine)
        SnapshotEngine(engine, mine, clock).save()
        val marker = Envelope.newBuilder().setFrom(self.name).setMarker(Marker.newBuilder().setSnapshotId(id))
        for (peer in peers) transport.send(peer, marker.setTo(peer.name).build())
        scope.launch {
            delay(deadline)
            if (!complete(id)) abort(id)
        }
    }

    private fun abort(id: String) {
        open.remove(id)
        aborted.add(id)
        dir.resolve(id).toFile().deleteRecursively()
    }

    // ponytail: one open-append-close per recorded envelope; keep the log open per channel if a
    // snapshot under heavy traffic shows it.
    private fun record(id: String, from: NodeId, envelope: Envelope) {
        Files.newOutputStream(part(dir, id).resolve("from-${from.name}.log"), CREATE, APPEND).use(envelope::writeDelimitedTo)
    }

    private fun part(root: Path, id: String): Path = root.resolve(id).resolve(self.name)
}
