package dynacache.cluster

import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Marker
import dynacache.engine.persist.SnapshotParts
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * One node's part of a Chandy-Lamport distributed snapshot (spec 2.8 steps 1 to 5). A
 * **channel** is one peer's envelopes to this node; there is one per peer, in order (the
 * `Transport` seam's promise). Where this node's state and its channels are kept is [parts]'
 * business: this class holds the marker rules and the channel bookkeeping, and hands the
 * adapter one channel's name and one envelope's bytes at a time.
 *
 * [initiate] is step 1, [receive] steps 2 and 3 fed by the node's inbound loop, [complete] step 4
 * for this node; the whole snapshot is complete when every node's part is. A part starts by
 * cutting the state and only then opens its channels, and the demux waits out the cut, so an
 * envelope is in the state or on a channel, never both (C10, I12). Recording happens beside
 * normal handling, so a snapshot never blocks the write path; the engine is only read.
 * [restoreFrom] is the way back: the state into the engine, then every recorded envelope
 * through [demux] as if it had just arrived, so a replicated write in flight at the cut lands.
 *
 * A node's part still open at [deadline] after it started is aborted: the whole set goes, the
 * engine is untouched, and a later marker for that id is ignored.
 *
 * @param demux the node's inbound loop (`InboundLoop.deliver`), for the replay.
 * @param scope the node's lifecycle scope; the deadline timer lives on it.
 * @param deadline how long this node waits for its channels to close (spec 2.8, default 30s).
 */
class DistributedSnapshot(
    private val self: NodeId,
    private val peers: Collection<NodeId>,
    private val transport: Outbound,
    private val parts: SnapshotParts,
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

    /**
     * Held while this node's state is being cut, so the demux hands nothing on in between (spec
     * 2.8: the state first, then recording). Only the initiator ever contends for it; a
     * receiver cuts on the demux's own coroutine.
     */
    private val cutting = Mutex()

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
            cutting.withLock {
                for ((id, channels) in open) {
                    if (from in channels) parts.record(id, from.name, envelope.toByteArray())
                }
            }
            return false
        }
        val id = envelope.marker.snapshotId
        if (id in aborted) return true
        if (!open.containsKey(id)) start(id)
        open[id]?.remove(from)
        return true
    }

    /**
     * Loads this node's state from snapshot [id] (I12) and re-delivers its recorded channels,
     * each in arrival order. Channels are replayed one after another: the cut ordered nothing
     * across them, so no order between them is owed.
     */
    suspend fun restoreFrom(id: String) {
        parts.restore(id)
        for (peer in peers) {
            for (record in parts.replay(id, peer.name)) demux(Envelope.parseFrom(record))
        }
    }

    private suspend fun start(id: String) {
        require(!open.containsKey(id)) { "snapshot $id already started on $self" }
        // The state before the channels (spec 2.8 step 1, then step 2). `open` is what tells the
        // demux -- another coroutine on a real node, where `initiate` runs on the node's scope --
        // that it may start appending, and the lock holds it off while the state is being cut:
        // what it applied before the cut is in the state, what it records is applied after the
        // cut, and nothing is in both.
        cutting.withLock {
            parts.cut(id)
            open[id] = peers.toMutableSet()
        }
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
        parts.delete(id)
    }
}
