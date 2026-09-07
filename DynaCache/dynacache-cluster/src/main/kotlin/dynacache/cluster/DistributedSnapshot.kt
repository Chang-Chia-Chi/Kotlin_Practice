package dynacache.cluster

import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Marker
import dynacache.engine.persist.SnapshotParts
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
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
 * **A part this node cannot write costs the set, never the node** (T80). [parts]' file work fails
 * with an `IOException` when the environment refuses it -- a full disk, a permission the process
 * lost, a directory taken from under it -- and that is caught here, at the only handler that
 * touches a disk on the inbound path, and answered by [abandon]. The policy is here and not in
 * the inbound loop because the loop's want of a per-envelope catch is deliberate (T68): a catch
 * up there would stand over every handler, and only this one knows what to do about a failure --
 * drop this set, keep the node. What is not an `IOException` is a bug in this build and is caught
 * nowhere: it ends the loop loudly, which is what a broken build should do.
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

    private val abandonments = AtomicInteger()

    /** How many sets this node abandoned because its storage refused: `INFO`'s count (plan 2.4). */
    val abandoned: Int get() = abandonments.get()

    /** The last such set and what it failed with, or null; `INFO`'s line beside [abandoned]. */
    @Volatile
    var lastAbandoned: String? = null
        private set

    /**
     * Held while this node's state is being cut, so the demux hands nothing on in between (spec
     * 2.8: the state first, then recording). Only the initiator ever contends for it; a
     * receiver cuts on the demux's own coroutine.
     */
    private val cutting = Mutex()

    /**
     * Step 1: this node records its state and sends a marker on every outgoing channel. [id] is
     * an operator's, not the wire's, so an id [parts] refuses fails here rather than being
     * dropped: what this node initiates always has the shape the adapter accepts.
     *
     * A cut the environment refuses is [abandon]ed like any other: no marker goes out, so no peer
     * opens a set this node could not start, and the operator reads the failure in `INFO`. The
     * caller is the node's own scope (`ClusterNode.snapshot` launches this), so throwing here
     * would cancel that scope and take the inbound loop with it.
     */
    suspend fun initiate(id: String) {
        try {
            start(id)
        } catch (failure: IOException) {
            abandon(id, failure)
        }
    }

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
                    if (from !in channels) continue
                    // A set whose channel cannot be written is abandoned and the envelope goes on
                    // to its ordinary handling: recording is beside the write path, so a disk that
                    // refuses a snapshot never refuses a client. `open` is concurrent, so
                    // abandoning one set from inside this walk leaves the others recording.
                    try {
                        parts.record(id, from.name, envelope.toByteArray())
                    } catch (failure: IOException) {
                        abandon(id, failure)
                    }
                }
            }
            return false
        }
        val id = envelope.marker.snapshotId
        // The id came off the wire and is a name on [parts]' storage, so the adapter is asked
        // before it is used. A refused id is dropped here and recorded nowhere: the inbound loop
        // has no per-envelope catch, so throwing would answer a crafted id by killing the node.
        if (!parts.accepts(id) || id in aborted) return true
        if (!open.containsKey(id)) {
            try {
                start(id)
            } catch (failure: IOException) {
                // The marker is consumed either way: the set is this node's to abandon, and the
                // node reads its next envelope.
                abandon(id, failure)
                return true
            }
        }
        open[id]?.remove(from)
        return true
    }

    /**
     * Loads this node's state from snapshot [id] (I12) and re-delivers its recorded channels,
     * each in arrival order. Channels are replayed one after another: the cut ordered nothing
     * across them, so no order between them is owed.
     *
     * A restore names its precondition: this node's part of the set is here ([SnapshotParts.holds])
     * and complete, no channel of it still recording, and not one this node gave up on. Otherwise
     * it fails and nothing is touched: an unknown id is an operator's typo, and restoring it as
     * the empty state it looks like on disk would empty a live node, while a part still recording
     * would drop what is in flight. A set that was aborted or [abandon]ed is refused by name and
     * not by what survives on disk, so whatever a failed cut left behind is never read as a whole
     * part.
     */
    suspend fun restoreFrom(id: String) {
        require(id !in aborted && parts.holds(id) && open[id].orEmpty().isEmpty()) {
            "$self has no complete part of snapshot $id"
        }
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

    /**
     * The environment refused this node's part of [id]. The set goes the way one that timed out
     * goes -- the part deleted, the id refused afterwards -- and the count and the reason are what
     * `INFO` shows an operator, since nothing else on this node has a voice (plan 2.4).
     */
    private fun abandon(id: String, failure: IOException) {
        abort(id)
        // The reason is the platform's own words about a path, and it becomes one line of an
        // `INFO` section whose lines are joined by CRLF: whatever whitespace it holds collapses.
        lastAbandoned = "$id $failure".replace(WHITESPACE, " ")
        abandonments.incrementAndGet()
    }

    private fun abort(id: String) {
        open.remove(id)
        aborted.add(id)
        parts.delete(id)
    }

    private companion object {
        val WHITESPACE = Regex("\\s+")
    }
}
