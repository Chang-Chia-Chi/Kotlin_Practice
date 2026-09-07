package dynacache.engine.persist

import dynacache.engine.ApEngine
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock

/**
 * Where one node keeps its **part** of each distributed snapshot **set** (spec 2.8 step 5): the
 * state it cut, and per **channel** the bytes it recorded on that channel while the set was open.
 * A set is named by its id and a channel by the peer it carries; both are opaque names here.
 *
 * The recorded bytes are opaque too. The caller hands over one message's bytes and gets the same
 * bytes back, in the order it recorded them, so nothing here knows what a marker, an envelope or
 * a protobuf is: the Chandy-Lamport rules stay with the caller and the file stays here.
 *
 * A set is written once and read once. [cut] opens this node's part and puts the state in it,
 * [record] appends to a channel, and [restore] with [replay] is the way back. A part that was
 * never cut restores as an empty state with no channels. [delete] takes the whole set, every
 * node's part of it, which is what an aborted snapshot does at its deadline.
 *
 * The one adapter is [FileSnapshotParts].
 */
interface SnapshotParts {

    /** Opens this node's part of set [id] and writes the engine's state into it. */
    fun cut(id: String)

    /**
     * Appends [bytes] to [channel]'s log in this node's part of [id], as one whole record or
     * none of it. The part must have been [cut] first, which is what the caller's marker rules
     * give it: a channel opens only once the state is down.
     */
    fun record(id: String, channel: String, bytes: ByteArray)

    /** Loads the state this node cut into set [id] back into the engine. */
    fun restore(id: String)

    /** Every whole record on [channel]'s log in this node's part of [id], in the order recorded. */
    fun replay(id: String, channel: String): List<ByteArray>

    /** Deletes set [id] whole, every node's part of it; a set that is not there is already gone. */
    fun delete(id: String)
}

/**
 * The real adapter: a set is a directory under [root], a part is `<root>/<id>/<self>/`, its state
 * is the T32 snapshot's `dump.rdb` in it, and a channel is `from-<peer>.wal` beside that.
 *
 * A channel log is an ordinary write-ahead log, so a record carries the checksum and the length
 * that let [WalReader] tell a whole record from the half-written one a crash mid-append leaves.
 * The tail is the only place either can go wrong: [replay] answers every whole record before a
 * torn tail and refuses a log whose checksum fails, since past that point nothing is trusted.
 * The log's sequence numbers are not used - a channel's order is its file order - so every
 * record is written under the same [CHANNEL_RECORD] op and seq.
 *
 * A recorded envelope is not fsynced: a snapshot records on the node's inbound path, and forcing
 * the disk there would stall it, while the whole set is thrown away if the snapshot does not
 * complete. What a crash costs is the tail of a set that was still open.
 */
class FileSnapshotParts(
    private val root: Path,
    private val self: String,
    private val engine: ApEngine,
    private val clock: Clock,
) : SnapshotParts {

    override fun cut(id: String) {
        val part = part(id)
        Files.createDirectories(part)
        SnapshotEngine(engine, part, clock).save()
    }

    // ponytail: one open-append-close per recorded message; keep the log open per channel if a
    // snapshot under heavy traffic shows it.
    override fun record(id: String, channel: String, bytes: ByteArray) {
        WalWriter(log(id, channel), firstSeq = 0, policy = FsyncPolicy.NEVER, clock = clock)
            // The writer hands a failed write to the append's future and nowhere else, and under
            // `NEVER` nothing later forces it, so the future is where a full disk is seen at all.
            .use { it.append(CHANNEL_RECORD, bytes).durable.join() }
    }

    override fun restore(id: String) {
        rejectPreWalChannelLogs(id)
        SnapshotEngine(engine, part(id), clock).restore()
    }

    override fun replay(id: String, channel: String): List<ByteArray> {
        val log = log(id, channel)
        if (!Files.exists(log)) return emptyList()
        val scan = WalReader(log).readAll()
        if (scan.stop == WalStop.CRC_MISMATCH) throw IOException("$log holds a corrupt record at ${scan.stoppedAt}")
        return scan.entries.map { it.payload }
    }

    override fun delete(id: String) {
        root.resolve(id).toFile().deleteRecursively()
    }

    /**
     * A part written before the channel log became a write-ahead log holds `from-<peer>.log`,
     * length-delimited protobuf with no checksum. Its records cannot be read as WAL records and
     * silently restoring the state without them would drop everything that was in flight (I12),
     * so such a part is refused rather than half-restored.
     */
    private fun rejectPreWalChannelLogs(id: String) {
        val part = part(id)
        if (!Files.isDirectory(part)) return
        val stale = Files.newDirectoryStream(part, "from-*.log").use { it.firstOrNull() } ?: return
        throw IOException("$stale predates the checksummed channel log and cannot be restored")
    }

    private fun part(id: String): Path = root.resolve(id).resolve(self)

    private fun log(id: String, channel: String): Path = part(id).resolve("from-$channel.wal")

    private companion object {
        /** The op every channel record is written under: a channel log holds one kind of record. */
        const val CHANNEL_RECORD: Byte = 0
    }
}
