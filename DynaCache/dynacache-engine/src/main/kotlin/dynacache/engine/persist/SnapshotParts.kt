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
 * [record] appends to a channel, and [restore] with [replay] is the way back, over a part this
 * adapter [holds]. [delete] takes the whole set, every node's part of it, which is what an
 * aborted snapshot does at its deadline.
 *
 * The one adapter is [FileSnapshotParts].
 */
interface SnapshotParts {

    /**
     * Whether [id] is a name this adapter can carry a set under. A snapshot id arrives on a
     * marker from the wire and becomes a name on this adapter's storage, so the caller asks
     * before it starts a part and drops the marker when the answer is false.
     *
     * The answer is a value and not a throw on purpose: the caller's inbound loop has no
     * per-envelope catch, so a throw here would turn a crafted id into a dead node. Every other
     * method takes an id this answered true for; one it did not is a caller's bug and fails.
     */
    fun accepts(id: String): Boolean

    /**
     * Opens this node's part of set [id] and writes the engine's state into it. The engine's own
     * log is left alone: it keeps running under the node's data directory, so the part holds the
     * state as of the log's seq at the cut and no log file, and [delete] can never take a file
     * recovery needs (C14, spec 2.8).
     */
    fun cut(id: String)

    /**
     * Appends [bytes] to [channel]'s log in this node's part of [id], as one whole record or
     * none of it. The part must have been [cut] first, which is what the caller's marker rules
     * give it: a channel opens only once the state is down.
     */
    fun record(id: String, channel: String, bytes: ByteArray)

    /**
     * Whether this node's part of set [id] is here with its state in it, which is what [restore]
     * needs. False for a set this node never cut and for an id [accepts] refuses, so an
     * operator's typo is an answer here and never a throw or a path.
     *
     * A part being here says nothing about whether its channels have closed: whether a part is
     * complete is the marker rules' answer, held by the caller, and a restore wants both.
     */
    fun holds(id: String): Boolean

    /**
     * Loads the state this node cut into set [id] back into the engine. The part must be one this
     * adapter [holds]; restoring one it does not hold would put an empty state into the engine
     * and so empty the node without a word (I12), and that fails instead.
     */
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

    override fun accepts(id: String): Boolean =
        SAFE_ID.matches(id) && id != "." && id != ".." && id.substringBefore('.').uppercase() !in RESERVED

    /** The part's directory is the snapshot engine's to create, so this only says where it goes. */
    override fun cut(id: String) {
        SnapshotEngine(engine, part(id), clock).save()
    }

    // ponytail: one open-append-close per recorded message; keep the log open per channel if a
    // snapshot under heavy traffic shows it.
    override fun record(id: String, channel: String, bytes: ByteArray) {
        WalWriter(log(id, channel), firstSeq = 0, policy = FsyncPolicy.NEVER, clock = clock)
            // The writer hands a failed write to the append's future and nowhere else, and under
            // `NEVER` nothing later forces it, so the future is where a full disk is seen at all.
            .use { it.append(CHANNEL_RECORD, bytes).durable.join() }
    }

    override fun holds(id: String): Boolean = accepts(id) && Files.isRegularFile(SnapshotEngine.stateFile(part(id)))

    override fun restore(id: String) {
        require(holds(id)) { "$self has no part of snapshot set $id" }
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
        set(id).toFile().deleteRecursively()
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

    /**
     * Set [id]'s directory. Every path this adapter builds goes through here, so an id the
     * shape refuses can never become a directory to write under or a tree to delete, whichever
     * method it came in by ([accepts] is how a caller avoids the failure).
     */
    private fun set(id: String): Path {
        require(accepts(id)) { "snapshot id is not a usable name" }
        return root.resolve(id)
    }

    private fun part(id: String): Path = set(id).resolve(self)

    private fun log(id: String, channel: String): Path = part(id).resolve("from-$channel.wal")

    private companion object {
        /** The op every channel record is written under: a channel log holds one kind of record. */
        const val CHANNEL_RECORD: Byte = 0

        /**
         * The shape of a usable set name: one path segment of at most 64 characters drawn from
         * letters, digits, `.`, `-` and `_`, so no separator, no control character, no drive
         * letter and no empty name can be one. `.` and `..` match it and are refused beside it.
         */
        val SAFE_ID = Regex("[A-Za-z0-9._-]{1,64}")

        /**
         * Names Windows keeps for devices, with or without an extension. They are of the safe
         * shape and no directory can carry one, so a marker holding one would fail the cut and,
         * with no per-envelope catch above, take the node down: they are refused with the rest.
         */
        val RESERVED = setOf("CON", "PRN", "AUX", "NUL") + (0..9).flatMap { listOf("COM$it", "LPT$it") }
    }
}
