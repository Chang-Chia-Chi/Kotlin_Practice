package dynacache.engine.persist

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Clock
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.CRC32

/*
 * The write-ahead log: an append-only file of mutations, replayed on restart (spec 2.8).
 *
 * One entry on disk, big-endian, exactly the spec's layout:
 *
 *     [crc32:u32][length:u32][seq:u64][op:u8][payload:length bytes]
 *
 * `length` is the payload's byte count, so the header is a fixed 17 bytes. The checksum covers
 * everything after itself - length, seq, op and payload - so a corrupted length field is caught
 * by the same check as a corrupted payload.
 *
 * An entry may carry an opaque **trailer** behind the payload the command codec reads: the
 * cluster's version for the key the command wrote ([KeyVersions], T67), which this file writes
 * down and hands back without ever looking inside it. Since the spec's header has no room for a
 * format version, bit 7 of the op code is the version: set, the payload ends with the trailer
 * and then the trailer's length, and the op code the reader answers has the bit cleared again.
 * The op codes are all below 64, so an entry written before T67 -- and every channel-log record
 * of a distributed snapshot, which never carries a version -- has the bit clear and reads back
 * with no trailer, which is exactly what it held.
 *
 *     [crc32:u32][length:u32][seq:u64][op|0x80:u8][payload][trailer][trailer_len:u32]
 */

private const val HEADER_BYTES = 4 + 4 + 8 + 1

/** Bit 7 of the op code: this entry's payload ends with a trailer and the trailer's length. */
private const val TRAILER_FLAG = 0x80

/** The u32 the trailer's own length is written as, at the very end of the payload. */
private const val TRAILER_LENGTH_BYTES = 4

private const val CRC_BYTES = 4
private const val LENGTH_AT = 4
private const val SEQ_AT = 8
private const val OP_AT = 16

/**
 * One logged mutation: an opaque [payload] under an [op] code, stamped with its [seq], and behind
 * it the equally opaque [trailer] the caller asked to carry -- [WalEntry.NO_TRAILER] for an entry
 * that carried none.
 */
class WalEntry(
    val seq: Long,
    val op: Byte,
    val payload: ByteArray,
    val trailer: ByteArray = NO_TRAILER,
) {

    override fun equals(other: Any?): Boolean =
        this === other ||
            (
                other is WalEntry && seq == other.seq && op == other.op &&
                    payload.contentEquals(other.payload) && trailer.contentEquals(other.trailer)
                )

    override fun hashCode(): Int =
        ((31 * seq.hashCode() + op) * 31 + payload.contentHashCode()) * 31 + trailer.contentHashCode()

    override fun toString(): String =
        "WalEntry(seq=$seq, op=$op, payload=${payload.size} bytes, trailer=${trailer.size} bytes)"

    companion object {
        /** What an entry that carries no version behind it holds: no bytes at all. */
        val NO_TRAILER = ByteArray(0)
    }
}

/** Why a read of the log stopped where it did. */
enum class WalStop {
    /** The last entry ended exactly at the end of the file: nothing was lost. */
    CLEAN_END,

    /**
     * The file ends in the middle of an entry, the shape a crash mid-append leaves. A length
     * field too large for the bytes that remain reads the same way, and is reported the same way:
     * there is no complete entry at this offset.
     */
    TORN_TAIL,

    /** An entry's bytes do not match its checksum. Nothing at or past it is trusted. */
    CRC_MISMATCH,
}

/**
 * What one pass over a log file found: every complete entry up to the point where reading
 * stopped, why it stopped, and the byte [stoppedAt] offset it stopped at.
 */
class WalScan(val entries: List<WalEntry>, val stop: WalStop, val stoppedAt: Long)

/** When an appended entry is forced to disk (spec 2.8). */
enum class FsyncPolicy {
    /** Every append is fsynced before its future completes. */
    ALWAYS,

    /** Fsynced by the first [WalWriter.tick] at least one second after the last fsync. */
    EVERY_SECOND,

    /** Never fsynced by the writer; the operating system decides. The future completes on write. */
    NEVER,
}

/** Where the log's bytes go: written, then forced to disk. The filesystem is a true boundary. */
interface WalSink : AutoCloseable {
    /** Writes every remaining byte of [bytes]. */
    fun write(bytes: ByteBuffer)

    fun fsync()
}

/** The real sink: one file opened for append. */
class FileChannelSink(path: Path) : WalSink {

    private val channel: FileChannel = FileChannel.open(
        path,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.APPEND,
    )

    override fun write(bytes: ByteBuffer) {
        while (bytes.hasRemaining()) channel.write(bytes)
    }

    override fun fsync() = channel.force(true)

    override fun close() = channel.close()
}

/**
 * One append: the [seq] it was given at once, and [durable], which completes when the policy
 * says the entry is on disk.
 */
class WalAppend(val seq: Long, val durable: CompletableFuture<Unit>)

/**
 * Appends entries to one log, handing each a sequence number one higher than the last, and
 * makes them durable per [policy].
 *
 * Group commit: an append encodes its entry and enqueues it, then the first appender to find no
 * flusher running becomes the flusher. It drains everything enqueued so far into one write and,
 * under `ALWAYS`, one fsync, completes those appends, and repeats while the queue refills. No
 * thread is owned here, and the engine's rule of no timers holds: `EVERY_SECOND` is forced by
 * the caller's [tick].
 */
class WalWriter(
    private var sink: WalSink,
    firstSeq: Long,
    private val policy: FsyncPolicy,
    private val clock: Clock,
) : AutoCloseable {

    constructor(
        path: Path,
        firstSeq: Long,
        policy: FsyncPolicy = FsyncPolicy.NEVER,
        clock: Clock = Clock.systemUTC(),
    ) : this(FileChannelSink(path), firstSeq, policy, clock)

    private class Pending(val record: ByteBuffer, val durable: CompletableFuture<Unit>)

    private var nextSeq: Long = firstSeq

    /** The seq handed out last; [firstSeq] minus one before the first append. */
    val lastSeq: Long get() = synchronized(this) { nextSeq - 1 }

    /** Held while the sink is forced or swapped, so a [tick] never forces a sink [rotate] just closed. */
    private val sinkLock = Any()

    /** Enqueued under the writer's monitor in seq order, so a FIFO drain is file order. */
    private val pending = ConcurrentLinkedQueue<Pending>()
    private val flushing = AtomicBoolean(false)

    /** Written but not yet forced; only `EVERY_SECOND` fills it. Guarded by itself. */
    private val awaitingFsync = ArrayList<CompletableFuture<Unit>>()

    @Volatile
    private var lastFsync: Instant = clock.instant()

    /**
     * Enqueues one entry, returns its seq at once, and flushes if nobody else is. [trailer] is
     * carried behind [payload] and read back beside it, uninterpreted either way.
     */
    fun append(op: Byte, payload: ByteArray, trailer: ByteArray = WalEntry.NO_TRAILER): WalAppend {
        val durable = CompletableFuture<Unit>()
        val seq = synchronized(this) {
            val seq = nextSeq++
            pending.add(Pending(encode(seq, op, payload, trailer), durable))
            seq
        }
        flushIfIdle()
        return WalAppend(seq, durable)
    }

    /**
     * The caller's clock tick: under `EVERY_SECOND`, forces everything written since the last
     * fsync once a second has passed, and completes those appends. A no-op otherwise.
     */
    fun tick() {
        if (clock.instant() < lastFsync.plusSeconds(1)) return
        val batch = takeAwaiting()
        if (batch.isNotEmpty()) fsyncAndComplete(batch)
    }

    /**
     * The checkpoint cut: everything appended so far is forced into the old sink, which is then
     * closed, and every later entry goes to [next]. The caller guarantees no append is in flight,
     * which the engine has by parking every partition at the cut; the check names that contract.
     */
    fun rotate(next: WalSink) {
        check(pending.isEmpty() && !flushing.get()) { "rotate needs a quiet log" }
        synchronized(sinkLock) {
            forceAwaiting()
            sink.close()
            sink = next
        }
    }

    private fun flushIfIdle() {
        // Re-check after releasing the flag: an entry enqueued between the drain and the release
        // would otherwise sit with nobody flushing it.
        while (pending.peek() != null) {
            if (!flushing.compareAndSet(false, true)) return
            try {
                writeBatch()
            } finally {
                flushing.set(false)
            }
        }
    }

    private fun writeBatch() {
        val batch = generateSequence { pending.poll() }.toList()
        if (batch.isEmpty()) return
        val bytes = ByteBuffer.allocate(batch.sumOf { it.record.remaining() })
        batch.forEach { bytes.put(it.record) }
        val waiters = batch.map { it.durable }
        try {
            sink.write(bytes.flip())
        } catch (e: IOException) {
            waiters.forEach { it.completeExceptionally(e) }
            return
        }
        when (policy) {
            FsyncPolicy.ALWAYS -> fsyncAndComplete(waiters)
            FsyncPolicy.NEVER -> waiters.forEach { it.complete(Unit) }
            FsyncPolicy.EVERY_SECOND -> synchronized(awaitingFsync) { awaitingFsync.addAll(waiters) }
        }
    }

    private fun takeAwaiting(): List<CompletableFuture<Unit>> = synchronized(awaitingFsync) {
        ArrayList(awaitingFsync).also { awaitingFsync.clear() }
    }

    private fun forceAwaiting() {
        val left = takeAwaiting()
        if (left.isNotEmpty()) fsyncAndComplete(left)
    }

    private fun fsyncAndComplete(batch: List<CompletableFuture<Unit>>) {
        try {
            synchronized(sinkLock) { sink.fsync() }
            lastFsync = clock.instant()
            batch.forEach { it.complete(Unit) }
        } catch (e: IOException) {
            batch.forEach { it.completeExceptionally(e) }
        }
    }

    private fun encode(seq: Long, op: Byte, payload: ByteArray, trailer: ByteArray): ByteBuffer {
        val carried = trailer.isNotEmpty()
        val length = payload.size + if (carried) trailer.size + TRAILER_LENGTH_BYTES else 0
        val record = ByteBuffer.allocate(HEADER_BYTES + length)
        record.position(CRC_BYTES)
        record.putInt(length).putLong(seq)
        record.put(if (carried) (op.toInt() or TRAILER_FLAG).toByte() else op).put(payload)
        if (carried) record.put(trailer).putInt(trailer.size)
        val crc = CRC32()
        crc.update(record.array(), CRC_BYTES, record.position() - CRC_BYTES)
        record.putInt(0, crc.value.toInt())
        return record.flip()
    }

    /** Writes what is still queued, forces what is still waiting, then closes the sink. */
    override fun close() {
        flushIfIdle()
        forceAwaiting()
        sink.close()
    }
}

/** Reads one log file back, entry by entry, in the order they were appended. */
class WalReader(private val path: Path) {

    fun readAll(): WalScan {
        val entries = ArrayList<WalEntry>()
        FileChannel.open(path, StandardOpenOption.READ).use { channel ->
            val size = channel.size()
            var offset = 0L
            val header = ByteBuffer.allocate(HEADER_BYTES)
            while (offset < size) {
                if (size - offset < HEADER_BYTES) return WalScan(entries, WalStop.TORN_TAIL, offset)
                header.clear()
                readFully(channel, header, offset)
                val length = header.getInt(LENGTH_AT)
                if (length < 0 || size - offset - HEADER_BYTES < length) {
                    return WalScan(entries, WalStop.TORN_TAIL, offset)
                }
                val payload = ByteBuffer.allocate(length)
                readFully(channel, payload, offset + HEADER_BYTES)
                val crc = CRC32()
                crc.update(header.array(), CRC_BYTES, HEADER_BYTES - CRC_BYTES)
                crc.update(payload.array())
                if (crc.value.toInt() != header.getInt(0)) {
                    return WalScan(entries, WalStop.CRC_MISMATCH, offset)
                }
                val entry = split(header.getLong(SEQ_AT), header.get(OP_AT), payload.array())
                    ?: return WalScan(entries, WalStop.TORN_TAIL, offset)
                entries.add(entry)
                offset += HEADER_BYTES + length
            }
            return WalScan(entries, WalStop.CLEAN_END, offset)
        }
    }

    /**
     * The payload as the writer left it: the command's bytes alone when the op code's trailer bit
     * is clear, and otherwise cut at the length the last four bytes name, with the bit cleared
     * off the op code the caller sees. Null when that length does not fit inside the payload,
     * which is a corrupt entry and reads as a torn tail like any other.
     */
    private fun split(seq: Long, flagged: Byte, bytes: ByteArray): WalEntry? {
        if (flagged.toInt() and TRAILER_FLAG == 0) return WalEntry(seq, flagged, bytes)
        if (bytes.size < TRAILER_LENGTH_BYTES) return null
        val trailerAt = bytes.size - TRAILER_LENGTH_BYTES
        val trailerLength = ByteBuffer.wrap(bytes).getInt(trailerAt)
        if (trailerLength < 0 || trailerLength > trailerAt) return null
        val op = (flagged.toInt() and TRAILER_FLAG.inv()).toByte()
        val from = trailerAt - trailerLength
        return WalEntry(seq, op, bytes.copyOfRange(0, from), bytes.copyOfRange(from, trailerAt))
    }

    private fun readFully(channel: FileChannel, into: ByteBuffer, from: Long) {
        var at = from
        while (into.hasRemaining()) {
            val read = channel.read(into, at)
            if (read < 0) throw IOException("$path ended at $at")
            at += read
        }
    }
}
