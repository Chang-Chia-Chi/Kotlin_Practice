package dynacache.engine.persist

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
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
 */

private const val HEADER_BYTES = 4 + 4 + 8 + 1

private const val CRC_BYTES = 4
private const val LENGTH_AT = 4
private const val SEQ_AT = 8
private const val OP_AT = 16

/** One logged mutation: an opaque [payload] under an [op] code, stamped with its [seq]. */
class WalEntry(val seq: Long, val op: Byte, val payload: ByteArray) {

    override fun equals(other: Any?): Boolean =
        this === other ||
            (other is WalEntry && seq == other.seq && op == other.op && payload.contentEquals(other.payload))

    override fun hashCode(): Int = (31 * seq.hashCode() + op) * 31 + payload.contentHashCode()

    override fun toString(): String = "WalEntry(seq=$seq, op=$op, payload=${payload.size} bytes)"
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

/**
 * Appends entries to one log file, handing each a sequence number one higher than the last.
 *
 * This is the `NEVER` fsync policy and only that: writes reach the file, the operating system
 * decides when they reach the disk. The other policies and group commit are ticket 34.
 */
class WalWriter(path: Path, firstSeq: Long) : AutoCloseable {

    private val channel: FileChannel = FileChannel.open(
        path,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.APPEND,
    )

    private var nextSeq: Long = firstSeq

    /** Appends one entry and returns the sequence number it was given. */
    @Synchronized
    fun append(op: Byte, payload: ByteArray): Long {
        val seq = nextSeq++
        val record = ByteBuffer.allocate(HEADER_BYTES + payload.size)
        record.position(CRC_BYTES)
        record.putInt(payload.size).putLong(seq).put(op).put(payload)
        val crc = CRC32()
        crc.update(record.array(), CRC_BYTES, record.position() - CRC_BYTES)
        record.putInt(0, crc.value.toInt())
        record.flip()
        while (record.hasRemaining()) channel.write(record)
        return seq
    }

    override fun close() = channel.close()
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
                entries.add(WalEntry(header.getLong(SEQ_AT), header.get(OP_AT), payload.array()))
                offset += HEADER_BYTES + length
            }
            return WalScan(entries, WalStop.CLEAN_END, offset)
        }
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
