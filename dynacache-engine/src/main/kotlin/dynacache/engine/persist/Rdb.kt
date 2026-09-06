package dynacache.engine.persist

import dynacache.engine.Key
import dynacache.engine.Value
import dynacache.engine.ds.SkipList
import dynacache.engine.fieldBytes
import dynacache.engine.fieldName
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.time.Instant
import java.util.Random
import java.util.zip.CRC32
import java.util.zip.CheckedInputStream
import java.util.zip.CheckedOutputStream

/*
 * The RDB snapshot codec: one file holding a point-in-time copy of a partition's keys (spec 2.8).
 *
 *     [magic:7][version:u8][count:u32] [entry]* [crc32:u32]
 *
 * and one entry, big-endian throughout as the WAL is:
 *
 *     [key_len:u32][key][type:u8][dvv_len:u32][dvv][ttl_abs:i64][value_len:u32][value]
 *
 * `ttl_abs` is epoch millis, -1 when the key never expires; TTLs travel as absolute instants so
 * a replica's clock skew cannot move a deadline (spec 5.4). The DVV is opaque here: version
 * vectors live in the cluster module and the engine cannot depend on it, so the codec carries
 * the bytes it is handed and hands them back.
 *
 * The checksum covers everything before it, so a corrupted length field is caught by the same
 * check as a corrupted payload.
 */

/** The seven ASCII bytes that open the file and say it is one of ours. */
internal const val RDB_MAGIC = "DYNARDB"

/** The one format this codec writes and the only one it reads. */
internal const val RDB_VERSION: Byte = 1

/** Why a file was refused. */
internal enum class RdbFault(val detail: String) {
    NOT_AN_RDB("not an RDB snapshot"),
    UNSUPPORTED_VERSION("RDB version is not $RDB_VERSION"),
    TRUNCATED("RDB snapshot ends mid-entry or does not parse as one"),
    CHECKSUM_MISMATCH("RDB checksum does not match the bytes"),
}

/** A file that is not a snapshot this codec can trust. Nothing partial is returned with it. */
internal class RdbFormatException(val fault: RdbFault) : IOException(fault.detail)

/** What a `ttl_abs` of -1 means: this key has no deadline. */
private const val NO_TTL = -1L

/**
 * The type byte, fixed here by the format rather than taken from the enum's order: reordering
 * [Value.Kind] must never change what an already-written file means.
 */
private val KIND_BY_CODE = mapOf<Byte, Value.Kind>(
    0.toByte() to Value.Kind.STRING,
    1.toByte() to Value.Kind.HASH,
    2.toByte() to Value.Kind.LIST,
    3.toByte() to Value.Kind.ZSET,
)

private val CODE_BY_KIND = KIND_BY_CODE.entries.associate { (code, kind) -> kind to code }

/**
 * One key as a snapshot holds it: what it is, when it dies, and the opaque version vector the
 * cluster stamped it with.
 *
 * [expiresAt] is null when the key has no TTL.
 */
internal class RdbEntry(
    val key: Key,
    val value: Value,
    val expiresAt: Instant?,
    val dvv: ByteArray,
) {
    /** Spec 5.4's one rule, the store's own: a key is readable through its deadline and gone after it. */
    fun expired(now: Instant): Boolean = expiresAt != null && now.isAfter(expiresAt)
}

/** Writes a snapshot, skipping the keys that have already expired at [now] (spec 5.4). */
internal object RdbWriter {

    fun write(sink: OutputStream, entries: Iterable<RdbEntry>, now: Instant) {
        // The header names the count, so the live entries are settled before the first byte goes
        // out. Only the references are held; no value is serialized until its turn comes.
        val live = entries.filterNot { it.expired(now) }
        val crc = CRC32()
        val out = DataOutputStream(CheckedOutputStream(sink, crc))
        out.write(RDB_MAGIC.toByteArray(Charsets.US_ASCII))
        out.writeByte(RDB_VERSION.toInt())
        out.writeInt(live.size)
        for (entry in live) writeEntry(out, entry)
        // Read before the checksum itself goes through the checked stream, so it covers only
        // what came before it.
        val checksum = crc.value.toInt()
        out.writeInt(checksum)
        out.flush()
    }

    private fun writeEntry(out: DataOutputStream, entry: RdbEntry) {
        writeBytes(out, entry.key.bytes)
        out.writeByte(CODE_BY_KIND.getValue(entry.value.kind).toInt())
        writeBytes(out, entry.dvv)
        out.writeLong(entry.expiresAt?.toEpochMilli() ?: NO_TTL)
        val value = encode(entry.value)
        writeBytes(out, value)
    }

    /**
     * A value's own bytes. A String is its bytes; every aggregate writes its element count and
     * then its elements, each length-prefixed, so nothing needs a terminator. A sorted set is
     * written in score order and its score as IEEE-754 bits, which round-trip the infinities.
     */
    private fun encode(value: Value): ByteArray = when (value) {
        is Value.Str -> value.bytes
        is Value.Hash -> body { out ->
            val fields = value.fields.entries().toList()
            out.writeInt(fields.size)
            for (field in fields) {
                writeBytes(out, fieldBytes(field.key))
                writeBytes(out, field.value)
            }
        }
        is Value.List -> body { out ->
            out.writeInt(value.items.size)
            for (item in value.items) writeBytes(out, item)
        }
        is Value.ZSet -> body { out ->
            out.writeInt(value.order.size)
            for (scored in value.order.forward()) {
                out.writeLong(scored.score.toRawBits())
                writeBytes(out, scored.member)
            }
        }
    }

    private inline fun body(fill: (DataOutputStream) -> Unit): ByteArray {
        val bytes = ByteArrayOutputStream()
        DataOutputStream(bytes).use(fill)
        return bytes.toByteArray()
    }

    private fun writeBytes(out: DataOutputStream, bytes: ByteArray) {
        out.writeInt(bytes.size)
        out.write(bytes)
    }
}

/**
 * Reads a snapshot back in file order.
 *
 * [seeds] is where a restored sorted set's skip list draws its levels from, the one piece of a
 * value the file does not carry; injecting it keeps a restore reproducible.
 */
internal class RdbReader(private val seeds: Random) {

    fun read(source: InputStream): List<RdbEntry> {
        val crc = CRC32()
        val input = DataInputStream(CheckedInputStream(source, crc))
        try {
            val magic = input.readNBytes(RDB_MAGIC.length).toString(Charsets.US_ASCII)
            if (magic != RDB_MAGIC) throw RdbFormatException(RdbFault.NOT_AN_RDB)
            if (input.readByte() != RDB_VERSION) throw RdbFormatException(RdbFault.UNSUPPORTED_VERSION)
            val count = input.readInt()
            if (count < 0) throw RdbFormatException(RdbFault.TRUNCATED)
            val entries = ArrayList<RdbEntry>(minOf(count, 1024))
            repeat(count) { entries.add(readEntry(input)) }
            val computed = crc.value.toInt()
            if (input.readInt() != computed) throw RdbFormatException(RdbFault.CHECKSUM_MISMATCH)
            return entries
        } catch (cut: EOFException) {
            throw RdbFormatException(RdbFault.TRUNCATED)
        }
    }

    private fun readEntry(input: DataInputStream): RdbEntry {
        val key = Key(readBytes(input))
        val kind = KIND_BY_CODE[input.readByte()] ?: throw RdbFormatException(RdbFault.TRUNCATED)
        val dvv = readBytes(input)
        val ttl = input.readLong()
        val value = DataInputStream(ByteArrayInputStream(readBytes(input)))
        return RdbEntry(key, decode(kind, value), if (ttl == NO_TTL) null else Instant.ofEpochMilli(ttl), dvv)
    }

    private fun decode(kind: Value.Kind, input: DataInputStream): Value = when (kind) {
        Value.Kind.STRING -> Value.Str(input.readAllBytes())
        Value.Kind.HASH -> Value.Hash().also { hash ->
            repeat(count(input)) { hash.fields.put(fieldName(readBytes(input)), readBytes(input)) }
        }
        Value.Kind.LIST -> Value.List().also { list ->
            repeat(count(input)) { list.items.addLast(readBytes(input)) }
        }
        // A restored sorted set is built the way a ZADD builds one, so its two indexes are
        // written together and a member can only ever be in both (I3).
        Value.Kind.ZSET -> Value.ZSet(SkipList(seeds.nextLong())).also { zset ->
            repeat(count(input)) {
                val score = Double.fromBits(input.readLong())
                zset.writeScore(score, readBytes(input))
            }
        }
    }

    /** An element count, refused before it is trusted enough to size a loop with. */
    private fun count(input: DataInputStream): Int {
        val count = input.readInt()
        if (count < 0) throw RdbFormatException(RdbFault.TRUNCATED)
        return count
    }

    /**
     * A length-prefixed byte string. A length longer than what is left reads short rather than
     * allocating what it claims, which is what keeps a corrupt file from asking for two gigabytes
     * before the checksum has had its say.
     */
    private fun readBytes(input: DataInputStream): ByteArray {
        val length = input.readInt()
        if (length < 0) throw RdbFormatException(RdbFault.TRUNCATED)
        val bytes = input.readNBytes(length)
        if (bytes.size != length) throw EOFException("wanted $length bytes, got ${bytes.size}")
        return bytes
    }
}
