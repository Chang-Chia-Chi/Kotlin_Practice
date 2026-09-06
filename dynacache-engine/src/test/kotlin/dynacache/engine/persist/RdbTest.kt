package dynacache.engine.persist

import dynacache.engine.Key
import dynacache.engine.Value
import dynacache.engine.ds.SkipList
import dynacache.engine.fieldName
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.time.Instant
import java.util.Random

/**
 * The RDB codec through its own seam: entries in, bytes out, entries back. Nothing here reads
 * the file layout except the tests that are about the layout.
 */
class RdbTest {

    private val now: Instant = Instant.parse("2026-09-06T12:00:00Z")

    private fun save(entries: List<RdbEntry>, at: Instant = now): ByteArray =
        ByteArrayOutputStream().also { RdbWriter.write(it, entries, at) }.toByteArray()

    private fun load(bytes: ByteArray): List<RdbEntry> =
        RdbReader(Random(31)).read(ByteArrayInputStream(bytes)).entries

    @Test
    fun rdb_empty_snapshot_roundtrip() {
        assertEquals(emptyList<RdbEntry>(), load(save(emptyList())))
    }

    @Test
    fun rdb_version_byte_present() {
        val file = save(emptyList())
        assertEquals(RDB_VERSION, file[RDB_MAGIC.length])

        // And it is honoured, not decoration: a file from a future version is not guessed at.
        val future = file.copyOf().also { it[RDB_MAGIC.length] = (RDB_VERSION + 1).toByte() }
        assertEquals(RdbFault.UNSUPPORTED_VERSION, assertThrows(RdbFormatException::class.java) { load(future) }.fault)
    }

    @Test
    fun rdb_save_restore_roundtrip() {
        val ttl = now.plusSeconds(600)
        val saved = listOf(
            entry("plain", Value.Str(bytes("hello")), null, dvv = byteArrayOf(1, 2, 3)),
            // Binary safety, in the key, the value and the version vector alike.
            entry(binaryText, Value.Str(byteArrayOf(0, -1, 7)), ttl, dvv = byteArrayOf(0, -128, 127)),
            entry("profile", hash("name" to "ada", "role" to "engineer", binaryText to "\u0000\u00ff"), ttl, byteArrayOf(9)),
            entry("queue", list("first", "second", "third"), null, ByteArray(0)),
            entry("board", zset(100.0 to "alice", -2.5 to "bob", Double.POSITIVE_INFINITY to "carol"), ttl, byteArrayOf(4)),
        )

        assertEquals(saved.map(::shape), load(save(saved)).map(::shape))
    }

    @Test
    fun rdb_excludes_expired() {
        val saved = listOf(
            entry("gone", Value.Str(bytes("x")), now.minusMillis(1), ByteArray(0)),
            entry("also-gone", list("x"), now.minusSeconds(3600), ByteArray(0)),
            // A key is readable through its deadline, so the one expiring exactly now is still in.
            entry("on-the-line", Value.Str(bytes("x")), now, ByteArray(0)),
            entry("alive", Value.Str(bytes("x")), now.plusMillis(1), ByteArray(0)),
            entry("forever", Value.Str(bytes("x")), null, ByteArray(0)),
        )

        val restored = load(save(saved)).map { it.key.toString() }

        assertEquals(listOf("on-the-line", "alive", "forever"), restored)
    }

    @Test
    fun rdb_bad_checksum_rejected() {
        val file = save(listOf(entry("k", Value.Str(bytes("value")), null, byteArrayOf(1))))

        // One flipped bit anywhere in the body, well away from the header and the checksum itself.
        val corrupt = file.copyOf().also { it[it.size - 8] = (it[it.size - 8].toInt() xor 1).toByte() }

        assertEquals(RdbFault.CHECKSUM_MISMATCH, assertThrows(RdbFormatException::class.java) { load(corrupt) }.fault)
    }

    @Test
    fun rdb_truncated_file_rejected() {
        val file = save(listOf(entry("k", hash("f" to "v"), now.plusSeconds(60), byteArrayOf(1, 2))))

        // Cut mid-entry, and cut in the checksum itself: both are files with no whole snapshot in
        // them, and neither yields the entries that did survive.
        for (kept in listOf(file.size / 2, file.size - 1)) {
            val fault = assertThrows(RdbFormatException::class.java) { load(file.copyOf(kept)) }.fault
            assertEquals(RdbFault.TRUNCATED, fault, "truncated to $kept of ${file.size} bytes")
        }
    }

    @Test
    fun rdb_not_an_rdb_rejected() {
        val alien = "not a snapshot at all, no".toByteArray()
        assertEquals(RdbFault.NOT_AN_RDB, assertThrows(RdbFormatException::class.java) { load(alien) }.fault)
    }

    // --- building and reading values, in the words the engine uses for them ---

    /** A name no text encoding survives: the codec must be counting bytes, not characters. */
    private val binaryText = "k\u0000\u00ff"

    private fun bytes(text: String) = text.toByteArray(Charsets.ISO_8859_1)

    private fun entry(key: String, value: Value, expiresAt: Instant?, dvv: ByteArray) =
        RdbEntry(Key(bytes(key)), value, expiresAt, dvv)

    private fun hash(vararg fields: Pair<String, String>) =
        Value.Hash().also { h -> fields.forEach { (f, v) -> h.fields.put(fieldName(bytes(f)), bytes(v)) } }

    private fun list(vararg items: String) = Value.List().also { l -> items.forEach { l.items.addLast(bytes(it)) } }

    private fun zset(vararg scored: Pair<Double, String>) =
        Value.ZSet(SkipList(7L)).also { z -> scored.forEach { (s, m) -> z.writeScore(s, bytes(m)) } }

    /**
     * Everything about an entry that must survive the file, as something comparable. A sorted set
     * shows both of its indexes, so a restore that filled one and not the other fails here.
     */
    private fun shape(entry: RdbEntry): String {
        val value = when (val v = entry.value) {
            is Value.Str -> "str ${v.bytes.toList()}"
            is Value.Hash -> "hash " + v.fields.entries().map { "${it.key}=${it.value.toList()}" }.sorted().toList()
            is Value.List -> "list " + v.items.map { it.toList() }
            is Value.ZSet -> "zset " + v.order.forward().map { "${it.score}:${it.member.toList()}" }.toList() +
                " scores " + v.scores.entries().map { "${it.key}=${it.value}" }.sorted().toList()
        }
        return "${entry.key.bytes.toList()} ${entry.expiresAt} ${entry.dvv.toList()} $value"
    }
}
