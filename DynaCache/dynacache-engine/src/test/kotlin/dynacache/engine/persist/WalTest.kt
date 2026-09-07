package dynacache.engine.persist

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

class WalTest {

    @TempDir
    lateinit var dir: Path

    @Test
    fun wal_write_read_roundtrip() {
        val wal = dir.resolve("roundtrip.wal")

        WalWriter(wal, firstSeq = 1L).use { writer ->
            writer.append(OP_SET, "alpha".toByteArray())
            writer.append(OP_DEL, "beta".toByteArray())
            writer.append(OP_SET, ByteArray(0))
        }

        val scan = WalReader(wal).readAll()

        assertEquals(
            listOf(
                WalEntry(1L, OP_SET, "alpha".toByteArray()),
                WalEntry(2L, OP_DEL, "beta".toByteArray()),
                WalEntry(3L, OP_SET, ByteArray(0)),
            ),
            scan.entries,
        )
        assertEquals(WalStop.CLEAN_END, scan.stop)
        assertEquals(Files.size(wal), scan.stoppedAt)
    }

    @Test
    fun wal_seq_strictly_increasing() {
        val wal = dir.resolve("seq.wal")

        val first = WalWriter(wal, firstSeq = 100L).use { writer ->
            (1..4).map { writer.append(OP_SET, byteArrayOf(it.toByte())).seq }
        }
        assertEquals(listOf(100L, 101L, 102L, 103L), first)

        // A restart resumes the numbering from where recovery left off, appending to the same file.
        val resumed = WalWriter(wal, firstSeq = first.last() + 1).use { writer ->
            listOf(writer.append(OP_DEL, byteArrayOf(9)).seq)
        }
        assertEquals(listOf(104L), resumed)

        val seqs = WalReader(wal).readAll().entries.map { it.seq }
        assertEquals(listOf(100L, 101L, 102L, 103L, 104L), seqs)
        assertEquals(seqs.sorted().distinct(), seqs)
    }

    @Test
    fun wal_crash_recovery() {
        // A crash mid-write leaves a fragment of the last entry. Torn inside the header and torn
        // inside the payload are both "no complete entry here", reported at the entry's offset.
        assertTornAt(threeEntryLog("torn-header.wal", truncateTo = 2 * ENTRY_BYTES + 5))
        assertTornAt(threeEntryLog("torn-payload.wal", truncateTo = 2 * ENTRY_BYTES + HEADER_BYTES + 2))
    }

    private fun assertTornAt(scan: WalScan) {
        assertEquals(listOf(1L, 2L), scan.entries.map { it.seq })
        assertEquals(WalStop.TORN_TAIL, scan.stop)
        assertEquals(2 * ENTRY_BYTES, scan.stoppedAt)
    }

    @Test
    fun wal_crc_detects_corruption() {
        // A byte flipped in the second entry's payload: the first entry survives, the reader
        // stops where the damage starts and says so.
        val payloadHit = corruptedLog("bad-payload.wal", at = ENTRY_BYTES + HEADER_BYTES + 1)
        assertEquals(listOf(1L), payloadHit.entries.map { it.seq })
        assertEquals(WalStop.CRC_MISMATCH, payloadHit.stop)
        assertEquals(ENTRY_BYTES, payloadHit.stoppedAt)

        // The checksum covers the header too, so a flipped sequence number is caught the same way.
        val seqHit = corruptedLog("bad-seq.wal", at = ENTRY_BYTES + 8)
        assertEquals(listOf(1L), seqHit.entries.map { it.seq })
        assertEquals(WalStop.CRC_MISMATCH, seqHit.stop)
        assertEquals(ENTRY_BYTES, seqHit.stoppedAt)
    }

    /** Three four-byte entries, then a truncation that cuts the third one short. */
    private fun threeEntryLog(name: String, truncateTo: Long): WalScan {
        val wal = writeThreeEntries(name)
        FileChannel.open(wal, StandardOpenOption.WRITE).use { it.truncate(truncateTo) }
        return WalReader(wal).readAll()
    }

    /** Three four-byte entries, then one bit flipped at [at]. */
    private fun corruptedLog(name: String, at: Long): WalScan {
        val wal = writeThreeEntries(name)
        FileChannel.open(wal, StandardOpenOption.READ, StandardOpenOption.WRITE).use { channel ->
            val byte = ByteBuffer.allocate(1)
            channel.read(byte, at)
            byte.flip()
            byte.put(0, (byte.get(0).toInt() xor 0x01).toByte())
            channel.write(byte, at)
        }
        return WalReader(wal).readAll()
    }

    /**
     * T67: an entry may carry an opaque trailer behind its payload -- the cluster's version for
     * the key the command wrote. The writer never looks inside it and the reader hands it back
     * whole, split from the payload the command codec reads.
     */
    @Test
    fun wal_entry_carries_an_opaque_trailer() {
        val wal = dir.resolve("trailer.wal")

        WalWriter(wal, firstSeq = 1L).use { writer ->
            writer.append(OP_SET, "alpha".toByteArray(), "version-of-alpha".toByteArray())
            writer.append(OP_DEL, "beta".toByteArray())
            writer.append(OP_SET, ByteArray(0), "v".toByteArray())
        }

        val scan = WalReader(wal).readAll()

        assertEquals(WalStop.CLEAN_END, scan.stop)
        assertEquals(Files.size(wal), scan.stoppedAt)
        // The op code reads back as the command's own: the trailer flag is the file's, not the codec's.
        assertEquals(listOf(OP_SET, OP_DEL, OP_SET), scan.entries.map { it.op })
        assertEquals(listOf("alpha", "beta", ""), scan.entries.map { String(it.payload) })
        assertEquals(listOf("version-of-alpha", "", "v"), scan.entries.map { String(it.trailer) })
    }

    /**
     * The WAL entry format's version bump is the op code's high bit, so an entry written before
     * T67 -- and every channel-log record, which carries no version at all -- still reads back,
     * with no trailer, which is exactly what it held.
     */
    @Test
    fun wal_entry_without_a_trailer_reads_back_with_none() {
        val wal = writeThreeEntries("untrailered.wal")

        val scan = WalReader(wal).readAll()

        assertEquals(WalStop.CLEAN_END, scan.stop)
        assertEquals(listOf(0, 0, 0), scan.entries.map { it.trailer.size })
        assertEquals(WalEntry(1L, OP_SET, PAYLOAD), scan.entries.first())
    }

    private fun writeThreeEntries(name: String): Path {
        val wal = dir.resolve(name)
        WalWriter(wal, firstSeq = 1L).use { writer ->
            repeat(3) { writer.append(OP_SET, PAYLOAD) }
        }
        assertEquals(3 * ENTRY_BYTES, Files.size(wal))
        return wal
    }

    private companion object {
        const val OP_SET: Byte = 1
        const val OP_DEL: Byte = 2

        /** The spec's header: crc32 + length + seq + op. */
        const val HEADER_BYTES = 4 + 4 + 8 + 1
        val PAYLOAD = "abcd".toByteArray()
        const val ENTRY_BYTES = (HEADER_BYTES + 4).toLong()
    }
}
