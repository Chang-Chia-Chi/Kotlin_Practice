package sftp.connector.client

import sftp.connector.config.Digest
import sftp.connector.error.Attempt
import sftp.connector.error.IncompleteTransfer
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.security.MessageDigest
import java.util.HexFormat

/**
 * Where downloaded bytes land before anyone is allowed to see them.
 *
 * Writing a file under its final name and filling it afterwards means every reader of that
 * directory - the application, another process, an operator - can open a file that is still being
 * written and get half of it. So bytes go to a partial file, are counted and digested as they
 * arrive, are checked against the size the server said the file had, and only then does the file
 * take its final name, in one move the filesystem cannot show half of.
 *
 * That is one operation with one guarantee, not four steps for a caller to sequence: **however
 * [receive] ends - a broken transfer, a cancelled coroutine, a byte count that does not add up,
 * an error nobody expected - the partial file is not there afterwards.** A caller cannot forget
 * the cleanup because a caller is never told about the partial file at all.
 */
internal class StagingArea(private val algorithm: Digest) {

    /**
     * Runs [transfer] into a partial file beside [target] and, if exactly [expectedSize] bytes
     * arrive, moves it onto [target] and reports what landed there.
     *
     * @param attempt what to blame if the transfer comes up short.
     * @throws IncompleteTransfer when the byte count does not match, which means the stream ended
     *   somewhere other than the end of the file.
     */
    suspend fun receive(
        target: Path,
        expectedSize: Long,
        attempt: Attempt,
        transfer: suspend (OutputStream) -> Unit,
    ): LocalFile {
        val partial = target.resolveSibling("${target.fileName}$PARTIAL_SUFFIX")
        try {
            val tally = Tally(
                Files.newOutputStream(
                    partial,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE,
                ),
                algorithm,
            )
            tally.use { transfer(it) }

            if (tally.count != expectedSize) {
                throw IncompleteTransfer(
                    attempt,
                    "the transfer ended after ${tally.count} bytes where the listing said the file " +
                        "had $expectedSize, so what arrived is not the whole file",
                )
            }

            // The one step a reader of this directory can observe, and it takes the file from
            // absent to complete with nothing in between.
            Files.move(partial, target, StandardCopyOption.ATOMIC_MOVE)
            return LocalFile(target, tally.count, tally.hex(), algorithm)
        } finally {
            // On the path that succeeded the move already took the partial file away, so this does
            // nothing. On every other path - and there is no way out of this method that is not one
            // of the two - it is what stops a fragment of a file being left where a later run, or a
            // person, would find it and take it for a whole one.
            Files.deleteIfExists(partial)
        }
    }

    /**
     * Counts and digests the bytes on their way to disk.
     *
     * Both answers come free here because the bytes are already passing through: reading the file
     * back to digest it would double the I/O, and asking the filesystem for the size afterwards
     * would answer about whatever is on disk rather than about what the transfer delivered.
     */
    private class Tally(private val sink: OutputStream, algorithm: Digest) : OutputStream() {

        private val digest = MessageDigest.getInstance(algorithm.algorithmName)

        var count: Long = 0
            private set

        override fun write(b: Int) {
            sink.write(b)
            digest.update(b.toByte())
            count++
        }

        override fun write(b: ByteArray, off: Int, len: Int) {
            sink.write(b, off, len)
            digest.update(b, off, len)
            count += len
        }

        override fun flush() = sink.flush()

        override fun close() = sink.close()

        fun hex(): String = HexFormat.of().formatHex(digest.digest())
    }

    private companion object {
        private const val PARTIAL_SUFFIX = ".part"
    }
}
