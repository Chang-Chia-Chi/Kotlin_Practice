package sftp.connector.client

import sftp.connector.config.Digest
import sftp.connector.error.Attempt
import sftp.connector.error.IncompleteTransfer
import sftp.connector.error.UnsafeFileName
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
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
        clearWhatAnEarlierRunLeft(partial, attempt)
        try {
            val tally = Tally(
                Files.newOutputStream(
                    partial,
                    // Together these say "make this file, and fail if anything is already there" -
                    // and the failure is what matters. The partial file's name is the server's
                    // name plus a suffix, so it is predictable to whoever names the file, and
                    // creating it any other way would open whatever is at that name, following a
                    // symbolic link somebody else put there straight into the file it points at.
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE,
                    LinkOption.NOFOLLOW_LINKS,
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
     * Takes away a partial file an earlier run left at [partial], and refuses everything else that
     * might be sitting there.
     *
     * Spec 6.3 says no partial file survives a run and 14.1 defers resume, so a `.part` found here
     * is a fragment nobody is ever going to finish and going over it is the right answer - but only
     * when it is a plain file, which is the only thing this connector can have written. A symbolic
     * link at that name was put there by somebody else, and it is the whole of the attack: the name
     * is the server's name plus a suffix, so anyone who can see the listing can predict it, and a
     * link followed here writes the download into whatever it points at. The link is left where it
     * is rather than removed, because the connector did not put it there and quietly clearing it
     * would take away the only evidence that somebody did.
     *
     * Read without following links, so the question asked is about the entry at that name and not
     * about whatever it leads to.
     */
    private fun clearWhatAnEarlierRunLeft(partial: Path, attempt: Attempt) {
        val existing = try {
            Files.readAttributes(partial, BasicFileAttributes::class.java, LinkOption.NOFOLLOW_LINKS)
        } catch (nothingThere: NoSuchFileException) {
            // The ordinary case, and the only one that costs nothing.
            return
        }
        if (!existing.isRegularFile) {
            throw UnsafeFileName(
                attempt,
                detail = "$partial is not a partial file this connector left behind - it is a link or a " +
                    "directory somebody else put at the name the download would be written under, so " +
                    "nothing was written and it was left where it is",
            )
        }
        Files.delete(partial)
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
