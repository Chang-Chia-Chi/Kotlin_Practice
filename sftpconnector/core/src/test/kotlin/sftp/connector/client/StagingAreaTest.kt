package sftp.connector.client

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.config.Digest
import sftp.connector.error.Attempt
import sftp.connector.error.IncompleteTransfer
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.listDirectoryEntries

/**
 * The staging dance on its own, with the bytes supplied by the test rather than by a server.
 *
 * Everything I13 is about happens here and nowhere else, so this is where it is proved: one method
 * creates the partial file, and the ways out of it are a successful move and everything else. Both
 * are exercised below, the second one three ways - a transfer that failed, a transfer that was
 * cancelled, and one that delivered a number of bytes nobody promised.
 */
class StagingAreaTest {

    @TempDir
    lateinit var stage: Path

    @Test
    fun `a completed transfer is renamed into place and reports its size and digest`() = runBlocking<Unit> {
        val target = stage.resolve("ledger.csv")

        val landed = StagingArea(Digest.SHA256).receive(target, CONTENT.size.toLong(), ATTEMPT) { it.write(CONTENT) }

        assertThat(landed.path).isEqualTo(target)
        assertThat(landed.size).isEqualTo(CONTENT.size.toLong())
        assertThat(landed.digest).isEqualTo(SHA256_OF_CONTENT)
        assertThat(landed.digestAlgorithm).isEqualTo(Digest.SHA256)
        assertThat(Files.readAllBytes(target)).isEqualTo(CONTENT)
        // The final name and nothing beside it, which is what a caller listing this directory sees.
        assertThat(stage.listDirectoryEntries()).containsExactly(target)
    }

    @Test
    fun `the digest is the one the configuration asked for`() = runBlocking<Unit> {
        val landed = StagingArea(Digest.MD5).receive(stage.resolve("ledger.csv"), CONTENT.size.toLong(), ATTEMPT) {
            it.write(CONTENT)
        }

        assertThat(landed.digest).isEqualTo(MD5_OF_CONTENT)
        assertThat(landed.digestAlgorithm).isEqualTo(Digest.MD5)
    }

    @Test
    fun `a transfer that delivers the wrong number of bytes is refused rather than reported as a file`() =
        runBlocking<Unit> {
            assertThatThrownBy {
                runBlocking {
                    // The listing promised more than arrives, which is what a truncated transfer
                    // looks like from here.
                    StagingArea(Digest.SHA256).receive(stage.resolve("ledger.csv"), CONTENT.size + 10L, ATTEMPT) {
                        it.write(CONTENT)
                    }
                }
            }
                .isInstanceOf(IncompleteTransfer::class.java)
                .hasMessageContaining("ended after ${CONTENT.size} bytes")
                .hasMessageContaining("listing said the file had ${CONTENT.size + 10}")
                .hasMessageContaining("path=/drop/ledger.csv")
        }

    @Test
    fun `I13_no partial file survives a transfer that failed`() = runBlocking<Unit> {
        assertThatThrownBy {
            runBlocking {
                StagingArea(Digest.SHA256).receive(stage.resolve("ledger.csv"), CONTENT.size.toLong(), ATTEMPT) {
                    it.write(CONTENT, 0, 4)
                    throw IOException("the connection broke half way through")
                }
            }
        }.isInstanceOf(IOException::class.java)

        assertThat(stage).isEmptyDirectory()
    }

    @Test
    fun `I13_no partial file survives a byte count that did not add up`() = runBlocking<Unit> {
        runCatching {
            StagingArea(Digest.SHA256).receive(stage.resolve("ledger.csv"), CONTENT.size + 10L, ATTEMPT) {
                it.write(CONTENT)
            }
        }

        // Neither the partial file nor - which matters more - the final name, because whatever
        // finds the final name takes what is under it for a whole file.
        assertThat(stage).isEmptyDirectory()
    }

    @Test
    fun `I13_no partial file survives a cancelled transfer`() = runBlocking<Unit> {
        val started = CompletableDeferred<Unit>()
        val neverAnswers = CompletableDeferred<Unit>()
        val download = CoroutineScope(Dispatchers.Default).launch {
            StagingArea(Digest.SHA256).receive(stage.resolve("ledger.csv"), CONTENT.size.toLong(), ATTEMPT) {
                it.write(CONTENT, 0, 4)
                started.complete(Unit)
                // Begun, and waiting on a server that has stopped answering - the state a shutdown
                // or a consumer that gave up cancels out of.
                neverAnswers.await()
            }
        }

        started.await()
        download.cancelAndJoin()

        assertThat(stage).isEmptyDirectory()
    }

    @Test
    fun `a file downloaded a second time replaces the one that was there`() = runBlocking<Unit> {
        val staging = StagingArea(Digest.SHA256)
        val target = stage.resolve("ledger.csv")
        staging.receive(target, 5, ATTEMPT) { it.write("stale".toByteArray()) }

        val landed = staging.receive(target, CONTENT.size.toLong(), ATTEMPT) { it.write(CONTENT) }

        assertThat(Files.readAllBytes(landed.path)).isEqualTo(CONTENT)
        assertThat(stage.listDirectoryEntries()).containsExactly(target)
    }

    private companion object {
        private val CONTENT = "id,amount\n1,42\n".toByteArray()

        /** Taken from `sha256sum` and `md5sum` over exactly those bytes, not from this code. */
        private const val SHA256_OF_CONTENT =
            "0f7573cb5487f607c74e1f891a1ded6a94a24d81b4c46f6ab92e1c65dd6f36d8"
        private const val MD5_OF_CONTENT = "9ede8f0bbcd1302e2b0b86693491acba"

        private val ATTEMPT = Attempt("sftp.example:22", "download", "/drop/ledger.csv")
    }
}
