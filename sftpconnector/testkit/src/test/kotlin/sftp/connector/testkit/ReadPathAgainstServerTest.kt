package sftp.connector.testkit

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.client.SftpClient
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.NoSuchFile
import sftp.connector.pool.SftpPool
import sftp.connector.transport.jsch.JschTransport
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.createDirectory
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.readText
import kotlin.io.path.writeText

/**
 * The read path against a real SFTP server rather than a script.
 *
 * The fake transport proves what the client does with what it is handed; only a real server proves
 * what it is handed. Everything below is about the seam itself: that a directory listing arrives
 * entry by entry over the wire, that a download of real bytes digests to what the file really
 * hashes to, and that a file removed underneath a download comes back as the connector's own
 * "not there" rather than as a JSch type or an unrecognised failure.
 */
class ReadPathAgainstServerTest {

    @TempDir
    lateinit var remoteRoot: Path

    @TempDir
    lateinit var stage: Path

    @Test
    fun `a real listing reports the files of a directory with their sizes`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        remoteRoot.resolve("drop/ledger.csv").writeText(CONTENT)
        remoteRoot.resolve("drop/notes.txt").writeText("x")
        remoteRoot.resolve("drop/archive").createDirectory()

        withClient { client ->
            val seen = client.list("/drop").toList()

            assertThat(seen.map { it.name }).containsExactlyInAnyOrder("ledger.csv", "notes.txt")
            assertThat(seen.single { it.name == "ledger.csv" }.size).isEqualTo(CONTENT.length.toLong())
        }
    }

    @Test
    fun `a real download lands under its final name and digests to what the file hashes to`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        remoteRoot.resolve("drop/ledger.csv").writeText(CONTENT)

        withClient { client ->
            val landed = client.download(client.list("/drop").toList().single())

            assertThat(landed.path).isEqualTo(stage.resolve("ledger.csv"))
            assertThat(landed.path.readText()).isEqualTo(CONTENT)
            assertThat(landed.digest).isEqualTo(SHA256_OF_CONTENT)
            assertThat(stage.listDirectoryEntries()).containsExactly(landed.path)
        }
    }

    @Test
    fun `stat and exists answer about a real path and about one that is not there`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        remoteRoot.resolve("drop/ledger.csv").writeText(CONTENT)

        withClient { client ->
            assertThat(client.stat("/drop/ledger.csv")?.size).isEqualTo(CONTENT.length.toLong())
            assertThat(client.exists("/drop/ledger.csv")).isTrue()
            assertThat(client.stat("/drop/never-existed.csv")).isNull()
            assertThat(client.exists("/drop/never-existed.csv")).isFalse()
        }
    }

    /**
     * A real server answering a real request for a file another consumer has just moved away. It
     * is the mapping the layer above depends on to tell a file that has gone from an operation that
     * failed, and it is proved here rather than against the fake because the fake is where the
     * answer would be assumed rather than observed.
     */
    @Test
    fun `a file removed between the listing and the download is reported as a path that is not there`() =
        runBlocking<Unit> {
            remoteRoot.resolve("drop").createDirectory()
            remoteRoot.resolve("drop/ledger.csv").writeText(CONTENT)

            withClient { client ->
                val listed = client.list("/drop").toList().single()
                Files.delete(remoteRoot.resolve("drop/ledger.csv"))

                assertThatThrownBy { runBlocking { client.download(listed) } }
                    .isInstanceOf(NoSuchFile::class.java)
                    .hasMessageContaining("/drop/ledger.csv")

                assertThat(stage).isEmptyDirectory()
            }
        }

    /**
     * S11. A hundred thousand entries, and the caller wants a thousand.
     *
     * The claim is not merely that a thousand arrive - a listing that read the whole directory and
     * handed on the first thousand would do that too - but that the other ninety-nine thousand are
     * never looked at. So the count asserted is the number of entries that reached the connector at
     * all, taken inside the callback the server's own batches drive. It is exactly the number the
     * caller asked for, which is what "does not materialise" means when it is written down as a
     * number: the work, and therefore the memory, is bounded by the request rather than by the
     * directory.
     */
    @Test
    fun `S11_a hundred thousand entries with a limit of a thousand stops after a thousand`() = runBlocking<Unit> {
        val drop = remoteRoot.resolve("drop").createDirectory()
        repeat(ENTRIES) { Files.createFile(drop.resolve("file-$it.csv")) }
        val reachedTheConnector = AtomicInteger()

        withClient { client ->
            val seen = client
                .list("/drop", maxEntries = WANTED, filter = { reachedTheConnector.incrementAndGet(); true })
                .toList()

            assertThat(seen).hasSize(WANTED)
            assertThat(reachedTheConnector.get()).isEqualTo(WANTED)
        }
    }

    private suspend fun withClient(block: suspend (SftpClient) -> Unit) {
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
            val config = configFor(server)
            block(SftpClient(SftpPool(JschTransport(config), config), config))
        }
    }

    private fun configFor(server: EmbeddedSftpServer): SftpConnectorConfig = sftpConnector("read-path-demo") {
        endpoint { host = server.host; port = server.port }
        auth { password(USER, PASSWORD) }
        // The embedded server generates a fresh key per instance, so there is no key a test could
        // have recorded in advance.
        hostKey = HostKeyPolicy.AcceptAll
        polling { staging { dir = stage } }
    }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"

        private const val CONTENT = "id,amount\n1,42\n"

        /** Taken from `sha256sum` over exactly those bytes, not from this code. */
        private const val SHA256_OF_CONTENT =
            "0f7573cb5487f607c74e1f891a1ded6a94a24d81b4c46f6ab92e1c65dd6f36d8"

        private const val ENTRIES = 100_000
        private const val WANTED = 1_000
    }
}
