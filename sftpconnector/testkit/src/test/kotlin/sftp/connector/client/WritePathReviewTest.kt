package sftp.connector.client

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeoutOrNull
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.NoSuchFile
import sftp.connector.error.ServerFailure
import sftp.connector.error.SftpException
import sftp.connector.pool.SftpPool
import sftp.connector.testkit.EmbeddedSftpServer
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Operation
import sftp.connector.transport.jsch.JschTransport
import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.createDirectory
import kotlin.io.path.exists
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.time.Duration.Companion.seconds

/**
 * Findings of the review of the write path (T7): what a green suite did not prove about what the
 * server is left holding, and about whether the caller is told the truth about it.
 *
 * Everything about the SSH library's own reading of a path, and everything about the POSIX rename
 * extension, runs against the embedded server, because the fake is a server without the
 * extension that takes every path literally - it cannot show either.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class WritePathReviewTest {

    @TempDir
    lateinit var remoteRoot: Path

    @TempDir
    lateinit var local: Path

    /**
     * The SSH library reads `*` and `?` in the last component of a path as a pattern and lists the
     * directory to resolve it, so a delete of `*.csv` used to send one remove per file that matched.
     * A path names one thing; a name the server listed that happens to hold a wildcard must still
     * name only itself.
     */
    @Test
    fun `a delete names one file even when the name looks like a pattern`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        remoteRoot.resolve("drop/a.csv").writeText(CONTENT)
        remoteRoot.resolve("drop/b.csv").writeText(CONTENT)

        withClient { client ->
            assertThatThrownBy { runBlocking { client.delete("/drop/*.csv") } }
                .isInstanceOf(SftpException::class.java)

            assertThat(remoteRoot.resolve("drop/a.csv").exists()).describedAs("a.csv survives").isTrue()
            assertThat(remoteRoot.resolve("drop/b.csv").exists()).describedAs("b.csv survives").isTrue()
        }
    }

    /**
     * The same reading on a rename target: `l*.csv` resolved to the neighbour `ledger-old.csv`, and
     * on a server with the extension the rename then replaced that neighbour and reported success.
     * Under REPLACE that is a silent overwrite of a file nobody named.
     */
    @Test
    fun `a replacing rename lands on the name it was given and not on a neighbour that matches it`() =
        runBlocking<Unit> {
            remoteRoot.resolve("drop/temp").createDirectories()
            remoteRoot.resolve("drop/ledger.csv").writeText(CONTENT)
            remoteRoot.resolve("drop/temp/ledger-old.csv").writeText("yesterday's run")

            withClient { client ->
                // Whether the literal name can exist is the host filesystem's business (Windows
                // refuses it); what must hold everywhere is that the neighbour is untouched.
                runCatching { client.rename("/drop/ledger.csv", "/drop/temp/l*.csv", Overwrite.REPLACE) }

                assertThat(remoteRoot.resolve("drop/temp/ledger-old.csv").readText()).isEqualTo("yesterday's run")
            }
        }

    @Test
    fun `an upload lands on the name it was given and not on a neighbour that matches it`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        remoteRoot.resolve("drop/ledger.csv").writeText("yesterday's run")

        withClient { client ->
            runCatching { client.upload(fileHolding(CONTENT), "/drop/l*.csv", Overwrite.REPLACE) }

            assertThat(remoteRoot.resolve("drop/ledger.csv").readText()).isEqualTo("yesterday's run")
        }
    }

    /** T7 proved this against the fake only; a startup that creates its folders runs twice for real. */
    @Test
    fun `mkdir twice against a real server is content the second time`() = runBlocking<Unit> {
        withClient { client ->
            client.mkdir("/drop/temp", parents = true)
            client.mkdir("/drop/temp", parents = true)
            client.mkdir("/drop/temp")

            assertThat(client.stat("/drop/temp")?.isDirectory).isTrue()
        }
    }

    private fun fileHolding(content: String): Path = local.resolve("ledger.csv").apply { writeText(content) }

    private suspend fun withClient(separateFilesystemAt: String? = null, block: suspend (SftpClient) -> Unit) {
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD, separateFilesystemAt = separateFilesystemAt).use { server ->
            val config = sftpConnector("write-path-review") {
                endpoint { host = server.host; port = server.port }
                auth { password(USER, PASSWORD) }
                hostKey = HostKeyPolicy.AcceptAll
                polling { staging { dir = local } }
            }
            block(SftpClient(SftpPool(JschTransport(config), config), config))
        }
    }

    private fun fakeConfig(): SftpConnectorConfig = sftpConnector("write-path-review") {
        endpoint { host = "fake.example"; port = 22 }
        auth { password(USER, PASSWORD) }
        hostKey = HostKeyPolicy.AcceptAll
    }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"

        private const val CONTENT = "id,amount\n1,42\n"
    }
}
