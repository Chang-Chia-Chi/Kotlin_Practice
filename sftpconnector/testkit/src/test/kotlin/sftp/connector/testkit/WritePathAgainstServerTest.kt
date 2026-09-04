package sftp.connector.testkit

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.client.SftpClient
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.Overwrite
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.NoSuchFile
import sftp.connector.error.SftpException
import sftp.connector.pool.SftpPool
import sftp.connector.transport.jsch.JschTransport
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.createDirectory
import kotlin.io.path.exists
import kotlin.io.path.isDirectory
import kotlin.io.path.readText
import kotlin.io.path.writeText

/**
 * The write path against a real SFTP server rather than a script.
 *
 * The fake proves what the connector does with the answers it is given; only a real server proves
 * which answers it gets. That matters most for the overwrite policy, because the answer to a rename
 * onto an occupied path is the one thing about this protocol that differs between servers.
 */
class WritePathAgainstServerTest {

    @TempDir
    lateinit var remoteRoot: Path

    @TempDir
    lateinit var local: Path

    @Test
    fun `a real upload puts the bytes on the server and a download brings them back`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()

        withClient { client ->
            client.upload(fileHolding(CONTENT), "/drop/ledger.csv")

            assertThat(remoteRoot.resolve("drop/ledger.csv").readText()).isEqualTo(CONTENT)
            val landed = client.download(client.list("/drop").toList().single(), local.resolve("back.csv"))
            assertThat(landed.path.readText()).isEqualTo(CONTENT)
        }
    }

    @Test
    fun `a real upload told not to replace leaves the file that is already there`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        remoteRoot.resolve("drop/ledger.csv").writeText("the file that is already there")

        withClient { client ->
            assertThatThrownBy { runBlocking { client.upload(fileHolding(CONTENT), "/drop/ledger.csv") } }
                .isInstanceOf(SftpException::class.java)
                .hasMessageContaining("/drop/ledger.csv")

            assertThat(remoteRoot.resolve("drop/ledger.csv").readText()).isEqualTo("the file that is already there")
        }
    }

    @Test
    fun `a real rename moves the file`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        remoteRoot.resolve("drop/temp").createDirectory()
        remoteRoot.resolve("drop/ledger.csv").writeText(CONTENT)

        withClient { client ->
            client.rename("/drop/ledger.csv", "/drop/temp/ledger.csv")

            assertThat(remoteRoot.resolve("drop/ledger.csv").exists()).isFalse()
            assertThat(remoteRoot.resolve("drop/temp/ledger.csv").readText()).isEqualTo(CONTENT)
        }
    }

    /**
     * The case the whole overwrite policy exists for: something is already at the target. What the
     * server does about it is not the same on every server - one offering the POSIX rename extension
     * replaces the target in a single request, one without it refuses and leaves the connector to
     * clear the way and ask again - so what is asserted here is the outcome both paths must reach,
     * and the fake-transport suite is where the second path's sequence is pinned request by request.
     */
    @Test
    fun `a real rename told to replace lands on a target that was already there`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        remoteRoot.resolve("drop/temp").createDirectory()
        remoteRoot.resolve("drop/ledger.csv").writeText(CONTENT)
        remoteRoot.resolve("drop/temp/ledger.csv").writeText("yesterday's run")

        withClient { client ->
            client.rename("/drop/ledger.csv", "/drop/temp/ledger.csv", Overwrite.REPLACE)

            assertThat(remoteRoot.resolve("drop/ledger.csv").exists()).isFalse()
            assertThat(remoteRoot.resolve("drop/temp/ledger.csv").readText()).isEqualTo(CONTENT)
        }
    }

    /**
     * The measurement this test exists for: **this server replaces the target if the rename is
     * sent.** It advertises the POSIX rename extension, the SSH library uses it without being
     * asked, and the request comes back a success with the old file gone. So refusing cannot be
     * left to the server, and what is asserted here is that the connector refuses on its own -
     * against the very server that would not have.
     */
    @Test
    fun `a real rename told not to replace leaves the target that was already there`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        remoteRoot.resolve("drop/temp").createDirectory()
        remoteRoot.resolve("drop/ledger.csv").writeText(CONTENT)
        remoteRoot.resolve("drop/temp/ledger.csv").writeText("yesterday's run")

        withClient { client ->
            assertThatThrownBy {
                runBlocking { client.rename("/drop/ledger.csv", "/drop/temp/ledger.csv") }
            }.isInstanceOf(SftpException::class.java)

            assertThat(remoteRoot.resolve("drop/temp/ledger.csv").readText()).isEqualTo("yesterday's run")
            assertThat(remoteRoot.resolve("drop/ledger.csv").readText()).isEqualTo(CONTENT)
        }
    }

    /**
     * The signal a retry reads after a reply goes missing. The source is gone because the earlier
     * attempt landed, and the connector says so about the source - which is what tells a retry to
     * go and look at the target rather than report a failure.
     */
    @Test
    fun `a real rename whose source is not there reports the source as missing`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        remoteRoot.resolve("drop/temp").createDirectory()
        remoteRoot.resolve("drop/temp/ledger.csv").writeText(CONTENT)

        withClient { client ->
            assertThatThrownBy {
                runBlocking { client.rename("/drop/ledger.csv", "/drop/temp/ledger.csv", Overwrite.REPLACE) }
            }
                .isInstanceOf(NoSuchFile::class.java)
                .hasMessageContaining("/drop/ledger.csv")

            // The evidence the retry would then go looking for: the target is there, whole.
            assertThat(client.stat("/drop/temp/ledger.csv")?.size).isEqualTo(CONTENT.length.toLong())
        }
    }

    @Test
    fun `delete, mkdir with parents and exists round trip against a real server`() = runBlocking<Unit> {
        withClient { client ->
            client.mkdir("/drop/temp/2026", parents = true)

            assertThat(remoteRoot.resolve("drop/temp/2026").isDirectory()).isTrue()
            assertThat(client.exists("/drop/temp/2026")).isTrue()
            assertThat(client.exists("/drop/temp/2025")).isFalse()

            client.upload(fileHolding(CONTENT), "/drop/temp/2026/ledger.csv")
            assertThat(client.exists("/drop/temp/2026/ledger.csv")).isTrue()

            client.delete("/drop/temp/2026/ledger.csv")
            assertThat(client.exists("/drop/temp/2026/ledger.csv")).isFalse()
            assertThat(remoteRoot.resolve("drop/temp/2026/ledger.csv").exists()).isFalse()

            assertThatThrownBy { runBlocking { client.delete("/drop/temp/2026/ledger.csv") } }
                .isInstanceOf(NoSuchFile::class.java)
        }
    }

    @Test
    fun `mkdir under a parent that is not there is refused rather than invented`() = runBlocking<Unit> {
        withClient { client ->
            assertThatThrownBy { runBlocking { client.mkdir("/drop/temp/2026") } }
                .isInstanceOf(SftpException::class.java)

            assertThat(remoteRoot.resolve("drop").exists()).isFalse()
        }
    }

    /**
     * Several operations on one session, which is what a caller reaches for withSession to get.
     * Here it is a stage-then-publish: the file is written under a name nothing is watching and
     * renamed into place, and the rename is the moment the finished file appears.
     */
    @Test
    fun `withSession runs a whole sequence against a real server on one session`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()

        withClient { client ->
            client.withSession {
                mkdir("/drop/temp")
                Files.newInputStream(fileHolding(CONTENT)).use {
                    writeFrom("/drop/temp/ledger.csv.uploading", it)
                }
                rename("/drop/temp/ledger.csv.uploading", "/drop/temp/ledger.csv")
            }

            assertThat(remoteRoot.resolve("drop/temp/ledger.csv").readText()).isEqualTo(CONTENT)
            assertThat(remoteRoot.resolve("drop/temp/ledger.csv.uploading").exists()).isFalse()
        }
    }

    private fun fileHolding(content: String): Path = local.resolve("ledger.csv").apply { writeText(content) }

    private suspend fun withClient(block: suspend (SftpClient) -> Unit) {
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
            val config = configFor(server)
            block(SftpClient(SftpPool(JschTransport(config), config), config))
        }
    }

    private fun configFor(server: EmbeddedSftpServer): SftpConnectorConfig = sftpConnector("write-path-demo") {
        endpoint { host = server.host; port = server.port }
        auth { password(USER, PASSWORD) }
        // The embedded server generates a fresh key per instance, so there is no key a test could
        // have recorded in advance.
        hostKey = HostKeyPolicy.AcceptAll
        polling { staging { dir = local } }
    }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"

        private const val CONTENT = "id,amount\n1,42\n"
    }
}
