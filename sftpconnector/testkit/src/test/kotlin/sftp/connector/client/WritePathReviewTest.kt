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

    /**
     * S6's refusal with something at the target. The server refuses a rename across filesystems
     * with the same status it refuses an occupied target with, and the replace sequence used to
     * read every refusal with an occupied target as "the target is in the way": it deleted a
     * healthy file, sent the rename again, and passed on the second refusal - with the caller told
     * nothing about the file that was gone. On a server offering the POSIX rename extension a
     * refusal is never about the target being occupied, because that server replaces without
     * being asked, so there is nothing to clear and no reason to try.
     */
    @Test
    fun `a replacing rename refused for a reason that is not the target leaves the target alone`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        remoteRoot.resolve("other").createDirectory()
        remoteRoot.resolve("drop/ledger.csv").writeText(CONTENT)
        remoteRoot.resolve("other/ledger.csv").writeText("yesterday's run")

        withClient(separateFilesystemAt = "other") { client ->
            assertThatThrownBy {
                runBlocking { client.rename("/drop/ledger.csv", "/other/ledger.csv", Overwrite.REPLACE) }
            }.isInstanceOf(ServerFailure::class.java)

            assertThat(remoteRoot.resolve("other/ledger.csv").exists())
                .describedAs("the target, which the rename could never have replaced, is still there")
                .isTrue()
            assertThat(remoteRoot.resolve("other/ledger.csv").readText()).isEqualTo("yesterday's run")
            assertThat(remoteRoot.resolve("drop/ledger.csv").readText()).isEqualTo(CONTENT)
        }
    }

    /**
     * A directory at the target is not something REPLACE means to remove, and on a server
     * without the extension the delete would be refused anyway. Against the fake, which does not
     * know a directory from a file when deleting, the old sequence removed the directory and
     * put the file in its place.
     */
    @Test
    fun `a replacing rename refused by a directory at the target does not try to clear it`() = runBlocking<Unit> {
        val server = FakeSftpTransport().file("/drop/ledger.csv", CONTENT).directory("/drop/temp")
        val config = fakeConfig()
        val client = SftpClient(SftpPool(server, config), config)

        assertThatThrownBy { runBlocking { client.rename("/drop/ledger.csv", "/drop/temp", Overwrite.REPLACE) } }
            .isInstanceOf(ServerFailure::class.java)

        assertThat(server.calls.map { it.operation }).doesNotContain(Operation.Delete)
        assertThat(client.stat("/drop/temp")?.isDirectory).isTrue()
    }

    /**
     * T7's promise to T11 was that a missing path reported by a rename is always the source. The
     * server also answers "no such file" when the target's directory does not exist, and that
     * answer used to come back naming the source - so a retry would have looked at the target,
     * found nothing, and reported the source gone while it sat where it always was. What is
     * missing is now looked for: the source still being there means the answer was about the
     * target, and the failure names the target.
     */
    @Test
    fun `a rename into a directory that is not there names the target as missing and leaves the source alone`() =
        runBlocking<Unit> {
            remoteRoot.resolve("drop").createDirectory()
            remoteRoot.resolve("drop/ledger.csv").writeText(CONTENT)

            withClient { client ->
                for (policy in Overwrite.entries) {
                    assertThatThrownBy {
                        runBlocking { client.rename("/drop/ledger.csv", "/drop/nowhere/ledger.csv", policy) }
                    }
                        .describedAs("under $policy")
                        .isInstanceOfSatisfying(NoSuchFile::class.java) {
                            assertThat(it.attempt.path).describedAs("the path reported missing under $policy")
                                .isEqualTo("/drop/nowhere/ledger.csv")
                        }
                    assertThat(remoteRoot.resolve("drop/ledger.csv").readText()).isEqualTo(CONTENT)
                }
            }
        }

    /**
     * The loan is revoked when the block returns, and the revocation used to be a flag read at the
     * start of each call. A call that had passed the check and was still on the wire when the block
     * ended kept using the session after the pool had lent it to somebody else - I2 broken from
     * outside the pool, by a block that launched work it did not wait for. The loan now ends when
     * the last call on it does.
     */
    @Test
    fun `a call still in flight when the block ends keeps the session until it finishes`() = runTest {
        val parked = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val server = FakeSftpTransport(answer = { call ->
            if (call.operation == Operation.Realpath) {
                parked.complete(Unit)
                release.await()
            }
        }).directory("/drop")
        val config = fakeConfig()
        val pool = SftpPool(server, config)
        val client = SftpClient(pool, config)

        val block = async {
            client.withSession {
                val session = this
                backgroundScope.launch { session.realpath("/drop") }
                parked.await()
            }
        }

        assertThat(withTimeoutOrNull(10.seconds) { block.await() })
            .describedAs("withSession returning while a call it launched is still on the wire")
            .isNull()
        assertThat(pool.stats().inUse).describedAs("sessions out while the call is in flight").isEqualTo(1)

        release.complete(Unit)
        block.await()
        assertThat(pool.stats().inUse).isZero()
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
