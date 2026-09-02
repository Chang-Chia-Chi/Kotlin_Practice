package sftp.connector.testkit

import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.HostKeyRejected
import sftp.connector.transport.SftpConnection
import sftp.connector.transport.jsch.JschTransport
import java.nio.file.Files
import java.nio.file.Path

/**
 * The transport against a real server, which is the whole of the walking skeleton's claim: a
 * session opens, resolves a path, and leaves nothing behind when it closes.
 */
class JschTransportTest {

    @TempDir
    lateinit var remoteRoot: Path

    @TempDir
    lateinit var localFiles: Path

    @Test
    fun `a session opens, resolves a path and closes`() = runBlocking {
        Files.createDirectory(remoteRoot.resolve("inbox"))

        withServer { server ->
            JschTransport(configFor(server)).connect().closingAfter { connection ->
                assertThat(connection.realpath("inbox")).isEqualTo("/inbox")
            }
        }
    }

    @Test
    fun `a session opens through an HTTP CONNECT proxy`() = runBlocking {
        Files.createDirectory(remoteRoot.resolve("inbox"))

        withServer { server ->
            LoopbackConnectProxy.start().use { tunnel ->
                JschTransport(configFor(server, through = tunnel)).connect().closingAfter { connection ->
                    assertThat(connection.realpath("inbox")).isEqualTo("/inbox")
                }
            }
        }
    }

    /**
     * The reader thread is the one resource a closed connection can silently keep. It outlives
     * the coroutine that opened it, so nothing in the calling code shows it is still there, and
     * a pool leaking one per evicted session would fill the process with them.
     *
     * `join` returns the instant the thread ends, so this waits on the fact under test rather
     * than on a guessed duration.
     */
    @Test
    fun `the session's reader thread is gone once the connection is closed`() = runBlocking {
        withServer { server ->
            val before = readerThreads()
            val connection = JschTransport(configFor(server)).connect()
            val reader = (readerThreads() - before).singleOrNull()
            assertThat(reader).describedAs("the reader thread JSch started for the new session").isNotNull()

            assertThat(connection.realpath(".")).isEqualTo("/")
            connection.close()

            reader!!.join(THREAD_EXIT_BUDGET_MILLIS)
            assertThat(reader.isAlive).describedAs("the reader thread outlived its session").isFalse()
        }
    }

    /**
     * The other half of the host key policy: without this, accept-all could be doing nothing at
     * all and every test above would still pass.
     *
     * The assertion is on the exception's type, not its wording. JSch's message text is free text
     * that changes between releases; the connector's own type is the contract, and a JSch type
     * arriving here instead would mean the transport seam had let one through.
     */
    @Test
    fun `a strict host key policy refuses a server whose key it has never seen`() = runBlocking {
        val emptyKnownHosts = Files.createFile(localFiles.resolve("known_hosts"))

        withServer { server ->
            val verifying = configFor(server, policy = HostKeyPolicy.Strict(emptyKnownHosts))

            val refusal = runCatching { JschTransport(verifying).connect() }.exceptionOrNull()

            assertThat(refusal).isInstanceOf(HostKeyRejected::class.java)
        }
    }

    private inline fun withServer(body: (EmbeddedSftpServer) -> Unit) =
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use(body)

    /**
     * The embedded server generates a fresh key on every run, so accepting any key is the only
     * policy a test can start from; the one test that needs verifying passes its own.
     */
    private fun configFor(
        server: EmbeddedSftpServer,
        through: LoopbackConnectProxy? = null,
        policy: HostKeyPolicy = HostKeyPolicy.AcceptAll,
    ): SftpConnectorConfig = sftpConnector("walking-skeleton") {
        endpoint {
            host = server.host
            port = server.port
            through?.let { tunnel -> proxy { httpConnect(tunnel.host, tunnel.port) } }
        }
        auth { password(USER, PASSWORD) }
        hostKey = policy
    }

    private fun readerThreads(): Set<Thread> =
        Thread.getAllStackTraces().keys.filterTo(mutableSetOf()) { it.name.startsWith(READER_THREAD_PREFIX) }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"

        /** What JSch names the thread it reads a connected session on. */
        private const val READER_THREAD_PREFIX = "Connect thread "

        private const val THREAD_EXIT_BUDGET_MILLIS = 5_000L
    }
}

private suspend inline fun SftpConnection.closingAfter(body: (SftpConnection) -> Unit) {
    try {
        body(this)
    } finally {
        close()
    }
}
