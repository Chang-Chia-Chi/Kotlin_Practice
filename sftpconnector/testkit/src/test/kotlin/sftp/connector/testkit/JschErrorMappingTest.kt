package sftp.connector.testkit

import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorBuilder
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.AuthenticationFailed
import sftp.connector.error.ConnectFailed
import sftp.connector.error.HostKeyRejected
import sftp.connector.error.LeaseFate
import sftp.connector.error.Retry
import sftp.connector.error.SessionLost
import sftp.connector.error.SftpException
import sftp.connector.error.WatchReaction
import sftp.connector.transport.jsch.JschTransport
import java.net.InetAddress
import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Every row of the error table, against a server that really produces the condition.
 *
 * A table row written from the library's source, or from what its messages used to say, is a row
 * that has never been checked: this fork rejects an unrecognised host key with "reject HostKey:"
 * where older versions said "UnknownHostKey:", and only a real refusal from a real server catches
 * that. So each test here stages the actual fault - a wrong password, a killed server, a tunnel
 * that goes quiet, a proxy port with nothing behind it - and reads what comes out.
 *
 * Every one of them also proves the boundary. [failureFrom] insists on the connector's own
 * exception type, so a JSch type escaping through the transport fails whichever row let it out.
 * That is a hole the architecture rules cannot see, because they inspect what a class imports and
 * not what its methods throw.
 */
class JschErrorMappingTest {

    @TempDir
    lateinit var remoteRoot: Path

    @TempDir
    lateinit var localFiles: Path

    @Test
    fun `S10_a wrong password is fatal, is not held against the server, and stops the watch`() = runBlocking<Unit> {
        withServer { server ->
            val failure = failureFrom {
                connectTo(configFor(server) { auth { password(USER, "not the password") } })
            }

            assertThat(failure).isInstanceOf(AuthenticationFailed::class.java)
            assertThat(failure.disposition.retry).isEqualTo(Retry.NEVER)
            assertThat(failure.disposition.countsAgainstTheBreaker).isFalse()
            assertThat(failure.disposition.watch).isEqualTo(WatchReaction.STOP)
        }
    }

    @Test
    fun `a host key the connector was not told to expect is fatal`() = runBlocking<Unit> {
        val emptyKnownHosts = Files.createFile(localFiles.resolve("known_hosts"))

        withServer { server ->
            val failure = failureFrom {
                connectTo(configFor(server) { hostKey = HostKeyPolicy.Strict(emptyKnownHosts) })
            }

            assertThat(failure).isInstanceOf(HostKeyRejected::class.java)
            assertThat(failure.disposition.watch).isEqualTo(WatchReaction.STOP)
        }
    }

    /** Nothing ever answers the handshake, so the connect timeout is the only thing that ends it. */
    @Test
    fun `a server that accepts the socket and then says nothing is a failure to connect`() = runBlocking<Unit> {
        ServerSocket(0, 1, InetAddress.getLoopbackAddress()).use { mute ->
            val failure = failureFrom {
                connectTo(configFor(LOOPBACK, mute.localPort) { pool { connectTimeout = 400.milliseconds } })
            }

            assertThat(failure).isInstanceOf(ConnectFailed::class.java)
            assertThat(failure.disposition.retry).isEqualTo(Retry.IMMEDIATELY)
            assertThat(failure.disposition.lease).isEqualTo(LeaseFate.EVICTED)
        }
    }

    /** The two commonest ways a deployment fails on its first day. */
    @Test
    fun `a refused port and a name that does not resolve are both failures to connect`() = runBlocking<Unit> {
        val closedPort = ServerSocket(0, 1, InetAddress.getLoopbackAddress()).use { it.localPort }

        assertThat(failureFrom { connectTo(configFor(LOOPBACK, closedPort)) })
            .isInstanceOf(ConnectFailed::class.java)
        assertThat(failureFrom { connectTo(configFor("no.such.host.invalid", 22)) })
            .isInstanceOf(ConnectFailed::class.java)
    }

    @Test
    fun `a proxy with nothing behind it is a failure to connect`() = runBlocking<Unit> {
        val deadProxyPort = LoopbackConnectProxy.start().use { it.port }

        withServer { server ->
            val failure = failureFrom { connectTo(configFor(server, viaProxyPort = deadProxyPort)) }

            assertThat(failure).isInstanceOf(ConnectFailed::class.java)
            assertThat(failure).hasMessageContaining("proxy")
        }
    }

    /**
     * SSH is fine, the account is fine, and the server simply does not offer SFTP. No session ever
     * becomes usable, which is what makes this a failure to connect rather than a lost one.
     */
    @Test
    fun `a server that refuses the SFTP subsystem is a failure to connect`() = runBlocking<Unit> {
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD, offersSftp = false).use { server ->
            val failure = failureFrom { connectTo(configFor(server)) }

            assertThat(failure).isInstanceOf(ConnectFailed::class.java)
        }
    }

    /**
     * The tunnel stops moving bytes without closing, so the request is neither answered nor
     * refused and only a clock can end it.
     *
     * The clock is `keepAlive`, not `socketTimeout`: JSch implements the keepalive by making it
     * the socket's read timeout and giving up once its probes go unanswered, which also means the
     * same value bounds the key exchange. Too long and this test waits; too short and the
     * handshake itself times out, which fails the connect instead of the read and tests nothing.
     * Two seconds clears a warmed-up handshake by an order of magnitude, and the throwaway
     * connection below is what warms it - the first key exchange in a JVM is far slower than
     * every one after it.
     */
    @Test
    fun `S2_a tunnel that goes quiet loses the session, poisons it, and counts against the server`() = runBlocking<Unit> {
        withServer { server ->
            LoopbackConnectProxy.start().use { tunnel ->
                val config = configFor(server, viaProxyPort = tunnel.port) {
                    pool { keepAlive = 2.seconds }
                }
                JschTransport(config).connect().close()

                val connection = JschTransport(config).connect()
                tunnel.stall()

                val failure = failureFrom { connection.realpath("inbox") }

                assertThat(failure).isInstanceOf(SessionLost::class.java)
                assertThat((failure as SessionLost).poisons).isTrue()
                assertThat(failure.disposition.lease).isEqualTo(LeaseFate.EVICTED)
                assertThat(failure.disposition.countsAgainstTheBreaker).isTrue()
                assertThat(failure).hasMessageContainingAll("op=realpath", "path=inbox")
            }
        }
    }

    @Test
    fun `a server that goes away under a live session loses the session`() = runBlocking<Unit> {
        val server = EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD)
        val connection = JschTransport(configFor(server)).connect()
        server.close()

        val failure = failureFrom { connection.realpath("inbox") }

        assertThat(failure).isInstanceOf(SessionLost::class.java)
        assertThat(failure.disposition.lease).isEqualTo(LeaseFate.EVICTED)
    }

    /** What an operator has to be able to read off one line without going looking for the rest. */
    @Test
    fun `a failure says which server, which operation and which try it was`() = runBlocking<Unit> {
        withServer { server ->
            val failure = failureFrom {
                connectTo(configFor(server) { auth { password(USER, "not the password") } })
            }

            assertThat(failure).hasMessageContainingAll(
                "endpoint=$LOOPBACK:${server.port}",
                "op=connect",
                "attempt=1",
            )
        }
    }

    private suspend fun connectTo(config: SftpConnectorConfig) {
        JschTransport(config).connect()
    }

    /**
     * Runs [body] and insists it failed with one of the connector's own errors. A JSch type
     * arriving here is the transport seam leaking, whatever else the row was checking.
     */
    private suspend fun failureFrom(body: suspend () -> Unit): SftpException {
        val thrown = try {
            body()
            null
        } catch (failure: Throwable) {
            failure
        }

        assertThat(thrown).describedAs("the staged fault produced no failure at all").isNotNull()
        assertThat(thrown)
            .describedAs("what the transport let out")
            .isInstanceOf(SftpException::class.java)
        return thrown as SftpException
    }

    private inline fun withServer(body: (EmbeddedSftpServer) -> Unit) =
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use(body)

    private fun configFor(
        server: EmbeddedSftpServer,
        viaProxyPort: Int? = null,
        extra: SftpConnectorBuilder.() -> Unit = {},
    ) = configFor(server.host, server.port, viaProxyPort, extra)

    /**
     * Timeouts are short so that a test which has to wait for one is over in the time a suite can
     * afford; the connector's own defaults are tens of seconds, which is right in production and
     * useless here.
     */
    private fun configFor(
        host: String,
        port: Int,
        viaProxyPort: Int? = null,
        extra: SftpConnectorBuilder.() -> Unit = {},
    ): SftpConnectorConfig = sftpConnector("error-table") {
        endpoint {
            this.host = host
            this.port = port
            viaProxyPort?.let { proxy { httpConnect(LOOPBACK, it) } }
        }
        auth { password(USER, PASSWORD) }
        hostKey = HostKeyPolicy.AcceptAll
        pool {
            connectTimeout = BRIEF
            socketTimeout = BRIEF
            keepAlive = BRIEF
        }
        extra()
    }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"
        private const val LOOPBACK = "127.0.0.1"
        private val BRIEF: Duration = 5.seconds
    }
}
