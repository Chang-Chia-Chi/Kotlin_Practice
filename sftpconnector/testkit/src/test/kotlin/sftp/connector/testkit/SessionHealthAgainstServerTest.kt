package sftp.connector.testkit

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.pool.SftpPool
import sftp.connector.transport.jsch.JschTransport
import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * The two things that keep a pooled session honest, against a real server rather than a script:
 * the session speaks while it has nothing to say, and the pool asks before it hands one on.
 *
 * Both are answers to the same fact about this network - nothing tells either end when a tunnel
 * has been dropped - and neither can be proved without a peer. A scripted transport would agree
 * that a killed session is dead because the script said so.
 */
class SessionHealthAgainstServerTest {

    @TempDir
    lateinit var remoteRoot: Path

    /**
     * The whole point of validating on borrow, staged end to end: the server throws the session
     * away while it is parked, nobody tells the pool, and the caller after that is handed a
     * working session anyway and never learns that anything happened.
     */
    @Test
    fun `a session the server killed while it was parked is replaced before the caller sees it`() =
        runBlocking<Unit> {
            EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
                val meters = SimpleMeterRegistry()
                // Nothing is worth bypassing here: the whole session has been away since the
                // moment it was parked, so every borrow is worth a question.
                val config = config(server, keepAlive = 30.seconds, validationBypass = Duration.ZERO)
                val pool = SftpPool(JschTransport(config), config, meters)

                val first = pool.acquire()
                val killed = first.connection
                assertThat(killed.realpath(".")).isEqualTo("/")
                first.release()

                server.killLiveSessions()

                val second = pool.acquire()
                assertThat(second.connection)
                    .describedAs("the pool handed back the session the server had thrown away")
                    .isNotSameAs(killed)
                assertThat(second.connection.realpath(".")).isEqualTo("/")
                assertThat(meters.find("sftp_pool_evicted_total").tag("reason", "validation").counter()?.count())
                    .describedAs("sessions retired because they failed to answer")
                    .isEqualTo(1.0)

                second.release()
            }
        }

    /**
     * A keepalive is a request whose reply nobody reads, so the only proof that a session is
     * speaking while idle is the server hearing it. It matters twice over: it is what stops the
     * proxy dropping a quiet tunnel, and it is also the interval on which a read the server has
     * stopped answering finally fails.
     */
    @Test
    fun `a session keeps speaking on its own at the interval it was given`() = runBlocking<Unit> {
        val spoke = CompletableDeferred<String>()
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD, onGlobalRequest = { spoke.complete(it) })
            .use { server ->
                // The first key exchange in a JVM is by far the slowest, and the keepalive interval
                // is also what bounds it, so a short one on a cold JVM fails the handshake instead
                // of the test it was written for. This connection pays for the warm-up.
                JschTransport(config(server, keepAlive = 30.seconds)).connect().close()

                val quiet = JschTransport(config(server, keepAlive = KEEPALIVE)).connect()
                try {
                    assertThat(withTimeout(10.seconds) { spoke.await() })
                        .describedAs("what an idle session said to the server unprompted")
                        .contains("keepalive")
                } finally {
                    quiet.close()
                }
            }
    }

    private fun config(
        server: EmbeddedSftpServer,
        keepAlive: Duration,
        validationBypass: Duration = 500.milliseconds,
    ): SftpConnectorConfig = sftpConnector("session-health-demo") {
        endpoint { host = server.host; port = server.port }
        auth { password(USER, PASSWORD) }
        // The embedded server generates a fresh key per instance, so there is no key a test could
        // have recorded in advance.
        hostKey = HostKeyPolicy.AcceptAll
        pool {
            maxSize = 2
            acquireTimeout = 2.seconds
            this.keepAlive = keepAlive
            this.validationBypass = validationBypass
        }
    }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"

        /** Short enough that the test does not sit waiting, long enough to outlast a warm handshake. */
        private val KEEPALIVE = 400.milliseconds
    }
}
