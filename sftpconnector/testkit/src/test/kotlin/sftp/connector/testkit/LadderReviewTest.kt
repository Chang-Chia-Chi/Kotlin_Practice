package sftp.connector.testkit

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.sftpConnector
import sftp.connector.pool.SftpPool
import sftp.connector.transport.jsch.JschTransport
import java.nio.file.Path
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Findings of the review of T8 and the pool's cancellation paths, against the real adapter.
 *
 * The scripted transport answers a connect on the caller's own coroutine, so a cancellation that
 * lands while it is answering is one the caller sees before the session exists. The real adapter
 * runs the handshake on another thread, and that is where the gap the fake cannot show opens up.
 */
class LadderReviewTest {

    @TempDir
    lateinit var remoteRoot: Path

    private val meters = SimpleMeterRegistry()

    /**
     * T4 closed the gap between a connect returning to the pool and the entry being told about it.
     * The same gap exists one layer down: the handshake runs on the IO dispatcher, and a caller
     * cancelled while it runs has its result thrown away by the scope that carried it - a session
     * with a socket and a reader thread that the pool was never handed and so could never close.
     * The pool's own accounting is untouched either way, which is why the server is asked.
     *
     * The cancellation is landed by the tunnel, on the first bytes the client sends, so the
     * handshake has begun and will finish whether or not anybody is waiting for it.
     */
    @Test
    fun `I4_a session that finishes its handshake into a cancelled caller is hung up on, not left running`() =
        runBlocking<Unit> {
            EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
                LoopbackConnectProxy.start().use { tunnel ->
                    val config = configFor(server, tunnel)
                    val pool = SftpPool(JschTransport(config, meters), config, meters)

                    lateinit var givingUp: Job
                    tunnel.onNextClientRequest { givingUp.cancel() }
                    givingUp = launch { pool.acquire() }
                    givingUp.join()

                    assertThat(pool.stats().total).describedAs("what the pool thinks it holds").isZero()
                    // The server is the one party that cannot be lied to about an open session.
                    val hungUp = withTimeoutOrNull(SERVER_NOTICES_WITHIN) {
                        while (server.liveSessions > 0) delay(POLL)
                    }
                    assertThat(hungUp)
                        .describedAs("a session the server is still holding after the caller gave up on it")
                        .isNotNull()
                }
            }
        }

    private fun configFor(server: EmbeddedSftpServer, tunnel: LoopbackConnectProxy): SftpConnectorConfig =
        sftpConnector("ladder-review") {
            endpoint {
                host = server.host
                port = server.port
                proxy { httpConnect(tunnel.host, tunnel.port) }
            }
            auth { password(USER, PASSWORD) }
            hostKey = HostKeyPolicy.AcceptAll
            pool { maxSize = 1 }
        }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"

        /** A hang-up crosses loopback; a session still open after this is one nothing closed. */
        private val SERVER_NOTICES_WITHIN = 5.seconds
        private val POLL = 20.milliseconds
    }
}
