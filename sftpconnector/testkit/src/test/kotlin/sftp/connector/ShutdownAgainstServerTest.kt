package sftp.connector

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.PoolExhausted
import sftp.connector.testkit.EmbeddedSftpServer
import sftp.connector.testkit.LoopbackConnectProxy
import java.nio.file.Path
import kotlin.io.path.createDirectory
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.writeBytes
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * Closing a started connector while a real download is blocked on a real socket. The tunnel holds
 * the transfer mid-file and never lets it go, so nothing gentler than the cut can end it, and what
 * has to be true afterwards is that the half a file on disk is gone, the session is back and hung
 * up on, and none of that took longer than the bound.
 */
class ShutdownAgainstServerTest {

    @TempDir
    lateinit var remoteRoot: Path

    @TempDir
    lateinit var stage: Path

    private val meters = SimpleMeterRegistry()

    @Test
    fun `S9_closing during a download leaves no partial file, releases the lease, and returns within the bound`() =
        runBlocking<Unit> {
            remoteRoot.resolve("drop").createDirectory()
            remoteRoot.resolve("drop/big.bin").writeBytes(ByteArray(FILE_BYTES) { it.toByte() })

            EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
                LoopbackConnectProxy.start().use { tunnel ->
                    val config = configFor(server, tunnel)
                    val connector = SftpConnector.start(config, meterRegistry = meters)
                    val listed = connector.client.list("/drop").single()

                    val heldMidTransfer = CompletableDeferred<Unit>()
                    tunnel.holdAfter(HOLD_AFTER_BYTES) { heldMidTransfer.complete(Unit) }
                    val download = async { runCatching { connector.client.download(listed, stage.resolve("big.bin")) } }
                    heldMidTransfer.await()
                    assertThat(stage.listDirectoryEntries()).describedAs("the partial file mid-transfer").isNotEmpty()

                    val waited = TimeSource.Monotonic.markNow()
                    connector.close()
                    val took = waited.elapsedNow()

                    assertThat(took)
                        .describedAs("the drain has to run out before the transfer is cut, and the cut hands back well inside the grace")
                        .isBetween(DRAIN, DRAIN + GRACE + SLACK)
                    // The cut fails the transfer with a lost session; its retry meets the closing
                    // pool at the door, which is where an operation in progress at shutdown ends.
                    val ended = withTimeout(10.seconds) { download.await() }.exceptionOrNull()
                    assertThat(ended).isInstanceOfSatisfying(PoolExhausted::class.java) { assertThat(it.closing).isTrue() }
                    assertThat(stage.listDirectoryEntries()).describedAs("partial files left behind").isEmpty()
                    assertThat(connector.pool.stats().total).describedAs("sessions the pool still holds").isZero()
                    assertThat(meters.counter("sftp_pool_evicted_total", "endpoint", config.endpoint.address, "reason", "shutdown").count())
                        .isEqualTo(1.0)
                    withTimeout(5.seconds) { while (server.liveSessions > 0) delay(20) }
                }
            }
        }

    private fun configFor(server: EmbeddedSftpServer, tunnel: LoopbackConnectProxy): SftpConnectorConfig =
        sftpConnector("shutdown-demo") {
            endpoint {
                host = server.host
                port = server.port
                proxy { httpConnect(tunnel.host, tunnel.port) }
            }
            auth { password(USER, PASSWORD) }
            hostKey = HostKeyPolicy.AcceptAll
            pool { maxSize = 1; drainTimeout = DRAIN; cancelGrace = GRACE }
            resilience { retry { backoff = exponential(100.milliseconds, max = 100.milliseconds, jitter = false) } }
            polling { staging { dir = stage }; directories("/drop") }
        }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"

        /** Big enough that the transfer is still going long after the hold stops it. */
        private const val FILE_BYTES = 8 * 1024 * 1024
        private const val HOLD_AFTER_BYTES = 64L * 1024

        private val DRAIN: Duration = 1.seconds
        private val GRACE: Duration = 300.milliseconds

        /** Room for the closes themselves on a loaded machine; the bound being proved is the two above. */
        private val SLACK: Duration = 2.seconds
    }
}
