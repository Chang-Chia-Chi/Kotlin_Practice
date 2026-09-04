package sftp.connector.testkit

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.client.LocalFile
import sftp.connector.client.SftpClient
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.Overwrite
import sftp.connector.config.SftpConnectorBuilder
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.AuthenticationFailed
import sftp.connector.error.CircuitOpen
import sftp.connector.error.ConnectFailed
import sftp.connector.pool.SftpPool
import sftp.connector.source.Readiness
import sftp.connector.source.ReadinessCheck
import sftp.connector.source.SftpEvent.FileSeen
import sftp.connector.source.SftpSource
import sftp.connector.transport.jsch.JschTransport
import java.nio.file.Path
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.io.path.createDirectories
import kotlin.io.path.createDirectory
import kotlin.io.path.exists
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.readText
import kotlin.io.path.writeBytes
import kotlin.io.path.writeText
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration

/**
 * The scenarios a lost session, a stalled tunnel, a refusing proxy and a wrong password produce,
 * against a real server through a tunnel the test can interfere with.
 *
 * What each one asserts is what the caller was told and what the server is left holding - never
 * how the retry got there. A retry that gets the right answer for the wrong reason would pass
 * here and fail in production, which is why the counts - sessions opened, tries made, sessions
 * thrown away - are asserted too.
 */
class ResilienceAgainstServerTest {

    @TempDir
    lateinit var remoteRoot: Path

    @TempDir
    lateinit var stage: Path

    private val meters = SimpleMeterRegistry()

    private lateinit var pool: SftpPool

    private lateinit var config: SftpConnectorConfig

    /** The breaker's clock, moved by hand: nothing here waits for a wait in open to pass. */
    private val breakerClock = SettableClock()

    /**
     * The server throws the session away in the middle of a download. The consumer sees the file
     * once, the download it asked for completes, and the price is one handshake and one session
     * that is never handed out again.
     */
    @Test
    fun `S1_a session killed mid-download is replaced and the download completes, with the file seen once`() =
        runBlocking<Unit> {
            remoteRoot.resolve("drop").createDirectory()
            remoteRoot.resolve("drop/big.bin").writeBytes(ByteArray(FILE_BYTES) { it.toByte() })
            val watching: SftpConnectorBuilder.() -> Unit = {
                pool { validationBypass = 1.minutes }
                polling { directories("/drop"); readiness = ReadinessCheck { _, _ -> Readiness.Ready } }
            }

            withTunnelledClient(watching) { client, tunnel, server ->
                val source = SftpSource(client, config, meters)
                val heldMidTransfer = CompletableDeferred<Unit>()
                tunnel.holdAfter(HOLD_AFTER_BYTES) { heldMidTransfer.complete(Unit) }

                val seen = mutableListOf<FileSeen>()
                var landed: LocalFile? = null
                source.poll("/drop").collect { event ->
                    if (event !is FileSeen) return@collect
                    seen += event
                    val killer = launch {
                        heldMidTransfer.await()
                        server.killLiveSessions()
                        tunnel.resume()
                    }
                    landed = event.download(stage.resolve("big.bin"))
                    killer.join()
                }

                assertThat(seen).describedAs("the file, handed over once").hasSize(1)
                assertThat(landed?.size).isEqualTo(FILE_BYTES.toLong())
                assertThat(stage.listDirectoryEntries().map { it.fileName.toString() }).containsExactly("big.bin")
                assertThat(retries("download")).isEqualTo(1.0)
                assertThat(evictedAsPoisoned()).describedAs("the session the server killed").isEqualTo(1.0)
                assertThat(sessionsOpened()).describedAs("the first, and the one the retry ran on").isEqualTo(2.0)
            }
        }

    /**
     * The tunnel goes quiet under a call. Nothing but the keepalive ends it, which takes two
     * intervals; the session it was on is thrown away, the call is tried again on a new one, and
     * the caller never hears about any of it. The breaker did: one failure, which a window of
     * two makes visible as an open breaker once the retry's success has filled the window.
     */
    @Test
    fun `S2_a stall past the keepalive poisons the session and the call is retried on a fresh one`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        val quickToNotice: SftpConnectorBuilder.() -> Unit = {
            pool { keepAlive = KEEPALIVE; validationBypass = 1.minutes }
            resilience { circuitBreaker { slidingWindow = 2; failureRateThreshold = 50 } }
        }

        withTunnelledClient(quickToNotice) { client, tunnel, _ ->
            // Parked through the pool alone, so the breaker's window holds only the call under test.
            pool.withLease { it.connection.realpath(".") }
            tunnel.stall()

            assertThat(client.exists("/drop")).isTrue()

            assertThat(retries("exists")).isEqualTo(1.0)
            assertThat(evictedAsPoisoned()).isEqualTo(1.0)
            assertThat(sessionsOpened()).isEqualTo(2.0)
            assertThat(breakerState())
                .describedAs("one failure then one success is half of a window of two, so the failure was counted")
                .isEqualTo(2)
        }
    }

    /**
     * The proxy refuses for a while. Once enough dials have failed the breaker opens and the
     * next call is refused here, without a dial; when the wait in open has passed the next call
     * is let through as the probe, and the server answering it closes the breaker again.
     */
    @Test
    fun `S3_the breaker opens on failed dials, refuses without dialling, and closes on a successful probe`() =
        runBlocking<Unit> {
            val oneTryEach: SftpConnectorBuilder.() -> Unit = {
                resilience {
                    retry { maxAttempts = 1 }
                    circuitBreaker { slidingWindow = 2; failureRateThreshold = 50; waitInOpen = WAIT_IN_OPEN }
                }
            }

            withTunnelledClient(oneTryEach) { client, tunnel, _ ->
                tunnel.refuseConnections()
                repeat(2) {
                    assertThatThrownBy { runBlocking { client.exists("/") } }.isInstanceOf(ConnectFailed::class.java)
                }
                val dialled = tunnel.connectsAsked

                assertThatThrownBy { runBlocking { client.exists("/") } }
                    .isInstanceOf(CircuitOpen::class.java)
                    .hasMessageContaining("op=exists")
                assertThat(tunnel.connectsAsked).describedAs("dials while the breaker is open").isEqualTo(dialled)
                assertThat(breakerState()).isEqualTo(2)

                tunnel.acceptConnections()
                breakerClock.advance(WAIT_IN_OPEN + 1.milliseconds)

                assertThat(client.exists("/")).describedAs("the half-open probe").isTrue()
                assertThat(breakerState()).isEqualTo(0)
                assertThat(retries("exists")).isZero()
            }
        }

    /**
     * A wrong password is not a flaky network. It is refused once, nothing asks again, and the
     * breaker - sized here so that a single counted failure would open it - is untouched.
     */
    @Test
    fun `S10_a wrong password is refused once, never retried, and never held against the server`() = runBlocking<Unit> {
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
            config = sftpConnector("wrong-password") {
                endpoint { host = server.host; port = server.port }
                auth { password(USER, "not the password") }
                hostKey = HostKeyPolicy.AcceptAll
                pool { maxSize = 1 }
                resilience { circuitBreaker { slidingWindow = 1 } }
            }
            // How many passwords one connect offers is the SSH library's business; what matters
            // is that the client's one call offers exactly one connect's worth.
            runCatching { JschTransport(config).connect() }
            val perConnect = server.authAttempts
            pool = SftpPool(JschTransport(config, meters), config, meters)
            val client = SftpClient(pool, config, meters)

            assertThatThrownBy { runBlocking { client.exists("/") } }.isInstanceOf(AuthenticationFailed::class.java)

            assertThat(server.authAttempts).describedAs("one more connect's worth of passwords").isEqualTo(perConnect * 2)
            assertThat(retries("exists")).isZero()
            assertThat(breakerState()).isZero()
            assertThat(sessionsOpened()).isZero()
        }
    }

    /**
     * The rename goes out, the server does it, and the reply is lost on the way back. The retry
     * on a fresh session is told the source is not there, looks at the target, finds the file
     * with the size it had, and reports the truth: the move happened.
     */
    @Test
    fun `I11_a rename whose reply is lost on the wire is retried and reports the move that landed`() = runBlocking<Unit> {
        remoteRoot.resolve("drop/temp").createDirectories()
        remoteRoot.resolve("drop/ledger.csv").writeText(CONTENT)

        withTunnelledClient({ pool { keepAlive = KEEPALIVE; validationBypass = 1.minutes } }) { client, tunnel, _ ->
            val listed = checkNotNull(client.stat("/drop/ledger.csv"))
            // Fires once what the client sent has been passed on, so the request reaches the
            // server and its reply is what the stall swallows.
            tunnel.onNextClientRequest { tunnel.stall() }

            client.rename("/drop/ledger.csv", "/drop/temp/ledger.csv", Overwrite.REPLACE, listed)

            assertThat(remoteRoot.resolve("drop/temp/ledger.csv").readText()).isEqualTo(CONTENT)
            assertThat(remoteRoot.resolve("drop/ledger.csv").exists()).isFalse()
            assertThat(retries("rename")).isEqualTo(1.0)
            assertThat(sessionsOpened()).isEqualTo(2.0)
        }
    }

    private fun retries(op: String): Double =
        meters.find("sftp_retry_total").tag("op", op).counter()?.count() ?: 0.0

    private fun breakerState(): Int = meters.get("sftp_breaker_state").gauge().value().toInt()

    private fun sessionsOpened(): Double = meters.find("sftp_pool_created_total").counter()?.count() ?: 0.0

    private fun evictedAsPoisoned(): Double =
        meters.find("sftp_pool_evicted_total").tag("reason", "poisoned").counter()?.count() ?: 0.0

    /**
     * A client reaching a real server through a tunnel the test can interfere with, over a pool
     * of exactly one session, retrying without waiting - the backoff is virtual time's business
     * and is proved elsewhere.
     */
    private suspend fun withTunnelledClient(
        extra: SftpConnectorBuilder.() -> Unit = {},
        block: suspend (SftpClient, LoopbackConnectProxy, EmbeddedSftpServer) -> Unit,
    ) {
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
            LoopbackConnectProxy.start().use { tunnel ->
                config = configFor(server, tunnel, extra)
                // The keepalive interval bounds the key exchange as well, and the first key
                // exchange in a JVM is far slower than every one after it. This throwaway
                // connection warms it, on the shipped interval.
                JschTransport(configFor(server, tunnel, extra = {})).connect().close()
                pool = SftpPool(JschTransport(config, meters), config, meters)
                block(SftpClient(pool, config, meters, breakerClock), tunnel, server)
            }
        }
    }

    private fun configFor(
        server: EmbeddedSftpServer,
        tunnel: LoopbackConnectProxy,
        extra: SftpConnectorBuilder.() -> Unit,
    ): SftpConnectorConfig = sftpConnector("resilience-demo") {
        endpoint {
            host = server.host
            port = server.port
            proxy { httpConnect(tunnel.host, tunnel.port) }
        }
        auth { password(USER, PASSWORD) }
        hostKey = HostKeyPolicy.AcceptAll
        pool { maxSize = 1 }
        polling { staging { dir = stage } }
        resilience { retry { backoff = exponential(1.milliseconds, max = 1.milliseconds, jitter = false) } }
        extra()
    }

    private class SettableClock : Clock() {
        @Volatile
        private var now: Instant = Instant.EPOCH

        fun advance(by: Duration) {
            now = now.plus(by.toJavaDuration())
        }

        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId): Clock = this
        override fun instant(): Instant = now
    }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"
        private const val CONTENT = "id,amount\n1,42\n"

        private const val FILE_BYTES = 2 * 1024 * 1024
        private const val HOLD_AFTER_BYTES = 64L * 1024

        private val KEEPALIVE = 400.milliseconds
        private val WAIT_IN_OPEN = 1.minutes
    }
}
