package sftp.connector.testkit

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.client.SftpClient
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.Overwrite
import sftp.connector.config.SftpConnectorBuilder
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.OperationTimeout
import sftp.connector.error.SessionLost
import sftp.connector.pool.EntryState
import sftp.connector.pool.SftpPool
import sftp.connector.transport.jsch.JschTransport
import java.nio.file.Path
import kotlin.io.path.createDirectory
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.writeBytes
import kotlin.io.path.writeText
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * What a cancelled caller costs, against a real server through a real tunnel.
 *
 * Cancelling a coroutine cancels a coroutine, and the thread inside the SSH library goes on
 * reading a socket that has never heard of coroutines. Everything here is about what happens next,
 * and specifically about the difference between the two ways a blocked call can end: one that
 * notices and stops, leaving a session the next caller can have, and one that has to be cut apart
 * to give its thread back, which costs a handshake. Which of the two happened is the only thing
 * that decides the session's fate, so it is what each test below actually asserts.
 *
 * The tunnel is the instrument. A stall holds a call still with nothing behind it that could ever
 * unblock it; a hold stops a transfer at a byte count of the test's choosing and then lets the same
 * transfer carry on, which is how a cancellation is landed in the middle of a real transfer rather
 * than near it.
 */
class CancellationLadderTest {

    @TempDir
    lateinit var remoteRoot: Path

    @TempDir
    lateinit var stage: Path

    private val meters = SimpleMeterRegistry()

    private lateinit var pool: SftpPool

    private lateinit var endpoint: String

    /**
     * The transfer is stopped mid-file, the caller gives up, and the transfer is let go again so
     * that the next chunk of bytes is what carries the news. That is the cheap rung: the library
     * asks between chunks whether anybody still wants this, hears no, and closes the remote handle
     * cleanly.
     *
     * The join is asserted well inside the grace period on purpose. Finishing within the grace is
     * what proves nothing was cut apart, because the grace is the earliest the cutting could have
     * begun.
     */
    @Test
    fun `a download cancelled in mid transfer stops itself, leaves nothing behind, and keeps its session`() =
        runBlocking<Unit> {
            remoteRoot.resolve("drop").createDirectory()
            remoteRoot.resolve("drop/big.bin").writeBytes(ByteArray(FILE_BYTES) { it.toByte() })
            // Zero, so every borrow after the first asks the server whether the session is still
            // there. A session that only looks healthy would then not survive to be asserted on.
            val proveEveryBorrow: SftpConnectorBuilder.() -> Unit =
                { pool { cancelGrace = 3.seconds; validationBypass = Duration.ZERO } }

            withTunnelledClient(proveEveryBorrow) { client, tunnel ->
                // Opened and parked before anything is armed, so the bytes counted below are the
                // file's rather than a handshake's.
                val listed = client.list("/drop").single()

                val heldMidTransfer = CompletableDeferred<Unit>()
                tunnel.holdAfter(HOLD_AFTER_BYTES) { heldMidTransfer.complete(Unit) }
                val download = launch { client.download(listed, stage.resolve("big.bin")) }
                heldMidTransfer.await()

                download.cancel()
                tunnel.resume()
                withTimeout(1.seconds) { download.join() }

                // The bytes the server was made to send are the only place a transfer that stopped
                // and one that ran to the end of the file differ from outside. A monitor that
                // never says no would pull all eight megabytes and still tidy up afterwards.
                assertThat(tunnel.bytesDelivered)
                    .describedAs("bytes the server sent before it was told to stop")
                    .isBetween(HOLD_AFTER_BYTES, FILE_BYTES / 8L)

                assertThat(stage.listDirectoryEntries())
                    .describedAs("a partial file, or worse a final name over half a file, left behind")
                    .isEmpty()
                assertThat(pool.stats().idle).describedAs("the session the cancelled download had").isEqualTo(1)

                assertThat(client.exists("/drop/big.bin")).isTrue()
                assertThat(sessionsOpened())
                    .describedAs("one session, dialled once: a second means the cancellation cost a handshake")
                    .isEqualTo(1.0)
            }
        }

    /**
     * The listing's version of the same rung, which was there before this ticket: a consumer that
     * stops collecting leaves the entry with nowhere to go, and the selector says stop rather than
     * being told to. What is new is what becomes of the session afterwards.
     */
    @Test
    fun `a listing its consumer walked away from leaves the session fit for the next operation`() = runBlocking<Unit> {
        val drop = remoteRoot.resolve("drop").createDirectory()
        repeat(ENTRIES) { drop.resolve("file-$it.csv").writeText("x") }

        withTunnelledClient { client, _ ->
            val seen = client.list("/drop").take(WANTED).toList()

            assertThat(seen).hasSize(WANTED)
            assertThat(pool.stats().idle).isEqualTo(1)
            assertThat(client.exists("/drop/file-0.csv")).isTrue()
            assertThat(sessionsOpened())
                .describedAs("one session listed and then answered; a second means the first was thrown away")
                .isEqualTo(1.0)
        }
    }

    /**
     * Nobody cancelled anything here. The server accepted a request and went quiet, which is the
     * floor underneath the whole ladder: the keepalive probes go unanswered and the SSH library
     * ends the read itself, whether or not any caller had lost interest.
     *
     * The interval is shortened so the test does not wait a minute for it, and the throwaway
     * connection in the harness is what makes that safe - the same value bounds the key exchange,
     * and the first key exchange in a JVM is slow enough to fail a short one. Validation on borrow
     * is switched off for the length of the test, so that the call left blocked is the one this is
     * about rather than the pool's own liveness check.
     */
    @Test
    fun `a server that goes quiet ends the call itself, and the session goes with it`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()

        // One try, because this is about how the call ends and not about what happens next: with
        // the retries T11 added, the stalled call would be tried again on a fresh tunnel and the
        // lost session would never reach the caller. The assertion is unchanged.
        val oneTry: SftpConnectorBuilder.() -> Unit = {
            pool { keepAlive = KEEPALIVE; validationBypass = 1.minutes }
            resilience { retry { maxAttempts = 1 } }
        }
        withTunnelledClient(oneTry) { client, tunnel ->
            client.exists("/drop")
            tunnel.stall()

            val waited = TimeSource.Monotonic.markNow()
            assertThatThrownBy { runBlocking { client.exists("/drop") } }
                .isInstanceOf(SessionLost::class.java)
            val bound = waited.elapsedNow()

            assertThat(bound)
                .describedAs("a probe is sent after one interval and given up on after the next")
                .isBetween(KEEPALIVE, KEEPALIVE * 10)
            assertThat(pool.stats().total).describedAs("a session nobody should be handed next").isZero()
            assertThat(evictedAsPoisoned()).isEqualTo(1.0)
        }
    }

    /**
     * The rung nothing else reaches. The tunnel is holding a call that will never be answered, and
     * the caller gives up: the transfer monitor is no help because no byte will ever arrive to ask
     * it anything, and the keepalive is a minute away. So the grace runs out and the session is cut
     * apart, which is the one thing a blocked socket read cannot ignore.
     *
     * Without the cut this test does not fail, it hangs - until the default keepalive gives up a
     * minute later - and that is the whole of what the ticket is about.
     */
    @Test
    fun `a call nothing else unblocks is cut loose after the grace, and its entry ends closed`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()

        withTunnelledClient({ pool { cancelGrace = GRACE; validationBypass = 1.minutes } }) { client, tunnel ->
            client.exists("/drop")
            tunnel.stall()

            val lent = CompletableDeferred<StateFlow<EntryState>>()
            val onTheWire = CompletableDeferred<Unit>()
            tunnel.onNextClientRequest { onTheWire.complete(Unit) }
            val stuck = launch {
                pool.withLease { lease ->
                    lent.complete(lease.state)
                    lease.connection.realpath("/drop")
                }
            }
            // The request is on the wire, so the thread that sent it is committed to waiting for
            // an answer the stalled tunnel will never bring. Cancelling before that would be
            // cancelling a call that had not started.
            val entry = lent.await()
            onTheWire.await()

            val waited = TimeSource.Monotonic.markNow()
            stuck.cancel()
            withTimeout(BEFORE_THE_KEEPALIVE_WOULD) { stuck.join() }
            val bound = waited.elapsedNow()

            assertThat(bound)
                .describedAs("either nothing waited for the grace, or something waited far past it")
                .isBetween(GRACE, GRACE * 20)
            assertThat(entry.value).isEqualTo(EntryState.Closed)
            assertThat(pool.stats().total).describedAs("the entry the cut session belonged to").isZero()
            assertThat(evictedAsPoisoned()).isEqualTo(1.0)
        }
    }

    /**
     * The same cut, during a download rather than during a bare round trip, which is where it can
     * leave something behind. The transfer is stopped mid-file and never let go again, so no byte
     * arrives to ask the monitor anything and the grace is the only thing left - and what has to
     * be true afterwards is that the half a file already on disk is not.
     *
     * The failure the cut raises travels out through the staging area on its way to being dropped,
     * which is what runs the cleanup. That is the arm of this invariant the read path could not
     * prove before there was an abort to prove it with.
     */
    @Test
    fun `I13_no partial file survives a transfer the pool had to cut apart`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        remoteRoot.resolve("drop/big.bin").writeBytes(ByteArray(FILE_BYTES) { it.toByte() })

        withTunnelledClient({ pool { cancelGrace = GRACE; validationBypass = 1.minutes } }) { client, tunnel ->
            val listed = client.list("/drop").single()

            val heldMidTransfer = CompletableDeferred<Unit>()
            tunnel.holdAfter(HOLD_AFTER_BYTES) { heldMidTransfer.complete(Unit) }
            val download = launch { client.download(listed, stage.resolve("big.bin")) }
            heldMidTransfer.await()
            assertThat(stage.listDirectoryEntries())
                .describedAs("the partial file the cleanup below has to remove")
                .isNotEmpty()

            download.cancel()
            withTimeout(BEFORE_THE_KEEPALIVE_WOULD) { download.join() }

            assertThat(stage.listDirectoryEntries()).isEmpty()
            assertThat(pool.stats().total).isZero()
        }
    }

    /**
     * A caller gives up on a call that is already hanging, and the keepalive gives up on the
     * server a moment later - inside the grace, so nothing is ever cut apart. The call stopped in
     * time, which is what the cheap rung looks like from outside, and the session is dead anyway.
     *
     * So what ended the call is asked what became of the session rather than the answer being read
     * off the clock. Without that this is where a dead session goes quietly back on the shelf, and
     * the next caller finds out.
     */
    @Test
    fun `a cancelled call the keepalive ends inside the grace still costs the session`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        val graceLongerThanTheKeepalive: SftpConnectorBuilder.() -> Unit =
            { pool { keepAlive = KEEPALIVE; cancelGrace = 5.seconds; validationBypass = 1.minutes } }

        withTunnelledClient(graceLongerThanTheKeepalive) { client, tunnel ->
            client.exists("/drop")
            tunnel.stall()

            val onTheWire = CompletableDeferred<Unit>()
            tunnel.onNextClientRequest { onTheWire.complete(Unit) }
            val stuck = launch { client.exists("/drop") }
            onTheWire.await()

            val waited = TimeSource.Monotonic.markNow()
            stuck.cancel()
            withTimeout(BEFORE_THE_KEEPALIVE_WOULD) { stuck.join() }

            assertThat(waited.elapsedNow())
                .describedAs("the keepalive ended it, so this must be well inside the grace of five seconds")
                .isLessThan(2.seconds)
            assertThat(pool.stats().total).describedAs("a session the keepalive had already lost").isZero()
            assertThat(evictedAsPoisoned()).isEqualTo(1.0)
        }
    }

    /**
     * The pool asking a parked session whether it is still there is a blocking call like any
     * other, made before any lease exists - so a caller that gives up while the pool is asking a
     * dead server is a caller blocked on a socket read, and nothing in its own code said so.
     */
    @Test
    fun `a borrow cancelled while the pool is proving a session is cut loose as well`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()

        withTunnelledClient({ pool { cancelGrace = GRACE; validationBypass = Duration.ZERO } }) { client, tunnel ->
            client.exists("/drop")
            tunnel.stall()

            val onTheWire = CompletableDeferred<Unit>()
            tunnel.onNextClientRequest { onTheWire.complete(Unit) }
            val stuck = launch { client.exists("/drop") }
            // The proving round trip is the first thing a borrow of a parked session sends, so
            // this is the request that has gone out and this is what the borrow is blocked on.
            onTheWire.await()

            val waited = TimeSource.Monotonic.markNow()
            stuck.cancel()
            withTimeout(BEFORE_THE_KEEPALIVE_WOULD) { stuck.join() }

            assertThat(waited.elapsedNow())
                .describedAs("either nothing waited for the grace, or something waited far past it")
                .isBetween(GRACE, GRACE * 20)
            assertThat(pool.stats().total).isZero()
        }
    }

    /**
     * The rung C1 was about: not a stalled *read* but a blocked *write*. The tunnel stops reading
     * from the client, so the client's send buffer fills and JSch's upload parks inside the socket
     * write - holding the session's write lock, which is the lock the keepalive probe and a plain
     * `disconnect()` both need. So neither of the two gentler tiers can end it, and a forced
     * disconnect that went through JSch's orderly channel close would park on that same lock. The
     * fix closes the retained socket first; this proves the cut still lands within the grace.
     *
     * Without it this does not fail, it times out: the write, the reader thread and the cutting
     * thread all wait for the kernel to give up on the connection, minutes away.
     */
    @Test
    fun `a cancelled upload on a tunnel that stopped reading is cut within the grace`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        val big = stage.resolve("big.bin")
        big.writeBytes(ByteArray(BLACK_HOLE_FILE_BYTES) { it.toByte() })

        withTunnelledClient({ pool { cancelGrace = GRACE; validationBypass = 1.minutes } }) { client, tunnel ->
            // Open and park the one session, so the bytes the black hole counts are the upload's.
            client.exists("/drop")

            val blocked = CompletableDeferred<Unit>()
            tunnel.blackHoleClientAfter(BLACK_HOLE_AFTER_BYTES) { blocked.complete(Unit) }
            val upload = launch { client.upload(big, "/drop/big.bin", Overwrite.REPLACE) }
            blocked.await()

            val waited = TimeSource.Monotonic.markNow()
            upload.cancel()
            withTimeout(BEFORE_THE_KEEPALIVE_WOULD) { upload.join() }
            val bound = waited.elapsedNow()

            assertThat(bound)
                .describedAs("either the socket close ended the write near the grace, or nothing did and it waited far past it")
                .isBetween(GRACE, GRACE * 20)
            assertThat(pool.stats().total).describedAs("the entry the cut session belonged to").isZero()
            assertThat(evictedAsPoisoned()).isEqualTo(1.0)
        }
    }

    /**
     * Lens 1 H1. A slow collector fills the listing's buffer and the hand-off parks the IO thread.
     * The time limiter cancels the listing's coroutine - not the collector, which stays alive with
     * the channel open - so the parked thread is freed only if the hand-off watches that coroutine
     * as well as the channel. When it does, the selector answers stop within a slice, JSch closes
     * the handle cleanly, and the session goes back healthy; the caller is told the request timed
     * out.
     *
     * Without the fix the hand-off watches the channel alone, the time limiter cannot reach it, the
     * wait is the collector's own three seconds, and the grace runs out and cuts a healthy session
     * apart - so the timing and the poison count are both wrong.
     */
    @Test
    fun `a listing whose collector stalls is stopped by the time limiter without destroying its session`() =
        runBlocking<Unit> {
            val drop = remoteRoot.resolve("drop").createDirectory()
            repeat(LISTING_FILES) { drop.resolve("file-$it.csv").writeText("x") }

            val timeLimited: SftpConnectorBuilder.() -> Unit = {
                pool { cancelGrace = 200.milliseconds; validationBypass = 1.minutes; acquireTimeout = 100.milliseconds }
                resilience { transferTimeout = 500.milliseconds; retry { maxAttempts = 1 } }
            }
            withTunnelledClient(timeLimited) { client, _ ->
                val waited = TimeSource.Monotonic.markNow()
                val failure = runCatching {
                    var first = true
                    client.list("/drop").collect {
                        // Take one, then never take another: the buffer fills, the hand-off parks,
                        // and only the time limiter can end it - through the coroutine, not the
                        // channel this collector is keeping open.
                        if (first) {
                            first = false
                            delay(3.seconds)
                        }
                    }
                }.exceptionOrNull()
                val bound = waited.elapsedNow()

                assertThat(failure).isInstanceOf(OperationTimeout::class.java)
                assertThat(bound)
                    .describedAs("the time limiter reached the parked thread, rather than the collector's own 3 s")
                    .isLessThan(500.milliseconds + 200.milliseconds + 1.seconds)
                assertThat(evictedAsPoisoned())
                    .describedAs("a healthy session cut apart because the cancellation never reached it")
                    .isZero()
            }
        }

    private fun sessionsOpened(): Double =
        meters.counter("sftp_pool_created_total", "endpoint", endpoint).count()

    private fun evictedAsPoisoned(): Double =
        meters.counter("sftp_pool_evicted_total", "endpoint", endpoint, "reason", "poisoned").count()

    /**
     * A client reaching a real server through a tunnel the test can interfere with, over a pool of
     * exactly one session - so that "the session" is an unambiguous thing to assert about.
     */
    private suspend fun withTunnelledClient(
        extra: SftpConnectorBuilder.() -> Unit = {},
        block: suspend (SftpClient, LoopbackConnectProxy) -> Unit,
    ) {
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
            LoopbackConnectProxy.start().use { tunnel ->
                val config = configFor(server, tunnel, extra)
                endpoint = config.endpoint.address
                // The keepalive interval bounds the key exchange as well as the reads, and the
                // first key exchange in a JVM is far slower than every one after it. This
                // throwaway connection is what warms it - and it uses the shipped interval rather
                // than whatever a test shortened it to, because otherwise the warm-up is itself
                // the cold handshake it exists to absorb and fails with a rekeying timeout.
                JschTransport(configFor(server, tunnel, extra = {})).connect().close()
                pool = SftpPool(JschTransport(config, meters), config, meters)
                block(SftpClient(pool, config, meters), tunnel)
            }
        }
    }

    private fun configFor(
        server: EmbeddedSftpServer,
        tunnel: LoopbackConnectProxy,
        extra: SftpConnectorBuilder.() -> Unit,
    ): SftpConnectorConfig = sftpConnector("cancellation-demo") {
        endpoint {
            host = server.host
            port = server.port
            proxy { httpConnect(tunnel.host, tunnel.port) }
        }
        auth { password(USER, PASSWORD) }
        hostKey = HostKeyPolicy.AcceptAll
        pool { maxSize = 1 }
        polling { staging { dir = stage } }
        extra()
    }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"

        /** Big enough that the transfer is still going long after the hold below stops it. */
        private const val FILE_BYTES = 8 * 1024 * 1024
        private const val HOLD_AFTER_BYTES = 64L * 1024

        private const val ENTRIES = 500
        private const val WANTED = 3

        /** Big enough that the whole file cannot buffer before the blocked write is cut. */
        private const val BLACK_HOLE_FILE_BYTES = 64 * 1024 * 1024
        private const val BLACK_HOLE_AFTER_BYTES = 1L * 1024 * 1024

        /** More than the listing channel's buffer, so a stalled collector parks the hand-off. */
        private const val LISTING_FILES = 200

        private val KEEPALIVE = 400.milliseconds
        private val GRACE = 300.milliseconds

        /**
         * Long enough that the grace has had every chance, short enough that the default keepalive
         * - two intervals of thirty seconds - has had none. A test that passes on that instead of
         * on the cut would be proving the wrong thing.
         */
        private val BEFORE_THE_KEEPALIVE_WOULD = 15.seconds
    }
}
