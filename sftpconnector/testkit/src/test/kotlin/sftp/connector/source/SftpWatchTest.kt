package sftp.connector.source

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.client.SftpClient
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.OverlapPolicy
import sftp.connector.config.SftpConnectorBuilder
import sftp.connector.config.sftpConnector
import sftp.connector.error.Attempt
import sftp.connector.error.AuthenticationFailed
import sftp.connector.error.PermissionDenied
import sftp.connector.error.PoolExhausted
import sftp.connector.error.SessionLost
import sftp.connector.pool.SftpPool
import sftp.connector.pool.virtualClock
import sftp.connector.source.SftpEvent.FileSeen
import sftp.connector.source.SftpEvent.PollCompleted
import sftp.connector.source.SftpEvent.PollFailed
import sftp.connector.source.SftpEvent.PollSkipped
import sftp.connector.source.SftpEvent.PollStarted
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Operation
import java.nio.file.Path
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * A watch against a scripted server on virtual time: what its ticker does when the interval
 * comes round, what a failed tick turns into, and what ends a watch.
 *
 * The ticker runs in a scope of its own, as it does under a connector, on the same scheduler as
 * the test; nothing here waits, and every tick happens because the test moved the clock.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class SftpWatchTest {

    @TempDir
    lateinit var stage: Path

    private val registry = SimpleMeterRegistry()

    private lateinit var pool: SftpPool

    private lateinit var background: CoroutineScope

    @AfterEach
    fun stopTheTickers() {
        if (::background.isInitialized) background.cancel()
    }

    @Test
    fun `a watch polls when collected and again every interval, numbering its ticks after the source's polls`() = runTest {
        val transport = FakeSftpTransport().directory("/drop").file("/drop/a.csv", "1")
        val source = sourceOver(transport)
        val held = source.poll("/drop").toList().filterIsInstance<FileSeen>().single()

        val events = mutableListOf<SftpEvent>()
        val collector = launch { source.watch("/drop", EVERY).collect { events += it } }
        runCurrent()

        assertThat(events).containsExactly(PollStarted(2, "/drop"), PollCompleted(2, seen = 1, emitted = 0, notReady = 0))
        advanceTimeBy(EVERY)
        runCurrent()
        assertThat(events.filterIsInstance<PollStarted>().map { it.tick }).containsExactly(2L, 3L)
        assertThat(transport.calls.count { it.operation == Operation.List }).isEqualTo(3)

        collector.cancelAndJoin()
        held.ack()
    }

    /**
     * S8. The listing of the first tick never finishes; the interval comes round; under the
     * default policy the second tick is reported skipped and nothing is sent to the server for it.
     */
    @Test
    fun `S8_under SKIP a tick that comes round while the last is still running is skipped, and no second listing is sent`() = runTest {
        val transport = listingForever()
        val source = sourceOver(transport)
        val events = mutableListOf<SftpEvent>()
        val collector = launch { source.watch("/drop", EVERY).collect { events += it } }
        runCurrent()

        advanceTimeBy(EVERY * 2)
        runCurrent()

        assertThat(events).containsExactly(PollStarted(1, "/drop"), PollSkipped(2, SkipCause.OVERLAP), PollSkipped(3, SkipCause.OVERLAP))
        assertThat(transport.calls.count { it.operation == Operation.List }).describedAs("listings sent").isEqualTo(1)
        collector.cancelAndJoin()
    }

    @Test
    fun `under PROCEED a tick that comes round while the last is still running runs alongside it`() = runTest {
        val transport = listingForever()
        val source = sourceOver(transport) { polling { overlap = OverlapPolicy.PROCEED } }
        val events = mutableListOf<SftpEvent>()
        val collector = launch { source.watch("/drop", EVERY).collect { events += it } }
        runCurrent()

        advanceTimeBy(EVERY)
        runCurrent()

        assertThat(events).containsExactly(PollStarted(1, "/drop"), PollStarted(2, "/drop"))
        assertThat(transport.calls.count { it.operation == Operation.List }).describedAs("listings sent").isEqualTo(2)
        collector.cancelAndJoin()
    }

    /**
     * S12 under a real overlap. The first tick has handed over one file and is waiting for room
     * to hand over the next when the second tick lists alongside it; both see both files. The
     * held file is handed over once, and when room comes the waiting file is handed over once,
     * by whichever tick gets there.
     */
    @Test
    fun `S12_a file listed again by a tick running alongside is handed over once`() = runTest {
        val transport = FakeSftpTransport().directory("/drop").file("/drop/a.csv", "1").file("/drop/b.csv", "2")
        val source = sourceOver(transport) { polling { overlap = OverlapPolicy.PROCEED; maxInFlight = 1 } }
        val seen = mutableListOf<FileSeen>()
        val collector = launch { source.watch("/drop", EVERY).collect { if (it is FileSeen) seen += it } }
        runCurrent()

        advanceTimeBy(EVERY)
        runCurrent()
        assertThat(transport.calls.count { it.operation == Operation.List }).describedAs("listings alongside").isEqualTo(2)
        assertThat(seen.map { it.file.name }).containsExactly("a.csv")

        seen.single().ack()
        runCurrent()

        assertThat(seen.map { it.file.name }).containsExactly("a.csv", "b.csv")
        collector.cancelAndJoin()
    }

    /**
     * I10. A listing the server refused on permissions is reported and asked again on the next
     * tick, which is the whole of what "waits a full tick" means; a rejected password ends the
     * watch with the rejection, and no tick after it asks anything.
     */
    @Test
    fun `I10_a recoverable failure is reported and the watch goes on, and a fatal failure ends it with the error`() = runTest {
        val listings = mutableListOf<Throwable?>(
            PermissionDenied(Attempt(ENDPOINT, "list", "/drop"), "refused on permissions"),
            null,
            AuthenticationFailed(Attempt(ENDPOINT, "list", "/drop"), "the password was rejected"),
        )
        val transport = FakeSftpTransport { call -> if (call.operation == Operation.List) listings.removeFirst()?.let { throw it } }.directory("/drop")
        val source = sourceOver(transport)
        val events = mutableListOf<SftpEvent>()
        var ended: Throwable? = null
        val collector = launch { ended = runCatching { source.watch("/drop", EVERY).collect { events += it } }.exceptionOrNull() }

        runCurrent()
        advanceTimeBy(EVERY * 5)
        runCurrent()

        assertThat(events.filterIsInstance<PollFailed>().map { it.tick to it.error::class }).containsExactly(1L to PermissionDenied::class)
        assertThat(events.filterIsInstance<PollCompleted>().map { it.tick }).containsExactly(2L)
        assertThat(ended).isInstanceOf(AuthenticationFailed::class.java)
        assertThat(collector.isCompleted).isTrue()
        assertThat(transport.calls.count { it.operation == Operation.List }).describedAs("listings, none after the fatal one").isEqualTo(3)
    }

    /** S4. Somebody else holds the only session for longer than the acquire timeout; that tick fails and the next runs. */
    @Test
    fun `S4_a full pool fails the tick, and the watch continues`() = runTest {
        val source = sourceOver(FakeSftpTransport().directory("/drop")) { pool { maxSize = 1; acquireTimeout = 10.seconds } }
        val hog = launch { pool.withLease { awaitCancellation() } }
        runCurrent()
        val events = mutableListOf<SftpEvent>()
        val collector = launch { source.watch("/drop", EVERY).collect { events += it } }

        advanceTimeBy(10.seconds)
        runCurrent()
        assertThat(events.filterIsInstance<PollFailed>().map { it.tick to it.error::class }).containsExactly(1L to PoolExhausted::class)

        hog.cancelAndJoin()
        advanceTimeBy(EVERY)
        runCurrent()

        assertThat(events.filterIsInstance<PollCompleted>().map { it.tick }).containsExactly(2L)
        collector.cancelAndJoin()
    }

    /** One counted failure opens a breaker of one call; the tick after it is skipped rather than failed, and nothing is sent. */
    @Test
    fun `an open breaker skips the tick`() = runTest {
        var listings = 0
        val transport = FakeSftpTransport { call ->
            if (call.operation == Operation.List && ++listings == 1) throw SessionLost(Attempt(ENDPOINT, "list", "/drop"), "the tunnel went quiet")
        }.directory("/drop")
        val source = sourceOver(transport) { resilience { retry { maxAttempts = 1 }; circuitBreaker { slidingWindow = 1 } } }
        val events = mutableListOf<SftpEvent>()
        val collector = launch { source.watch("/drop", EVERY).collect { events += it } }

        runCurrent()
        advanceTimeBy(EVERY)
        runCurrent()

        assertThat(events.filterIsInstance<PollFailed>().map { it.tick to it.error::class }).containsExactly(1L to SessionLost::class)
        assertThat(events.filterIsInstance<PollSkipped>()).containsExactly(PollSkipped(2, SkipCause.BREAKER_OPEN))
        assertThat(listings).describedAs("listings sent while the breaker was open").isEqualTo(1)
        collector.cancelAndJoin()
    }

    @Test
    fun `a second watch of the same directory is refused until the first has ended`() = runTest {
        val source = sourceOver(FakeSftpTransport().directory("/drop"))
        val first = launch { source.watch("/drop", EVERY).collect {} }
        runCurrent()

        val refusal = runCatching { source.watch("/drop", EVERY).collect {} }.exceptionOrNull()
        assertThat(refusal).isInstanceOf(IllegalStateException::class.java).hasMessageContaining("/drop")

        first.cancelAndJoin()
        val again = launch { source.watch("/drop", EVERY).collect {} }
        runCurrent()
        assertThat(again.isActive).describedAs("a watch after the first ended").isTrue()
        again.cancelAndJoin()
    }

    /**
     * The block returning is the ack and the block throwing is the nack: the acked file is
     * deleted by its action and gone from the next tick, the nacked one is left and handed over
     * again, and the pipeline is still running when it is.
     */
    @Test
    fun `consume acks a file its block returns from, nacks one it throws on, and goes on`() = runTest {
        val transport = FakeSftpTransport().directory("/drop").file("/drop/good.csv", "1").file("/drop/bad.csv", "2")
        val source = sourceOver(transport) { polling { onAck = delete() } }
        val handled = mutableListOf<String>()
        val pipeline = launch {
            source.consume("/drop", EVERY) { seen ->
                handled += seen.file.name
                if (seen.file.name == "bad.csv") throw IllegalStateException("could not parse it")
            }
        }
        runCurrent()

        assertThat(handled).containsExactly("good.csv", "bad.csv")
        assertThat(transport.calls.filter { it.operation == Operation.Delete }.map { it.path }).containsExactly("/drop/good.csv")
        assertThat(registry.get("sftp_ack_total").tag("outcome", "nack").counter().count()).isEqualTo(1.0)
        assertThat(inFlight()).isZero()

        advanceTimeBy(EVERY)
        runCurrent()
        assertThat(handled).containsExactly("good.csv", "bad.csv", "bad.csv")
        assertThat(pipeline.isActive).isTrue()
        pipeline.cancelAndJoin()
    }

    /**
     * A check that times itself out and lets the timeout escape has thrown a cancellation at a
     * tick nobody cancelled. Under a poll that reaches the collector as it is; under a watch the
     * tick is a coroutine of its own, and a cancellation would end it without a word. So it ends
     * the watch, named as the bug it is, with the timeout as its cause.
     */
    @Test
    fun `a cancellation that is nobody's, let out of a check, ends the watch as a bug rather than silently`() = runTest {
        val source = sourceOver(FakeSftpTransport().directory("/drop").file("/drop/a.csv", "1")) {
            polling { readiness = ReadinessCheck { _, _ -> withTimeout(1.seconds) { awaitCancellation() } } }
        }

        var ended: Throwable? = null
        val collector = launch { ended = runCatching { source.watch("/drop", EVERY).collect {} }.exceptionOrNull() }
        advanceTimeBy(EVERY)
        runCurrent()

        assertThat(collector.isCompleted).isTrue()
        assertThat(ended).isInstanceOf(IllegalStateException::class.java).hasMessageContaining("times itself out")
        // Through the cause chain: the coroutine library's stack-trace recovery hands the
        // collector a copy of the exception with the original as its cause.
        assertThat(generateSequence(ended) { it.cause }.any { it is TimeoutCancellationException })
            .describedAs("the timeout is the cause").isTrue()
    }

    /** What consume exists to prevent, seen from watch: the collector's own exception ends the watch, and every place comes back. */
    @Test
    fun `a collector whose block throws ends the watch with its own exception and gives every place back`() = runTest {
        val source = sourceOver(FakeSftpTransport().directory("/drop").file("/drop/a.csv", "1").file("/drop/b.csv", "2"))

        val failure = runCatching {
            source.watch("/drop", EVERY).collect { if (it is FileSeen && it.file.name == "b.csv") throw IllegalStateException("bad row") }
        }.exceptionOrNull()
        runCurrent()

        assertThat(failure).isInstanceOf(IllegalStateException::class.java).hasMessage("bad row")
        assertThat(inFlight()).describedAs("places still taken after the failure").isZero()
        assertThat(source.poll("/drop").toList().filterIsInstance<FileSeen>()).describedAs("the directory is watchable again").hasSize(2)
    }

    private fun inFlight(): Int = registry.get("sftp_inflight").gauge().value().toInt()

    /** A server whose first listing never answers, so the first tick never ends. */
    private fun listingForever() = FakeSftpTransport { call -> if (call.operation == Operation.List) awaitCancellation() }.directory("/drop")

    /**
     * The ticker's scope is a supervisor on the test's scheduler, as the connector's is: a watch
     * ending on a fatal failure must not be a failure of the scope, and therefore of the test.
     */
    private fun TestScope.sourceOver(transport: FakeSftpTransport, extra: SftpConnectorBuilder.() -> Unit = {}): SftpSource {
        val config = sftpConnector("watch-demo") {
            endpoint { host = "fake.example"; port = 22 }
            auth { password("etl", "s3cret") }
            hostKey = HostKeyPolicy.AcceptAll
            polling {
                staging { dir = stage }
                directories("/drop")
                readiness = ReadinessCheck { _, _ -> Readiness.Ready }
            }
            resilience { retry { backoff = exponential(1.seconds, max = 1.seconds, jitter = false) } }
            extra()
        }
        pool = SftpPool(transport, config, registry, virtualClock())
        background = CoroutineScope(StandardTestDispatcher(testScheduler) + SupervisorJob())
        return SftpSource(SftpClient(pool, config, registry, virtualClock()), config, registry, virtualClock(), background)
    }

    private companion object {
        private const val ENDPOINT = "fake.example:22"
        private val EVERY = 1.minutes
    }
}
