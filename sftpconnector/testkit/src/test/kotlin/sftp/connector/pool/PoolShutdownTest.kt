package sftp.connector.pool

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.Attempt
import sftp.connector.error.PoolExhausted
import sftp.connector.error.SessionLost
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Operation
import java.io.ByteArrayOutputStream
import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * What closing the pool does to the sessions it holds, on virtual time: the ones on the shelf,
 * the ones that come back during the drain, the one that never does, and the one whose
 * handshake had not finished. The fake's hooks play the socket - a read that never answers is a
 * call blocked inside the SSH library, and a cut that the test then lets go of is a thread
 * getting back from a closed socket.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class PoolShutdownTest {

    private val meters = SimpleMeterRegistry()

    /**
     * Three sessions: parked, out and handed back a second in, and out on a call the server never
     * answers. Closing waits the drain for the third, cuts it, waits one grace for the cut to hand
     * back - which this one never does, because the fake's read stays blocked - and returns at
     * exactly the bound, with the entry written off and hung up on.
     */
    @Test
    fun `I9_close returns within the drain plus one grace and leaves every entry closed`() = runTest {
        val neverAnswered = CompletableDeferred<Unit>()
        val transport = FakeSftpTransport { if (it.operation == Operation.Read) neverAnswered.await() }
        val pool = SftpPool(transport, config(maxSize = 3), meters, virtualClock())
        val entries = mutableListOf<StateFlow<EntryState>>()

        // All three taken before one is parked, so that three entries exist rather than one reused.
        val parked = pool.acquire()
        val handedBackDuringTheDrain = launch { pool.withLease { entries += it.state; delay(1.seconds) } }
        val blocked = async {
            runCatching { pool.withLease { entries += it.state; it.connection.readTo("/f", ByteArrayOutputStream()) } }
        }
        runCurrent()
        parked.release()
        entries += parked.state

        val closer = launch { pool.close() }
        advanceTimeBy(1.seconds + 1.milliseconds)
        runCurrent()
        assertThat(handedBackDuringTheDrain.isCompleted).isTrue()
        assertThat(closer.isCompleted).describedAs("returned while a session was still out").isFalse()

        advanceTimeBy(DRAIN - 1.seconds)
        runCurrent()
        assertThat(transport.calls.filter { it.operation == Operation.Abort })
            .describedAs("the session still out at the end of the drain is cut")
            .hasSize(1)
        assertThat(closer.isCompleted).describedAs("returned before the cut had its grace").isFalse()

        advanceTimeBy(GRACE)
        runCurrent()
        assertThat(closer.isCompleted).describedAs("close within drainTimeout + cancelGrace").isTrue()
        assertThat(entries.map { it.value }).containsExactly(EntryState.Closed, EntryState.Closed, EntryState.Closed)
        assertThat(pool.stats().total).isZero()
        assertThat(transport.openSessions).describedAs("sessions left open").isZero()
        assertThat(evictedAsShutdown()).isEqualTo(3.0)

        // The thread comes back from its closed socket long after the pool stopped waiting, and
        // hands back an entry the pool has already finished with. Nothing is counted twice.
        neverAnswered.complete(Unit)
        runCurrent()
        assertThat(blocked.await().isFailure).isTrue()
        assertThat(evictedAsShutdown()).isEqualTo(3.0)
        assertThat(pool.stats().total).isZero()
    }

    /**
     * Nobody queues for a session that is never coming. A caller arriving after the pool started
     * closing is refused at the door; one that was already queued when it started is refused the
     * moment room comes free, rather than when its acquire timeout would have run out.
     */
    @Test
    fun `during closing an acquire fails at once with PoolExhausted closing`() = runTest {
        val pool = SftpPool(FakeSftpTransport(), config(maxSize = 1), meters, virtualClock())
        val held = pool.acquire()
        val queuedBefore = async { runCatching { pool.acquire() } }
        runCurrent()

        val closer = launch { pool.close() }
        runCurrent()
        val refusedAt = currentTime
        val refused = runCatching { pool.acquire() }.exceptionOrNull()

        assertThat(currentTime).describedAs("time spent waiting at the door").isEqualTo(refusedAt)
        assertThat(refused).isInstanceOfSatisfying(PoolExhausted::class.java) { assertThat(it.closing).isTrue() }
        assertThat(refused).hasMessageContaining("closing")

        advanceTimeBy(1.seconds)
        held.release()
        runCurrent()
        val queuedRefusal = queuedBefore.await().exceptionOrNull()
        assertThat(queuedRefusal).isInstanceOfSatisfying(PoolExhausted::class.java) { assertThat(it.closing).isTrue() }
        assertThat(currentTime).describedAs("the queued caller learned when room came free, not at its timeout").isEqualTo(1.seconds.inWholeMilliseconds)
        assertThat(closer.isCompleted).isTrue()
    }

    /**
     * The drain is a bound, not a wait: the pool closes as soon as the last session is back. And
     * whatever a session comes back with - nothing, or a failure that would have poisoned it - it
     * leaves as `shutdown`, because that is why it is leaving: the pool was keeping nothing.
     */
    @Test
    fun `a session handed back during the drain is retired as shutdown, and close returns once the last is back`() = runTest {
        val transport = FakeSftpTransport()
        val pool = SftpPool(transport, config(maxSize = 2), meters, virtualClock())
        launch { pool.withLease { delay(1.seconds) } }
        launch { pool.acquire().also { delay(2.seconds) }.releaseAfter(SessionLost(ATTEMPT, "the tunnel dropped")) }
        runCurrent()

        val closer = launch { pool.close() }
        advanceTimeBy(2.seconds + 100.milliseconds)
        runCurrent()

        assertThat(closer.isCompleted).describedAs("close returned when the last session came back").isTrue()
        assertThat(transport.calls.map { it.operation }).doesNotContain(Operation.Abort)
        assertThat(transport.openSessions).isZero()
        assertThat(evictedAsShutdown()).isEqualTo(2.0)
        assertThat(meters.counter("sftp_pool_evicted_total", "endpoint", ENDPOINT, "reason", "poisoned").count()).isZero()

        pool.close()
        assertThat(evictedAsShutdown()).describedAs("a second close finds nothing").isEqualTo(2.0)
    }

    /**
     * A handshake in progress when the pool starts closing lands into a pool that lends nothing:
     * the caller is refused and the session it was for is hung up on - whether the dial lands
     * during the drain or after the pool has already written its entry off.
     */
    @Test
    fun `a dial that lands into a closing pool is hung up on, and its caller refused`() = runTest {
        val landing = CompletableDeferred<Unit>()
        val transport = FakeSftpTransport { if (it.operation == Operation.Connect) landing.await() }
        val pool = SftpPool(transport, config(maxSize = 1), meters, virtualClock())
        val caller = async { runCatching { pool.acquire() } }
        runCurrent()

        val closer = launch { pool.close() }
        runCurrent()
        landing.complete(Unit)
        advanceTimeBy(100.milliseconds)
        runCurrent()

        assertThat(caller.await().exceptionOrNull()).isInstanceOfSatisfying(PoolExhausted::class.java) { assertThat(it.closing).isTrue() }
        assertThat(transport.openSessions).isZero()
        assertThat(evictedAsShutdown()).isEqualTo(1.0)
        assertThat(closer.isCompleted).describedAs("close returned once the landed session was hung up on").isTrue()
    }

    @Test
    fun `a dial that lands after the pool gave up waiting for it is hung up on all the same`() = runTest {
        val landing = CompletableDeferred<Unit>()
        val transport = FakeSftpTransport { if (it.operation == Operation.Connect) landing.await() }
        val pool = SftpPool(transport, config(maxSize = 1), meters, virtualClock())
        val caller = async { runCatching { pool.acquire() } }
        runCurrent()

        val closer = launch { pool.close() }
        advanceTimeBy(DRAIN + GRACE)
        runCurrent()
        assertThat(closer.isCompleted).describedAs("close waited no longer than the bound for the handshake").isTrue()

        landing.complete(Unit)
        runCurrent()

        assertThat(caller.await().exceptionOrNull()).isInstanceOfSatisfying(PoolExhausted::class.java) { assertThat(it.closing).isTrue() }
        assertThat(transport.openSessions).describedAs("the session nobody was left to receive").isZero()
        assertThat(evictedAsShutdown()).isEqualTo(1.0)
    }

    /** A closing pool is not topped up: spares opened for a shelf about to be cleared are handshakes for nothing. */
    @Test
    fun `a closing pool is not kept in shape`() = runTest {
        val transport = FakeSftpTransport()
        val pool = SftpPool(transport, config(maxSize = 3, minIdle = 2), meters, virtualClock())
        val keeper = launch { pool.housekeep() }

        pool.close()
        advanceTimeBy(HOUSEKEEPING * 3)
        runCurrent()

        assertThat(transport.calls.filter { it.operation == Operation.Connect }).isEmpty()
        keeper.cancel()
    }

    private fun evictedAsShutdown(): Double =
        meters.counter("sftp_pool_evicted_total", "endpoint", ENDPOINT, "reason", "shutdown").count()

    private fun config(maxSize: Int, minIdle: Int = 0): SftpConnectorConfig = sftpConnector("shutdown") {
        endpoint { host = "sftp.example" }
        auth { password("etl", "secret") }
        hostKey = HostKeyPolicy.Strict(Path.of("known_hosts"))
        pool {
            this.maxSize = maxSize
            this.minIdle = minIdle
            drainTimeout = DRAIN
            cancelGrace = GRACE
            housekeepingInterval = HOUSEKEEPING
        }
    }

    private companion object {
        private const val ENDPOINT = "sftp.example:22"
        private val ATTEMPT = Attempt(ENDPOINT, "read", "/f")
        private val DRAIN: Duration = 5.seconds
        private val GRACE: Duration = 1.seconds
        private val HOUSEKEEPING: Duration = 30.seconds
    }
}
