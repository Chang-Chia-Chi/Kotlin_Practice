package sftp.connector.pool

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Operation
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * The pool looking after itself: retiring sessions that have aged out, keeping the spares it was
 * told to keep, and saying so when a caller never gives one back.
 *
 * Every one of these is about time passing, and none of them waits. The pool's clock reads the
 * test scheduler, so half an hour of a session's life costs the suite nothing and still happens in
 * the order it would happen in production.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class HousekeeperTest {

    /**
     * The lifetime is the pool's promise that no session is used forever, and a session on its way
     * back is the only moment the promise can be kept without interrupting anybody. Left on the
     * shelf it would be handed to the next caller as healthy, and the promise would mean nothing.
     */
    @Test
    fun `I6_a session past its lifetime is closed when it comes back and never lent again`() = runTest {
        val transport = FakeSftpTransport()
        val pool = SftpPool(transport, config(maxSize = 1, maxLifetime = 10.minutes), clock = virtualClock())

        val lease = pool.acquire()
        val borrowed = lease.connection
        advanceTimeBy(10.minutes + 1.milliseconds)
        lease.release()

        assertThat(lease.state.value).isEqualTo(EntryState.Closed)
        assertThat(pool.stats().total).describedAs("what the pool kept of a session past its lifetime").isZero()
        assertThat(transport.openSessions).describedAs("sessions still open").isZero()

        // Nothing is left for the next caller to be handed, which is what "never reused" means to
        // the only party that could notice.
        assertThat(pool.acquire().connection).isNotSameAs(borrowed)
        assertThat(transport.calls.count { it.operation == Operation.Connect }).isEqualTo(2)
    }

    /**
     * A session nobody has wanted for minutes is a session the proxy is about to drop without
     * telling anyone. Letting go of it first turns a failure the next caller would have met into a
     * handshake the pool paid for while nothing was happening.
     */
    @Test
    fun `the housekeeper hangs up on a spare nobody has wanted since the idle timeout`() = runTest {
        val transport = FakeSftpTransport()
        val pool = SftpPool(transport, config(maxSize = 2, idleTimeout = 1.minutes), clock = virtualClock())
        pool.acquire().release()

        val keeper = launch { pool.housekeep() }

        advanceTimeBy(30.seconds)
        runCurrent()
        assertThat(pool.stats().idle).describedAs("a spare inside its idle timeout").isEqualTo(1)

        advanceTimeBy(1.minutes)
        runCurrent()
        assertThat(pool.stats().idle).describedAs("a spare past its idle timeout").isZero()
        assertThat(transport.openSessions).isZero()

        keeper.cancelAndJoin()
    }

    /**
     * The idle timeout and the minimum pull in opposite directions, and the minimum wins. A pool
     * told to keep a session ready that let the idle timeout take the last one would make the
     * caller after the quiet spell pay for a handshake, which is the cost the minimum exists to
     * remove.
     */
    @Test
    fun `the spares the pool was told to keep survive the idle timeout`() = runTest {
        val transport = FakeSftpTransport()
        val pool = SftpPool(
            transport,
            config(maxSize = 3, minIdle = 1, idleTimeout = 1.minutes),
            clock = virtualClock(),
        )
        val first = pool.acquire()
        val second = pool.acquire()
        first.release()
        second.release()

        val keeper = launch { pool.housekeep() }
        advanceTimeBy(2.minutes)
        runCurrent()

        assertThat(pool.stats().idle).describedAs("spares left once the timeout had taken what it may").isEqualTo(1)
        assertThat(transport.openSessions).isEqualTo(1)

        // And it stays: the survivor is not taken on the next round either, however long it sits.
        advanceTimeBy(5.minutes)
        runCurrent()
        assertThat(pool.stats().idle).isEqualTo(1)

        keeper.cancelAndJoin()
    }

    @Test
    fun `the housekeeper opens sessions until the pool holds the spares it was told to keep`() = runTest {
        val transport = FakeSftpTransport()
        val pool = SftpPool(transport, config(maxSize = 3, minIdle = 2), clock = virtualClock())

        val keeper = launch { pool.housekeep() }
        advanceTimeBy(31.seconds)
        runCurrent()

        assertThat(pool.stats()).isEqualTo(PoolStats(idle = 2, inUse = 0, connecting = 0))
        assertThat(transport.calls.count { it.operation == Operation.Connect }).isEqualTo(2)

        // Having reached the number, it stops. A housekeeper that dialled every round would burn a
        // handshake every thirty seconds on a pool with nothing to do.
        advanceTimeBy(2.minutes)
        runCurrent()
        assertThat(transport.calls.count { it.operation == Operation.Connect })
            .describedAs("handshakes paid for on a pool that already had its spares")
            .isEqualTo(2)

        keeper.cancelAndJoin()
    }

    /**
     * The spares the housekeeper opens are held by nobody, so nothing a caller does can bound
     * them. The infrastructure team allowed a fixed number of sessions to this server and the
     * pool's own maintenance is not exempt from it.
     */
    @Test
    fun `the housekeeper never opens a session the pool has no room for`() = runTest {
        val transport = FakeSftpTransport()
        val pool = SftpPool(transport, config(maxSize = 2, minIdle = 2), clock = virtualClock())

        val held = pool.acquire()
        val keeper = launch { pool.housekeep() }
        advanceTimeBy(2.minutes)
        runCurrent()

        assertThat(pool.stats().total)
            .describedAs("sessions the pool holds while it is short of spares and out of room")
            .isEqualTo(2)
        assertThat(pool.stats().idle).isEqualTo(1)
        assertThat(transport.openSessions).isEqualTo(2)

        held.release()
        keeper.cancelAndJoin()
    }

    /**
     * A pool that filled in one burst at startup would, without this, retire everything it holds
     * in another burst half an hour later, and every caller in that moment pays for a handshake at
     * once. The bounds are what the jitter promises: never early, and never beyond the window.
     */
    @Test
    fun `lifetime jitter never retires a session early and never keeps one past the window`() = runTest {
        val pool = SftpPool(
            FakeSftpTransport(),
            config(maxSize = 2, maxLifetime = 10.minutes, jitter = 1.0),
            clock = virtualClock(),
        )

        // Two sessions, so two draws from the jitter are put under both bounds rather than one.
        val leases = listOf(pool.acquire(), pool.acquire())
        advanceTimeBy(10.minutes - 1.milliseconds)
        leases.forEach { it.release() }
        assertThat(pool.stats().idle)
            .describedAs("sessions kept after a moment short of the configured lifetime")
            .isEqualTo(2)

        val again = listOf(pool.acquire(), pool.acquire())
        advanceTimeBy(10.minutes + 1.milliseconds)
        again.forEach { it.release() }
        assertThat(pool.stats().idle)
            .describedAs("sessions kept past twice the configured lifetime, which no jitter allows")
            .isZero()
    }

    /**
     * Leak detection is a report and not a repair. A JSch call in flight cannot be interrupted
     * from outside without destroying the session under whoever is using it, so taking the lease
     * back would turn a caller that is merely slow into one that fails. What it can do is name the
     * line that took the session, which is the one thing nobody can work out from the pool's
     * numbers afterwards.
     */
    @Test
    fun `a lease held past the threshold is reported once, with the stack that took it, and is not taken back`() =
        runTest {
            val meters = SimpleMeterRegistry()
            val pool = SftpPool(
                FakeSftpTransport(),
                config(maxSize = 1, leakDetectionThreshold = 5.minutes),
                meters,
                virtualClock(),
            )

            val lease = pool.acquire()
            val keeper = launch { pool.housekeep() }
            val reported = capturingStandardError {
                advanceTimeBy(6.minutes)
                runCurrent()
                advanceTimeBy(6.minutes)
                runCurrent()
            }

            assertThat(reported.split(HELD_TOO_LONG).size - 1)
                .describedAs("times one lease was reported over eleven rounds of housekeeping")
                .isEqualTo(1)
            assertThat(reported).contains("session #1").contains("HousekeeperTest")
            // A host running two connectors cannot otherwise tell whose session #1 this is.
            assertTrue(reported.contains("session #1 to sftp.example:22"), "which server the session is to: $reported")
            assertThat(meters.find("sftp_pool_leak_total").counter()?.count()).isEqualTo(1.0)

            // Never forced: the caller still holds a working session and gives it back itself.
            assertThat(lease.state.value).isEqualTo(EntryState.InUse)
            assertThat(lease.connection.realpath("/inbox")).isEqualTo("/inbox")
            lease.release()
            assertThat(pool.stats().idle).isEqualTo(1)

            keeper.cancelAndJoin()
        }

    /**
     * Lens 1 M2. A round retires a session and, being short of its minimum, opens a spare in the
     * same pass. The retired session holds no pool place, so its hang-up queues for an IO thread;
     * the spare it reserved must not sit registered as `Connecting` while that hang-up waits, or a
     * caller refused meanwhile reads "stuck opening sessions" for a pool that is stuck hanging up.
     * So the spare is dialled first and parked, and only then is the retired session hung up on -
     * and while that hang-up is held, nothing is `Connecting`.
     */
    @Test
    fun `a round whose hang-up waits does not hold room it has not dialled`() = runTest {
        val hangUpReached = CompletableDeferred<Unit>()
        val letHangUpFinish = CompletableDeferred<Unit>()
        val transport = FakeSftpTransport {
            if (it.operation == Operation.Close) {
                hangUpReached.complete(Unit)
                letHangUpFinish.await()
            }
        }
        val pool = SftpPool(transport, config(maxSize = 2, minIdle = 1, maxLifetime = 10.minutes), clock = virtualClock())

        // One idle session, aged past its lifetime, so the next round retires it and - now short of
        // the one spare it keeps - opens a replacement in the same round.
        pool.acquire().release()
        advanceTimeBy(10.minutes + 1.milliseconds)

        val keeper = launch { pool.housekeep() }
        advanceTimeBy(31.seconds)
        runCurrent()

        assertThat(hangUpReached.isCompleted)
            .describedAs("the round has dialled its spare and is now hanging up on the retired session")
            .isTrue()
        assertThat(pool.stats().connecting)
            .describedAs("a spare reserved but not dialled, held while the hang-up waits")
            .isZero()
        assertThat(pool.stats().idle).describedAs("the spare the round opened, already parked").isEqualTo(1)

        letHangUpFinish.complete(Unit)
        keeper.cancelAndJoin()
    }

    /**
     * The warning is the deliverable, so the test reads what an operator would read. The test
     * binding writes to standard error and looks it up on every call, so swapping the stream is
     * enough to capture it.
     */
    private fun capturingStandardError(body: () -> Unit): String {
        val captured = ByteArrayOutputStream()
        val original = System.err
        System.setErr(PrintStream(captured, true))
        try {
            body()
        } finally {
            System.setErr(original)
        }
        return captured.toString()
    }

    private fun config(
        maxSize: Int = 2,
        minIdle: Int = 0,
        maxLifetime: Duration = 30.minutes,
        jitter: Double = 0.0,
        idleTimeout: Duration = 4.minutes,
        leakDetectionThreshold: Duration = 10.minutes,
    ): SftpConnectorConfig = sftpConnector("housekeeper-test") {
        endpoint { host = "sftp.example" }
        auth { password("etl", "secret") }
        hostKey = HostKeyPolicy.Strict(Path.of("known_hosts"))
        pool {
            this.maxSize = maxSize
            this.minIdle = minIdle
            this.maxLifetime = maxLifetime
            this.maxLifetimeJitter = jitter
            this.idleTimeout = idleTimeout
            this.leakDetectionThreshold = leakDetectionThreshold
        }
    }

    private companion object {
        /** The words the leak report is counted by, so a second report cannot hide in the log. */
        private const val HELD_TOO_LONG = "has been held longer than"
    }
}
