package sftp.connector.pool

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeoutOrNull
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.Attempt
import sftp.connector.error.AuthenticationFailed
import sftp.connector.error.CircuitOpen
import sftp.connector.error.ConfigurationError
import sftp.connector.error.ConnectFailed
import sftp.connector.error.HostKeyRejected
import sftp.connector.error.LeaseFate
import sftp.connector.error.NoSuchFile
import sftp.connector.error.OperationTimeout
import sftp.connector.error.PermissionDenied
import sftp.connector.error.PoolExhausted
import sftp.connector.error.ServerFailure
import sftp.connector.error.SessionLost
import sftp.connector.error.SftpException
import sftp.connector.error.Unknown
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Call
import sftp.connector.testkit.FakeSftpTransport.Operation
import java.nio.file.Path
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * What a caller may expect of a lease: that waiting for one ends, that giving one back is exactly
 * once however the work ended, and that a session the pool has given up on is never seen again.
 *
 * Every wait here runs on the scheduler's virtual time, so an acquire timeout of thirty seconds
 * costs the suite nothing and the test still proves the caller really was made to wait for it.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class LeaseSemanticsTest {

    @Test
    fun `a caller that cannot be served is turned away rather than left queueing`() = runTest {
        val transport = FakeSftpTransport()
        val pool = SftpPool(transport, config(maxSize = 1))

        val holding = pool.acquire()
        val startedWaiting = testScheduler.currentTime
        val turnedAway = runCatching { pool.acquire() }.exceptionOrNull()

        assertThat(turnedAway).isInstanceOf(PoolExhausted::class.java)
        // The clock, not the exception's own account of itself: a bound that was never applied
        // would report the configured timeout just as happily as one that was.
        assertThat((testScheduler.currentTime - startedWaiting).milliseconds)
            .describedAs("how long the pool really made the caller wait")
            .isEqualTo(ACQUIRE_TIMEOUT)

        val exhausted = turnedAway as PoolExhausted
        assertThat(exhausted.waited).isEqualTo(ACQUIRE_TIMEOUT)
        assertThat(exhausted.stats).isEqualTo(PoolStats(idle = 0, inUse = 1, connecting = 0, pending = 1))
        assertThat(exhausted.disposition.lease).isEqualTo(LeaseFate.NONE_HELD)

        // The caller that was refused left nothing behind: no session, and no place in the queue.
        holding.release()
        assertThat(pool.stats()).isEqualTo(PoolStats(idle = 1, inUse = 0, connecting = 0, pending = 0))
    }

    /**
     * "The pool was full" is the class, not the message. Full because the server has stopped
     * completing handshakes, full because the work already holding the sessions is not finishing,
     * and full because there are not enough of them are three faults with three different
     * remedies, and the numbers separate them - so the failure says which one it is.
     */
    @Test
    fun `the exhaustion message names which of the three reasons the pool was full`() = runTest {
        assertThat(stuckDiallingPool()).contains("still opening", "look at the server and the network")

        assertThat(nothingHandedBackPool())
            .contains("1 in use", "0 still opening", "0 idle", "room came free 0 times")
            .contains("nothing came free at all, so the work already holding the sessions is not finishing")

        assertThat(oneSessionSharedByManyPool())
            .contains("room came free 1 times", "other callers took it")
    }

    /** Every session is being opened and none is open. Nothing about the pool's size is at fault. */
    private suspend fun TestScope.stuckDiallingPool(): String {
        val dialling = CompletableDeferred<Unit>()
        val pool = SftpPool(
            FakeSftpTransport { if (it.operation == Operation.Connect) dialling.await() },
            config(maxSize = 1),
        )
        val stuck = launch { pool.acquire() }
        runCurrent()
        val message = messageFromRefusing(pool)
        stuck.cancelAndJoin()
        dialling.complete(Unit)
        return message
    }

    /** One session, lent out, and its holder is not finished with it. */
    private suspend fun nothingHandedBackPool(): String {
        val pool = SftpPool(FakeSftpTransport(), config(maxSize = 1))
        pool.acquire()
        return messageFromRefusing(pool)
    }

    /**
     * One session that really does come free, and two callers wanting it. The one that loses the
     * race sees turnover, which is what tells it the pool is short rather than stalled.
     */
    private suspend fun TestScope.oneSessionSharedByManyPool(): String {
        val pool = SftpPool(FakeSftpTransport(), config(maxSize = 1))
        val held = pool.acquire()

        var message = ""
        val winner = launch { pool.acquire() }
        val loser = launch { message = messageFromRefusing(pool) }
        runCurrent()

        held.release()
        winner.join()
        loser.join()
        return message
    }

    private suspend fun messageFromRefusing(pool: SftpPool): String =
        checkNotNull(runCatching { pool.acquire() }.exceptionOrNull()?.message) {
            "the pool served a caller the test had arranged to refuse"
        }

    /**
     * The gap between a session opening and the pool recording it is the one place a cancelled
     * caller can leave a live socket that nothing owns: the entry has no connection to close, and
     * the connection has no entry to be closed from.
     */
    @Test
    fun `a session that opens into a cancelled caller is closed rather than left running`() = runTest {
        lateinit var givingUp: Job
        val transport = FakeSftpTransport { if (it.operation == Operation.Connect) givingUp.cancel() }
        val pool = SftpPool(transport, config(maxSize = 1))

        givingUp = launch { pool.acquire() }
        givingUp.join()

        assertThat(transport.openSessions).describedAs("sessions opened and never hung up on").isZero()
        assertThat(transport.calls.map { it.operation }).containsExactly(Operation.Connect, Operation.Close)
        assertThat(pool.stats().total).isZero()
        assertThat(withTimeoutOrNull(1.seconds) { pool.acquire() }).isNotNull()
    }

    /**
     * The deque is the only way back to a caller, so "never returns to it" is provable by asking
     * for the session again: an evicted one is gone for good and a kept one comes straight back.
     * Walking every failure class rather than a chosen few is what makes the loop a check on the
     * rule instead of on the examples someone thought of.
     */
    @Test
    fun `I3_a poisoned entry never returns to the idle deque`() = runTest {
        val pool = SftpPool(FakeSftpTransport(), config(maxSize = 1))

        // The loop below asks each failure what it thinks should happen and then checks that it
        // did. That is only a test of the rule if the failures between them ask for all three
        // things - otherwise it could be checking one branch twelve times.
        assertThat(EVERY_FAILURE.map { it.disposition.lease })
            .contains(LeaseFate.EVICTED, LeaseFate.RETURNED, LeaseFate.NONE_HELD)

        EVERY_FAILURE.forEach { failure ->
            val lease = pool.acquire()
            val borrowed = lease.connection
            val evicted = failure.disposition.lease == LeaseFate.EVICTED
            lease.releaseAfter(failure)

            assertThat(pool.stats().idle)
                .describedAs("sessions on the shelf after %s", failure::class.simpleName)
                .isEqualTo(if (evicted) 0 else 1)
            assertThat(lease.state.value)
                .describedAs("what became of the entry after %s", failure::class.simpleName)
                .isEqualTo(if (evicted) EntryState.Closed else EntryState.Idle)

            val next = pool.acquire()
            assertThat(next.connection === borrowed)
                .describedAs("%s was handed on again after %s", borrowed, failure::class.simpleName)
                .isEqualTo(!evicted)
            next.releaseAfter(SessionLost(ATTEMPT, "emptying the pool before the next case"))
        }

        // A throwable the connector never classified is nobody's word that the session is sound.
        val unvouched = pool.acquire()
        unvouched.releaseAfter(IllegalStateException("an application error the pool knows nothing about"))
        assertThat(pool.stats().idle).isZero()
    }

    /**
     * Counted by asking the pool afterwards what it can still lend, which is the only account that
     * matters: one permit lost on any path below and the pool never fills again, one invented and
     * the server sees more sessions than it was promised.
     */
    @Test
    fun `I4_every permit is released exactly once on every exit path`() = runTest {
        var onCall: suspend (Call) -> Unit = {}
        val transport = FakeSftpTransport { onCall(it) }
        val pool = SftpPool(transport, config(maxSize = 2))

        pool.withLease { }
        runCatching { pool.withLease { throw NoSuchFile(ATTEMPT, "the file was already gone") } }
        runCatching { pool.withLease { throw SessionLost(ATTEMPT, "the tunnel died") } }
        runCatching { pool.withLease { error("something the connector never classified") } }

        val byHand = pool.acquire()
        byHand.release()
        byHand.release()

        // The three paths below are the ones a caller takes when it has to open a session, so the
        // shelf has to be empty first: a pool with something idle hands it over without dialling,
        // and the test would then be exercising the same path three more times.
        runCatching { pool.withLease { throw SessionLost(ATTEMPT, "emptying the shelf") } }
        assertThat(pool.stats().total).describedAs("sessions left before the dialling paths").isZero()

        onCall = { if (it.operation == Operation.Connect) throw ConnectFailed(ATTEMPT, "the proxy refused") }
        runCatching { pool.acquire() }

        val dialling = CompletableDeferred<Unit>()
        onCall = { if (it.operation == Operation.Connect) dialling.await() }
        val duringConnect = launch { pool.acquire() }
        runCurrent()
        duringConnect.cancelAndJoin()
        dialling.complete(Unit)

        lateinit var afterConnect: Job
        onCall = { if (it.operation == Operation.Connect) afterConnect.cancel() }
        afterConnect = launch { pool.acquire() }
        afterConnect.join()
        onCall = {}

        val first = pool.acquire()
        val second = pool.acquire()
        assertThat(runCatching { pool.acquire() }.exceptionOrNull()).isInstanceOf(PoolExhausted::class.java)
        first.release()
        second.release()

        assertThat(runCatching { listOf(pool.acquire(), pool.acquire()) }.exceptionOrNull())
            .describedAs("the pool could not fill to its size, so a permit went missing on one of the paths above")
            .isNull()
        assertThat(runCatching { pool.acquire() }.exceptionOrNull())
            .describedAs("the pool lent past its size, so a permit was invented on one of the paths above")
            .isInstanceOf(PoolExhausted::class.java)
        assertThat(transport.openSessions)
            .describedAs("sessions open at the end, which is the two the pool is holding and nothing else")
            .isEqualTo(2)
    }

    @Test
    fun `the pool publishes what a dashboard needs to watch it fill up`() = runTest {
        val meters = SimpleMeterRegistry()
        val pool = SftpPool(FakeSftpTransport(), config(maxSize = 1), meters)

        val held = pool.acquire()
        assertThat(meters.read("sftp_pool_active")).isEqualTo(1.0)
        assertThat(meters.read("sftp_pool_idle")).isEqualTo(0.0)
        assertThat(meters.read("sftp_pool_created_total")).isEqualTo(1.0)

        val queueing = launch { runCatching { pool.acquire() } }
        runCurrent()
        assertThat(meters.read("sftp_pool_pending")).isEqualTo(1.0)
        queueing.join()

        assertThat(meters.read("sftp_pool_pending")).isEqualTo(0.0)
        assertThat(meters.read("sftp_pool_acquire_timeout_total")).isEqualTo(1.0)
        // The refused caller is not in the wait distribution; the one that got a session is.
        assertThat(meters.find("sftp_pool_acquire_seconds").timer()?.count()).isEqualTo(1L)

        held.release()
        assertThat(meters.read("sftp_pool_idle")).isEqualTo(1.0)
        assertThat(meters.read("sftp_pool_active")).isEqualTo(0.0)

        // Untagged, a service with two connectors would publish the sum of two pools, which is a
        // number describing neither of them.
        assertThat(meters.meters.map { it.id.name }).containsExactlyInAnyOrder(
            "sftp_pool_active",
            "sftp_pool_idle",
            "sftp_pool_pending",
            "sftp_pool_acquire_seconds",
            "sftp_pool_acquire_timeout_total",
            "sftp_pool_created_total",
        )
        assertThat(meters.meters.map { it.id.getTag("endpoint") }).containsOnly("sftp.example:22")
    }

    private fun SimpleMeterRegistry.read(name: String): Double =
        checkNotNull(find(name).meter()) { "$name was never registered" }
            .measure().first().value

    private fun config(maxSize: Int = 2): SftpConnectorConfig = sftpConnector("lease-test") {
        endpoint { host = "sftp.example" }
        auth { password("etl", "secret") }
        hostKey = HostKeyPolicy.Strict(Path.of("known_hosts"))
        pool {
            this.maxSize = maxSize
            acquireTimeout = ACQUIRE_TIMEOUT
        }
    }

    private companion object {
        private val ACQUIRE_TIMEOUT = 30.seconds
        private val ATTEMPT = Attempt("sftp.example:22", "list", "/inbox")

        /** One of every class, so the loop over them checks the rule and not a chosen few. */
        private val EVERY_FAILURE: List<SftpException> = listOf(
            ConnectFailed(ATTEMPT, "no session"),
            SessionLost(ATTEMPT, "the connection broke"),
            OperationTimeout(ATTEMPT, "took too long"),
            ServerFailure(ATTEMPT, statusCode = 4, detail = "the server refused"),
            Unknown(ATTEMPT, "a wording nobody has read"),
            PermissionDenied(ATTEMPT, "refused on permissions"),
            NoSuchFile(ATTEMPT, "no such path"),
            AuthenticationFailed(ATTEMPT, "wrong credential"),
            HostKeyRejected(ATTEMPT, "wrong key"),
            ConfigurationError("nothing was configured"),
            PoolExhausted(ATTEMPT),
            CircuitOpen(ATTEMPT),
        )
    }
}
