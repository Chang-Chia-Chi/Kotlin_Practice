package sftp.connector.pool

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.Attempt
import sftp.connector.error.NoSuchFile
import sftp.connector.error.ServerFailure
import sftp.connector.error.SessionLost
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Operation
import sftp.connector.transport.SftpConnection
import java.nio.file.Path
import kotlin.time.Duration.Companion.seconds

/**
 * The pool against the scripted transport: no socket, no server, no waiting on a wall clock.
 *
 * Everything that would otherwise need timing is arranged instead. A slow connect is a hook that
 * does not return until the test says so, and the one place a test has to prove that something
 * does *not* happen runs on the scheduler's virtual time, where a second passes for free.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class SftpPoolTest {

    @Test
    fun `the first caller opens a session and the next one gets it back`() = runTest {
        val transport = FakeSftpTransport()
        val pool = SftpPool(transport, config(maxSize = 3))

        pool.withLease { assertThat(it.connection).isNotNull() }
        pool.withLease { }

        assertThat(transport.calls.count { it.operation == Operation.Connect }).isEqualTo(1)
        assertThat(pool.stats()).isEqualTo(PoolStats(idle = 1, inUse = 0, connecting = 0))
    }

    /**
     * A session nobody could open is not a session the pool should go on counting. Left behind, it
     * would take up room for the life of the process and the pool would slowly starve.
     */
    @Test
    fun `a session that never opened is not left occupying the pool`() = runTest {
        val transport = FakeSftpTransport { throw SessionLost(ATTEMPT, "the handshake never finished") }
        val pool = SftpPool(transport, config(maxSize = 1))

        assertThat(runCatching { pool.acquire() }.exceptionOrNull()).isInstanceOf(SessionLost::class.java)
        assertThat(pool.stats().total).isZero()

        // The room it was taking up came back with it: the next caller reaches the server rather
        // than queueing behind capacity that nothing will ever return.
        assertThat(runCatching { pool.acquire() }.exceptionOrNull()).isInstanceOf(SessionLost::class.java)
        assertThat(transport.calls.count { it.operation == Operation.Connect }).isEqualTo(2)
    }

    /**
     * The second handback is a bug in the caller - it is using a session that now belongs to
     * someone else - but the damage the pool must not add is capacity it does not have.
     */
    @Test
    fun `a lease given back twice is ignored the second time`() = runTest {
        val pool = SftpPool(FakeSftpTransport(), config(maxSize = 1))

        val lease = pool.acquire()
        lease.release()
        lease.release()

        assertThat(pool.stats().idle).isEqualTo(1)
        pool.acquire()
        assertThat(withTimeoutOrNull(1.seconds) { pool.acquire() }).isNull()
    }

    /**
     * The four things a failure decides include what becomes of the session, so the pool reads
     * that answer instead of asking its caller for one. A refusal the server spelled out is proof
     * the channel is working; a lost connection is proof it is not.
     */
    @Test
    fun `what a failure says about the session is what happens to it`() = runTest {
        val transport = FakeSftpTransport()
        val pool = SftpPool(transport, config())

        runCatching { pool.withLease { throw NoSuchFile(ATTEMPT, "the file was already gone") } }
        assertThat(pool.stats().idle).isEqualTo(1)

        runCatching { pool.withLease { throw ServerFailure(ATTEMPT, 4, "this server has no posix-rename") } }
        assertThat(pool.stats().idle).isEqualTo(1)
        assertThat(transport.calls.count { it.operation == Operation.Connect }).isEqualTo(1)

        runCatching { pool.withLease { throw SessionLost(ATTEMPT, "the tunnel died") } }
        assertThat(pool.stats().total).isZero()
        assertThat(transport.openSessions).isZero()
    }

    @Test
    fun `an entry publishes the states it passes through`() = runTest {
        val pool = SftpPool(FakeSftpTransport(), config())

        val kept = pool.acquire()
        assertThat(kept.state.value).isEqualTo(EntryState.InUse)
        kept.release()
        assertThat(kept.state.value).isEqualTo(EntryState.Idle)

        val doomed = pool.acquire()
        doomed.releaseAfter(SessionLost(ATTEMPT, "the tunnel died"))
        assertThat(doomed.state.value).isEqualTo(EntryState.Closed)
    }

    /**
     * Cancellation lands in the middle of the one stretch where the pool is holding capacity for a
     * session that does not exist yet. Leaving the permit behind would shrink the pool by one for
     * the life of the process, and leaving the entry behind would make it look full.
     */
    @Test
    fun `a connect cancelled halfway leaves the pool all of its capacity`() = runTest {
        val dialling = CompletableDeferred<Unit>()
        val transport = FakeSftpTransport { if (it.operation == Operation.Connect) dialling.await() }
        val pool = SftpPool(transport, config(maxSize = 1))

        val givingUp = launch { pool.acquire() }
        runCurrent()
        assertThat(pool.stats().connecting).isEqualTo(1)
        givingUp.cancelAndJoin()

        assertThat(pool.stats().total).isZero()
        dialling.complete(Unit)
        assertThat(withTimeoutOrNull(1.seconds) { pool.acquire() }).isNotNull()
    }

    @Test
    fun `I1_idle plus inUse plus connecting never exceeds maxSize`() = runTest {
        lateinit var pool: SftpPool
        val transport = FakeSftpTransport { call ->
            assertThat(pool.stats().total)
                .describedAs("sessions accounted for during %s", call)
                .isLessThanOrEqualTo(MAX_SIZE)
        }
        pool = SftpPool(transport, config(maxSize = MAX_SIZE))

        val callers = (1..12).map {
            launch {
                pool.withLease {
                    assertThat(pool.stats().total).isLessThanOrEqualTo(MAX_SIZE)
                    yield()
                }
            }
        }
        callers.joinAll()

        assertThat(pool.stats().total).isLessThanOrEqualTo(MAX_SIZE)
        assertThat(transport.openSessions).isLessThanOrEqualTo(MAX_SIZE)
    }

    @Test
    fun `I2_an entry is handed to at most one lease at a time`() = runTest {
        val transport = FakeSftpTransport()
        val pool = SftpPool(transport, config(maxSize = MAX_SIZE))
        val lentOut = mutableSetOf<SftpConnection>()

        val callers = (1..12).map {
            launch {
                pool.withLease { lease ->
                    assertThat(lentOut.add(lease.connection))
                        .describedAs("%s was already lent to someone else", lease.connection)
                        .isTrue()
                    yield()
                    lentOut.remove(lease.connection)
                }
            }
        }
        callers.joinAll()

        assertThat(lentOut).isEmpty()
        assertThat(transport.calls.count { it.operation == Operation.Connect }).isLessThanOrEqualTo(MAX_SIZE)
    }

    /**
     * Asking the pool for its statistics needs the same lock the pool decides under, and a mutex
     * is not reentrant. So a transport call made from inside that lock could not get an answer
     * here at all: the wait would be the deadlock the rule exists to prevent, and the timeout
     * turns it into a failed test instead of a hung one.
     */
    @Test
    fun `I5_no transport call executes while the registry lock is held`() = runTest {
        lateinit var pool: SftpPool
        val transport = FakeSftpTransport { call ->
            assertThat(withTimeoutOrNull(1.seconds) { pool.stats() })
                .describedAs("%s ran while the pool was locked", call)
                .isNotNull()
        }
        pool = SftpPool(transport, config())

        val lease = pool.acquire()
        lease.connection.realpath("/inbox")
        lease.releaseAfter(SessionLost(ATTEMPT, "the tunnel died"))

        assertThat(transport.calls.map { it.operation })
            .containsExactly(Operation.Connect, Operation.Realpath, Operation.Close)
    }

    private fun config(maxSize: Int = 2): SftpConnectorConfig = sftpConnector("pool-test") {
        endpoint { host = "sftp.example" }
        auth { password("etl", "secret") }
        hostKey = HostKeyPolicy.Strict(Path.of("known_hosts"))
        pool { this.maxSize = maxSize }
    }

    private companion object {
        private const val MAX_SIZE = 3
        private val ATTEMPT = Attempt("sftp.example:22", "list", "/inbox")
    }
}
