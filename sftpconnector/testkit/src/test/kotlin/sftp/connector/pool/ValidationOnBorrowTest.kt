package sftp.connector.pool

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.Attempt
import sftp.connector.error.SessionLost
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Operation
import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * What the pool does with a session that has been sitting: it asks the server whether the session
 * is still there, but only when sitting long enough to have been dropped without either end
 * noticing.
 *
 * Opening a session costs a tunnel through the proxy, a key exchange, an authentication, a channel
 * open and a forked process on a server that starts refusing connections when too many are
 * half-open. One round trip is cheaper than all of it, which is why the pool asks rather than
 * replaces - and why it does not ask about a session it proved good a moment ago.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class ValidationOnBorrowTest {

    @Test
    fun `a session parked longer than the bypass window is proved before it is handed on`() = runTest {
        val transport = FakeSftpTransport()
        val pool = SftpPool(transport, config(), clock = virtualClock())

        pool.acquire().release()
        advanceTimeBy(1.seconds)
        val second = pool.acquire()

        assertThat(transport.calls.map { it.operation })
            .containsExactly(Operation.Connect, Operation.Realpath)
        assertThat(second.state.value).isEqualTo(EntryState.InUse)
    }

    /**
     * A session handed straight back to the next caller was proved good moments ago by the work
     * that just finished on it. Asking again would put a round trip in front of every borrow on a
     * busy pool, which is the cost this window exists to avoid.
     */
    @Test
    fun `a session parked inside the bypass window is handed straight on`() = runTest {
        val transport = FakeSftpTransport()
        val pool = SftpPool(transport, config(), clock = virtualClock())

        pool.acquire().release()
        advanceTimeBy(100.milliseconds)
        val second = pool.acquire()

        assertThat(transport.calls.map { it.operation }).containsExactly(Operation.Connect)
        assertThat(second.state.value).isEqualTo(EntryState.InUse)
    }

    /**
     * The caller asked for a session and gets one. That it is not the session the pool first
     * reached for is the pool's business, and the room the caller was admitted with stays with it
     * throughout - losing it here would shrink the pool by one every time the network dropped a
     * parked session, which is the moment a pool is needed most.
     */
    @Test
    fun `a session that cannot answer is replaced without the caller losing its place`() = runTest {
        var stillDead = true
        val transport = FakeSftpTransport { call ->
            if (call.operation == Operation.Realpath && stillDead) {
                stillDead = false
                throw SessionLost(ATTEMPT, "the tunnel was dropped while the session was parked")
            }
        }
        val meters = SimpleMeterRegistry()
        val pool = SftpPool(transport, config(maxSize = 1), meters, virtualClock())

        pool.acquire().release()
        advanceTimeBy(1.seconds)
        val replacement = pool.acquire()

        assertThat(transport.calls.map { it.operation })
            .containsExactly(Operation.Connect, Operation.Realpath, Operation.Close, Operation.Connect)
        assertThat(pool.stats()).isEqualTo(PoolStats(idle = 0, inUse = 1, connecting = 0))
        assertThat(transport.openSessions).describedAs("the dead one hung up on, the new one open").isEqualTo(1)
        assertThat(evictedFor(meters, "validation")).isEqualTo(1.0)

        // Admitted twice for two acquires, not three times for one that had to try again: the
        // caller kept the room it came in with rather than queueing for it a second time.
        assertThat(meters.find("sftp_pool_acquire_seconds").timer()?.count())
            .describedAs("times a caller was let through the door")
            .isEqualTo(2L)

        replacement.release()
        assertThat(pool.stats().idle).isEqualTo(1)
    }

    private fun evictedFor(meters: SimpleMeterRegistry, reason: String): Double? =
        meters.find("sftp_pool_evicted_total").tag("reason", reason).counter()?.count()

    private fun config(
        maxSize: Int = 2,
        validationBypass: Duration = 500.milliseconds,
    ): SftpConnectorConfig = sftpConnector("validation-test") {
        endpoint { host = "sftp.example" }
        auth { password("etl", "secret") }
        hostKey = HostKeyPolicy.Strict(Path.of("known_hosts"))
        pool {
            this.maxSize = maxSize
            this.validationBypass = validationBypass
        }
    }

    private companion object {
        private val ATTEMPT = Attempt("sftp.example:22", "realpath", ".")
    }
}
