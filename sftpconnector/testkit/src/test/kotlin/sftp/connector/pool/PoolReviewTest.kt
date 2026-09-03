package sftp.connector.pool

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Operation
import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Findings of the review of T3-T5: what a green suite did not prove about the pool.
 *
 * Both tests here are about the housekeeper being cancelled - which is what a shutdown does to it
 * - in the gap between deciding a round under the lock and carrying it out. Everything the round
 * decided is already off the shelf or already holding room by then, so a round that stops halfway
 * leaves the pool holding things nothing will ever finish.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class PoolReviewTest {

    /**
     * The spares a round decides to open take their room under the lock, before any of them is
     * dialled. Cancelled while dialling the first, the round used to hand that one back and leave
     * the rest registered as `Connecting` with their room still taken - capacity the pool has lost
     * for the life of the process, and invisible to leak detection, which only watches sessions a
     * caller holds.
     */
    @Test
    fun `I4_a housekeeper cancelled while opening spares gives back every room the round reserved`() = runTest {
        val dialling = CompletableDeferred<Unit>()
        val transport = FakeSftpTransport { if (it.operation == Operation.Connect) dialling.await() }
        val pool = SftpPool(transport, config(maxSize = 3, minIdle = 2), clock = virtualClock())

        val keeper = launch { pool.housekeep() }
        advanceTimeBy(31.seconds)
        runCurrent()
        assertThat(pool.stats().connecting).describedAs("spares the round decided to open").isEqualTo(2)

        keeper.cancel()
        keeper.join()
        dialling.complete(Unit)

        assertThat(pool.stats().total).describedAs("entries the cancelled round left behind").isZero()
        // The room those entries reserved is back, which is only provable by filling the pool.
        val everything = (1..3).map { pool.acquire() }
        assertThat(everything).hasSize(3)
        assertThat(transport.openSessions).isEqualTo(3)
    }

    /**
     * A retired session leaves the registry and the idle deque under the lock, and from then on
     * the only reference to its connection is the round's own list of things to hang up on.
     * Cancelled while hanging up on the first, the round used to drop the rest of that list, and
     * with it the only handle on sockets and reader threads the process would keep until it died.
     */
    @Test
    fun `a housekeeper cancelled while hanging up on one retired session still hangs up on the rest`() = runTest {
        val hangingUp = CompletableDeferred<Unit>()
        var firstClose = true
        val transport = FakeSftpTransport {
            if (it.operation == Operation.Close && firstClose) {
                firstClose = false
                hangingUp.await()
            }
        }
        val pool = SftpPool(transport, config(maxSize = 2, idleTimeout = 1.minutes), clock = virtualClock())
        val first = pool.acquire()
        val second = pool.acquire()
        first.release()
        second.release()

        val keeper = launch { pool.housekeep() }
        advanceTimeBy(1.minutes + 30.seconds)
        runCurrent()
        assertThat(pool.stats().idle).describedAs("spares the round decided to retire").isZero()

        keeper.cancel()
        hangingUp.complete(Unit)
        keeper.join()

        assertThat(transport.openSessions).describedAs("retired sessions never hung up on").isZero()
        assertThat(pool.stats().total).isZero()
    }

    private fun config(
        maxSize: Int,
        minIdle: Int = 0,
        idleTimeout: Duration = 4.minutes,
    ): SftpConnectorConfig = sftpConnector("pool-review") {
        endpoint { host = "sftp.example" }
        auth { password("etl", "secret") }
        hostKey = HostKeyPolicy.Strict(Path.of("known_hosts"))
        pool {
            this.maxSize = maxSize
            this.minIdle = minIdle
            this.idleTimeout = idleTimeout
        }
    }
}
