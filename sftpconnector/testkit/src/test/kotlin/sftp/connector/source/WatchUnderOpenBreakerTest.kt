package sftp.connector.source

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.client.SftpClient
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.sftpConnector
import sftp.connector.error.Attempt
import sftp.connector.error.ConnectFailed
import sftp.connector.pool.SftpPool
import sftp.connector.pool.virtualClock
import sftp.connector.source.SftpEvent.PollCompleted
import sftp.connector.source.SftpEvent.PollFailed
import sftp.connector.source.SftpEvent.PollSkipped
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Operation
import java.nio.file.Path
import kotlin.time.Duration.Companion.minutes

/**
 * S3 as the watch sees it. The pieces are proven elsewhere - the breaker against a real server in
 * `ResilienceAgainstServerTest`, one skipped tick in `SftpWatchTest` - and this is the whole
 * sequence in one place: the ticks that fail, every tick after them skipped without a dial, and
 * the first tick after the wait let through as the probe that closes the breaker.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class WatchUnderOpenBreakerTest {

    @TempDir
    lateinit var stage: Path

    private val registry = SimpleMeterRegistry()

    private lateinit var background: CoroutineScope

    @AfterEach
    fun stopTheTicker() {
        if (::background.isInitialized) background.cancel()
    }

    @Test
    fun `S3_an open breaker skips every tick without dialling, until the probe after the wait closes it`() = runTest {
        var refusals = 0
        val transport = FakeSftpTransport { call ->
            if (call.operation == Operation.Connect && refusals++ < 2) throw ConnectFailed(Attempt(ENDPOINT, "connect"), "the proxy refused")
        }.directory("/drop")
        val config = sftpConnector("breaker-watch-demo") {
            endpoint { host = "fake.example"; port = 22 }
            auth { password("etl", "s3cret") }
            hostKey = HostKeyPolicy.AcceptAll
            polling { staging { dir = stage }; directories("/drop") }
            resilience {
                retry { maxAttempts = 1 }
                circuitBreaker { slidingWindow = 2; failureRateThreshold = 50; waitInOpen = WAIT_IN_OPEN }
            }
        }
        val pool = SftpPool(transport, config, registry, virtualClock())
        background = CoroutineScope(StandardTestDispatcher(testScheduler) + SupervisorJob())
        val source = SftpSource(SftpClient(pool, config, registry, virtualClock()), config, registry, virtualClock(), background)
        val events = mutableListOf<SftpEvent>()
        val collector = launch { source.watch("/drop", EVERY).collect { events += it } }

        // Two ticks fail to dial and open the breaker; every tick until the wait has passed is skipped.
        advanceTimeBy(WAIT_IN_OPEN)
        runCurrent()
        assertThat(events.filterIsInstance<PollFailed>().map { it.tick to it.error::class })
            .containsExactly(1L to ConnectFailed::class, 2L to ConnectFailed::class)
        assertThat(events.filterIsInstance<PollCompleted>()).describedAs("ticks let through while open").isEmpty()
        assertThat(events.filterIsInstance<PollSkipped>()).hasSizeGreaterThanOrEqualTo(TICKS_SURELY_INSIDE_THE_WAIT)
        assertThat(transport.calls.count { it.operation == Operation.Connect }).describedAs("dials: two refused, none while open").isEqualTo(2)
        assertThat(breakerState()).isEqualTo(2)

        // The first tick after the wait is the probe; the server answers it and the breaker closes.
        advanceTimeBy(EVERY * 2)
        runCurrent()
        val outcomes = events.filterNot { it is SftpEvent.PollStarted }
        assertThat(outcomes.last()).isInstanceOf(PollCompleted::class.java)
        assertThat(outcomes.dropLast(1).drop(2)).describedAs("every tick between the failures and the probe")
            .isNotEmpty().allSatisfy { assertThat(it).isEqualTo(PollSkipped((it as PollSkipped).tick, SkipCause.BREAKER_OPEN)) }
        assertThat(transport.calls.count { it.operation == Operation.Connect }).describedAs("dials, the probe included").isEqualTo(3)
        assertThat(breakerState()).isZero()
        collector.cancelAndJoin()
    }

    private fun breakerState(): Int = registry.get("sftp_breaker_state").gauge().value().toInt()

    private companion object {
        private const val ENDPOINT = "fake.example:22"
        private val EVERY = 1.minutes
        private val WAIT_IN_OPEN = 5.minutes

        /** The breaker opens on the second tick, so the ticks from the third to the wait's end are inside it. */
        private val TICKS_SURELY_INSIDE_THE_WAIT = (WAIT_IN_OPEN / EVERY).toInt() - 1
    }
}
