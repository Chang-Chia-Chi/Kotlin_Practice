package sftp.connector

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.PollingBuilder
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.ConfigurationError
import sftp.connector.pool.virtualClock
import sftp.connector.testkit.FakeSftpTransport
import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * The connector as a thing with a life rather than a thing with methods: what it does before it
 * hands itself over, what it starts running afterwards, and what it leaves behind when it refuses.
 *
 * None of these waits. The pool's clock and the connector's own coroutines both read the test
 * scheduler, so half a minute of a housekeeping interval costs the suite nothing and still happens
 * in the order it would happen in production.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class SftpConnectorTest {

    @TempDir
    lateinit var local: Path

    /**
     * The pool fills itself to the spares it was told to keep, and nobody waits for it. A
     * deployment held up until warm sessions exist would be trading a readiness check that matters
     * for a handshake that does not: an empty pool works, it is merely slower on the first call.
     */
    @Test
    fun `the pool fills to its minimum in the background, and the connector works before it has`() = runTest {
        val transport = FakeSftpTransport().directory("/drop")
        val connector = start(transport) { directories("/drop") }

        // Only the session the checks themselves borrowed, which is one short of the minimum.
        assertThat(connector.pool.stats().total).describedAs("sessions the moment start-up returned").isEqualTo(1)
        assertThat(connector.client.exists("/drop")).describedAs("usable straight away").isTrue()

        advanceTimeBy(HOUSEKEEPING + 1.milliseconds)
        runCurrent()

        assertThat(connector.pool.stats().idle).describedAs("spares once the pool had looked at itself").isEqualTo(2)
        connector.backgroundWork.cancelAndJoin()
    }

    /**
     * The checks run before anything is launched, so a start-up that refuses leaves no housekeeper
     * dialling a server the connector has just declared unusable. The silence afterwards is what
     * says so: the pool is two sessions short of the minimum it was given and nothing is doing
     * anything about it, round after round.
     *
     * The sessions the checks themselves opened do outlive the refusal - closing the pool is the
     * phased shutdown that does not exist yet.
     */
    @Test
    fun `a start-up that was refused starts no housekeeper`() = runTest {
        val transport = FakeSftpTransport()

        val refusal = runCatching { start(transport) { directories("/drop") } }.exceptionOrNull()

        assertThat(refusal).isInstanceOf(ConfigurationError::class.java).hasMessageContaining("/drop")
        val callsWhenItGaveUp = transport.calls.size
        advanceTimeBy(HOUSEKEEPING * 10)
        runCurrent()
        assertThat(transport.calls.size).describedAs("calls made after the refusal").isEqualTo(callsWhenItGaveUp)
    }

    /**
     * The knobs that decide where a file goes are checked while the configuration is being built,
     * which is before a connector exists and therefore before anything could have been dialled.
     * This one would send every acked file back into the directory it was watched in, and the
     * connector would keep finding it for as long as it ran.
     */
    @Test
    fun `an action that files a message back where it came from is refused before there is a connector`() = runTest {
        val refusal = runCatching { configFor { directories("/drop"); onAck = move("/drop") } }.exceptionOrNull()

        assertThat(refusal).isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("move it onto itself")
    }

    private suspend fun TestScope.start(
        transport: FakeSftpTransport,
        polling: PollingBuilder.() -> Unit,
    ): SftpConnector =
        SftpConnector.start(
            configFor(polling),
            transport,
            clock = virtualClock(),
            background = StandardTestDispatcher(testScheduler),
        )

    private fun configFor(describePolling: PollingBuilder.() -> Unit): SftpConnectorConfig =
        sftpConnector("connector-demo") {
            endpoint { host = "sftp.example"; port = 22 }
            auth { password("etl", "s3cret") }
            hostKey = HostKeyPolicy.AcceptAll
            pool { maxSize = 3; minIdle = 2; housekeepingInterval = HOUSEKEEPING }
            polling { staging { dir = local }; describePolling() }
        }

    private companion object {
        private val HOUSEKEEPING: Duration = 30.seconds
    }
}
