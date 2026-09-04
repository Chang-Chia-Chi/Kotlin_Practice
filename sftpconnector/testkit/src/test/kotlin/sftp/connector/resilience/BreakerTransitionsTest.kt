package sftp.connector.resilience

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.client.SftpClient
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.Attempt
import sftp.connector.error.ConnectFailed
import sftp.connector.pool.SftpPool
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Operation
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Path
import java.time.Clock

/**
 * The breaker opening is the loudest thing the connector can do to a pipeline - every call after
 * it is refused without a packet leaving the host - and it used to do it in silence. The gauge
 * says what the state is now; nobody reads a gauge at three in the morning, and no gauge can be
 * read for a moment that has passed. So each transition is a line of its own.
 *
 * Nothing here waits: two dials that fail fill the window, and the third call is refused at the
 * door.
 */
class BreakerTransitionsTest {

    @TempDir
    lateinit var stage: Path

    private val registry = SimpleMeterRegistry()

    @Test
    fun `the breaker names the endpoint and the states it moved between when it opens`() {
        val transport = FakeSftpTransport { call ->
            if (call.operation == Operation.Connect) throw ConnectFailed(Attempt(ENDPOINT, "connect"), "the proxy refused")
        }
        val config = config()
        val client = SftpClient(SftpPool(transport, config, registry, CLOCK), config, registry, CLOCK)

        val logged = capturingStandardError {
            repeat(2) { runBlocking { runCatching { client.exists("/drop") } } }
        }

        assertEquals(2.0, registry.get("sftp_breaker_state").gauge().value(), "the breaker is open")
        val transitions = logged.lines().filter { it.contains("circuit breaker") }
        assertEquals(1, transitions.size, "one line per transition, and one transition happened: $logged")
        assertTrue(transitions.single().contains(ENDPOINT), "the endpoint it broke to: $logged")
        assertTrue(transitions.single().contains("open"), "the state it moved to: $logged")
        assertTrue(transitions.single().contains("closed"), "the state it moved from: $logged")
    }

    private fun config(): SftpConnectorConfig = sftpConnector("breaker-demo") {
        endpoint { host = "fake.example"; port = 22 }
        auth { password("etl", "s3cret") }
        hostKey = HostKeyPolicy.AcceptAll
        polling { staging { dir = stage }; directories("/drop") }
        resilience {
            retry { maxAttempts = 1 }
            circuitBreaker { slidingWindow = 2; failureRateThreshold = 50 }
        }
    }

    /**
     * The line is the deliverable, so the test reads what an operator would read. The test binding
     * writes to standard error and looks it up on every call, so swapping the stream is enough.
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

    private companion object {
        private const val ENDPOINT = "fake.example:22"
        private val CLOCK: Clock = Clock.systemUTC()
    }
}
