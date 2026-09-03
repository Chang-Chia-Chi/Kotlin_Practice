package sftp.connector.config

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import sftp.connector.error.ConfigurationError
import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * The resilience block: what a connector nobody tuned retries, breaks on and waits for, and the
 * combinations the builder refuses because they would make a busy pool read as a broken server.
 */
class ResilienceDslTest {

    @Test
    fun `a described resilience block becomes configuration, and every knob has a default`() {
        val tuned = minimalConnector {
            resilience {
                retry { maxAttempts = 5; backoff = exponential(2.seconds, max = 20.seconds, jitter = false) }
                circuitBreaker { failureRateThreshold = 60; slidingWindow = 10; waitInOpen = 2.minutes }
                bulkhead { maxConcurrentTransfers = 2 }
                operationTimeout = 45.seconds
                transferTimeout = 20.minutes
            }
        }.resilience

        assertThat(tuned.retry).isEqualTo(RetryPolicy(5, Backoff(2.seconds, 20.seconds, jitter = false)))
        assertThat(tuned.circuitBreaker).isEqualTo(BreakerPolicy(60, 10, 2.minutes))
        assertThat(tuned.maxConcurrentTransfers).isEqualTo(2)
        assertThat(tuned.operationTimeout).isEqualTo(45.seconds)
        assertThat(tuned.transferTimeout).isEqualTo(20.minutes)

        val defaults = minimalConnector { }.resilience
        assertThat(defaults.retry).isEqualTo(RetryPolicy(3, Backoff(1.seconds, 30.seconds, jitter = true)))
        assertThat(defaults.circuitBreaker).isEqualTo(BreakerPolicy(50, 20, 1.minutes))
        assertThat(defaults.maxConcurrentTransfers).isEqualTo(4)
        assertThat(defaults.operationTimeout).isEqualTo(1.minutes)
        assertThat(defaults.transferTimeout).isEqualTo(15.minutes)
    }

    @Test
    fun `retry and breaker settings that could never fire, or would fire on nothing, are refused`() {
        assertThatThrownBy {
            minimalConnector {
                resilience {
                    retry { maxAttempts = 0; backoff = exponential(Duration.ZERO, max = 1.seconds) }
                    circuitBreaker { failureRateThreshold = 0; slidingWindow = 0; waitInOpen = Duration.ZERO }
                }
            }
        }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContainingAll(
                "retry maxAttempts 0",
                "backoff initial 0s must be positive",
                "circuitBreaker failureRateThreshold 0 is outside 1..100",
                "circuitBreaker slidingWindow 0",
                "circuitBreaker waitInOpen must be positive",
            )

        assertThatThrownBy { minimalConnector { resilience { retry { backoff = exponential(10.seconds, max = 1.seconds) } } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("backoff max 1s is shorter than its initial 10s")
    }

    /** Transfers beyond the pool could never run at once; zero of them could never run at all. */
    @Test
    fun `more concurrent transfers than sessions is refused`() {
        assertThatThrownBy { minimalConnector { pool { maxSize = 2 }; resilience { bulkhead { maxConcurrentTransfers = 3 } } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("maxConcurrentTransfers 3 is more than pool maxSize 2")

        assertThatThrownBy { minimalConnector { resilience { bulkhead { maxConcurrentTransfers = 0 } } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("maxConcurrentTransfers 0")
    }

    /**
     * The time allowed for an operation starts before the session is borrowed, so a limit shorter
     * than the wait for a session would report a full pool as a server that stopped answering -
     * and count it against the breaker, which a full pool must never do.
     */
    @Test
    fun `an operation timeout that could run out while still queued for a session is refused`() {
        assertThatThrownBy { minimalConnector { pool { acquireTimeout = 30.seconds }; resilience { operationTimeout = 30.seconds } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("operationTimeout 30s is not longer than pool acquireTimeout 30s")

        assertThatThrownBy { minimalConnector { pool { acquireTimeout = 30.seconds }; resilience { transferTimeout = 10.seconds } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("transferTimeout 10s is not longer than pool acquireTimeout 30s")
    }

    private fun minimalConnector(extra: SftpConnectorBuilder.() -> Unit): SftpConnectorConfig =
        sftpConnector("vendor-drop") {
            endpoint { host = "sftp.example" }
            auth { password("etl", "s3cret") }
            hostKey = HostKeyPolicy.Strict(Path.of("/etc/etl/known_hosts"))
            extra()
        }
}
