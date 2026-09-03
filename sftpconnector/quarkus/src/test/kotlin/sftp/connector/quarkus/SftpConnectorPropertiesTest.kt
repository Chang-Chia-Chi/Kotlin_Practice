package sftp.connector.quarkus

import io.smallrye.config.PropertiesConfigSource
import io.smallrye.config.SmallRyeConfigBuilder
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import sftp.connector.client.Overwrite
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.PostAction
import sftp.connector.error.ConfigurationError
import sftp.connector.source.AllOf
import sftp.connector.source.MarkerFile
import sftp.connector.source.MinAge
import java.nio.file.Path
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * The translation from a properties file to a configuration, without a Quarkus application around
 * it - the mapping is read by SmallRye either way, and a test that boots a host to check a
 * property name is a slow test of the wrong thing.
 *
 * The claim being made in all five is one claim: the properties carry values and decide nothing.
 */
class SftpConnectorPropertiesTest {

    /**
     * The fault that must never be silent. There is no default host-key policy anywhere in this
     * connector, and the properties do not add one: a deployment that forgot to choose is stopped
     * and told, rather than started with every key accepted.
     */
    @Test
    fun `properties that name no host key policy are refused, and the refusal says so`() {
        assertThatThrownBy { propertiesOf(reachable() - "sftp.connector.host-key.policy").toConnectorConfig() }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("hostKey is unset")
            .hasMessageContaining("AcceptAll")
    }

    /**
     * Four faults, one exception. This is what the mapping buys by validating nothing itself: an
     * operator corrects a properties file in one pass instead of one restart per mistake.
     */
    @Test
    fun `every fault in one properties file is reported together`() {
        assertThatThrownBy {
            propertiesOf(
                mapOf(
                    "sftp.connector.endpoint.port" to "70000",
                    "sftp.connector.pool.max-size" to "0",
                    "sftp.connector.polling.on-ack.kind" to "MOVE",
                ),
            ).toConnectorConfig()
        }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("endpoint host is blank")
            .hasMessageContaining("endpoint port 70000")
            .hasMessageContaining("no auth block")
            .hasMessageContaining("hostKey is unset")
            .hasMessageContaining("pool maxSize 0")
            .hasMessageContaining("names no folder to move them to")
    }

    /**
     * The policy a deployment actually uses, and the file it cannot work without. A strict policy
     * that quietly dropped the file it was given would verify against nothing the operator chose,
     * which is the failure the whole knob exists to prevent.
     */
    @Test
    fun `a strict host key policy carries the known-hosts file it was given`() {
        val config = propertiesOf(
            reachable() + mapOf(
                "sftp.connector.host-key.policy" to "STRICT",
                "sftp.connector.host-key.known-hosts" to "/etc/etl/known_hosts",
            ),
        ).toConnectorConfig()

        assertThat(config.hostKey).isEqualTo(HostKeyPolicy.Strict(Path.of("/etc/etl/known_hosts")))
    }

    /** What nobody wrote is left to the builder, rather than restated here and drifting from it. */
    @Test
    fun `a property nobody set keeps the connector's own default`() {
        val config = propertiesOf(reachable()).toConnectorConfig()

        assertThat(config.pool.maxSize).isEqualTo(5)
        assertThat(config.pool.drainTimeout).isEqualTo(30.seconds)
        assertThat(config.polling.maxInFlight).isEqualTo(16)
        assertThat(config.polling.onAck).isEqualTo(PostAction.Noop)
        assertThat(config.resilience.retry.maxAttempts).isEqualTo(3)
        assertThat(config.resilience.retry.backoff.initial).isEqualTo(1.seconds)
        // Left alone, the readiness is the connector's own heuristic and not something built here.
        assertThat(config.polling.readiness).isInstanceOf(AllOf::class.java)
    }

    /** And what somebody did write arrives, in every block, with its units converted. */
    @Test
    fun `each block of a properties file reaches the configuration it describes`() {
        val config = propertiesOf(
            reachable() + mapOf(
                "sftp.connector.endpoint.proxy.host" to "proxy.internal",
                "sftp.connector.endpoint.proxy.port" to "3128",
                "sftp.connector.pool.max-size" to "3",
                "sftp.connector.pool.acquire-timeout" to "PT15S",
                "sftp.connector.polling.directories" to "/in,/priority",
                "sftp.connector.polling.on-ack.kind" to "MOVE",
                "sftp.connector.polling.on-ack.target" to "done/",
                "sftp.connector.polling.on-ack.overwrite" to "REPLACE",
                "sftp.connector.polling.on-nack.kind" to "DELETE",
                "sftp.connector.polling.readiness.min-age" to "PT2M",
                "sftp.connector.polling.readiness.marker-file" to ".ok",
                "sftp.connector.resilience.max-concurrent-transfers" to "2",
                "sftp.connector.resilience.retry.backoff-max" to "PT45S",
                "sftp.connector.resilience.circuit-breaker.sliding-window" to "50",
            ),
        ).toConnectorConfig()

        assertThat(config.name).isEqualTo("vendor-drop")
        assertThat(config.endpoint.proxy?.host).isEqualTo("proxy.internal")
        assertThat(config.hostKey).isEqualTo(HostKeyPolicy.AcceptAll)
        assertThat(config.pool.maxSize).isEqualTo(3)
        assertThat(config.pool.acquireTimeout).isEqualTo(15.seconds)
        assertThat(config.polling.directories).containsExactly("/in", "/priority")
        assertThat(config.polling.onAck).isEqualTo(PostAction.Move("done/", Overwrite.REPLACE))
        assertThat(config.polling.onNack).isEqualTo(PostAction.Delete)
        assertThat(config.resilience.maxConcurrentTransfers).isEqualTo(2)
        assertThat(config.resilience.retry.backoff.max).isEqualTo(45.seconds)
        // The half of the backoff curve nobody named is still the builder's, not a repeat of it.
        assertThat(config.resilience.retry.backoff.initial).isEqualTo(1.seconds)
        assertThat(config.resilience.circuitBreaker.slidingWindow).isEqualTo(50)

        // Two named checks compose, and they replace the default rather than joining it.
        val readiness = config.polling.readiness
        assertThat(readiness).isInstanceOf(AllOf::class.java)
        assertThat((readiness as AllOf).checks).hasSize(2)
        assertThat(readiness.checks.filterIsInstance<MinAge>().single().duration).isEqualTo(2.minutes)
        assertThat(readiness.checks.filterIsInstance<MarkerFile>().single().suffix).isEqualTo(".ok")
    }

    /** The least a properties file can say and still describe a connector that could start. */
    private fun reachable() = mapOf(
        "sftp.connector.name" to "vendor-drop",
        "sftp.connector.endpoint.host" to "sftp.example",
        "sftp.connector.auth.user" to "etl",
        "sftp.connector.auth.password" to "s3cret",
        "sftp.connector.host-key.policy" to "ACCEPT_ALL",
    )

    private fun propertiesOf(values: Map<String, String>): SftpConnectorProperties =
        SmallRyeConfigBuilder()
            .withSources(PropertiesConfigSource(values, "test", 100))
            .withMapping(SftpConnectorProperties::class.java)
            .build()
            .getConfigMapping(SftpConnectorProperties::class.java)
}
