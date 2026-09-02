package sftp.connector.config

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import sftp.connector.error.ConfigurationError
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class ConnectorDslTest {

    @Test
    fun `a described connector becomes an immutable configuration`() {
        val config = sftpConnector("vendor-drop") {
            endpoint {
                host = "sftp.example"
                port = 2222
                proxy { httpConnect("proxy.internal", 3128) }
            }
            auth { password("etl", "s3cret") }
            hostKey = HostKeyPolicy.Strict(KNOWN_HOSTS)
            pool { socketTimeout = 45.seconds }
        }

        assertThat(config.name).isEqualTo("vendor-drop")
        assertThat(config.endpoint).isEqualTo(Endpoint("sftp.example", 2222, HttpConnectProxy("proxy.internal", 3128)))
        assertThat(config.hostKey).isEqualTo(HostKeyPolicy.Strict(KNOWN_HOSTS))
        assertThat(config.pool.socketTimeout).isEqualTo(45.seconds)
        assertThat(config.pool.connectTimeout).isEqualTo(10.seconds)
        assertThat(config.pool.keepAlive).isEqualTo(30.seconds)
        assertThat(config.pool.maxSize).isEqualTo(5)
    }

    @Test
    fun `a password never reaches a log line through the configuration's own printing`() {
        val config = minimalConnector { auth { password("etl", "s3cret") } }

        assertThat(config.toString()).doesNotContain("s3cret").contains("***")
    }

    @Test
    fun `an unset host key policy is refused, so accepting any key is never what happens by default`() {
        assertThatThrownBy {
            sftpConnector("vendor-drop") {
                endpoint { host = "sftp.example" }
                auth { password("etl", "s3cret") }
            }
        }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("hostKey is unset")
    }

    @Test
    fun `accepting any host key warns while the configuration is being built`() {
        val warning = capturingStandardError {
            minimalConnector { hostKey = HostKeyPolicy.AcceptAll }
        }

        assertThat(warning)
            .contains("WARN")
            .contains("vendor-drop")
            .contains("accepts any host key")
    }

    @Test
    fun `a strict host key policy passes without a warning`() {
        val quiet = capturingStandardError {
            minimalConnector { hostKey = HostKeyPolicy.Strict(KNOWN_HOSTS) }
        }

        assertThat(quiet).doesNotContain("WARN")
    }

    /**
     * An empty proxy block reads as "there is a proxy". Left to mean the opposite, it would
     * connect direct and only fail where the direct route is blocked, which is production.
     */
    @Test
    fun `a proxy block that names no proxy is refused rather than read as no proxy`() {
        assertThatThrownBy {
            minimalConnector { endpoint { host = "sftp.example"; proxy { } } }
        }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("the proxy block names no proxy")
    }

    @Test
    fun `every fault is reported at once, not one restart at a time`() {
        assertThatThrownBy {
            sftpConnector("vendor-drop") {
                endpoint {
                    host = " "
                    port = 70000
                    proxy { httpConnect("proxy.internal", -1) }
                }
                auth { password("", "s3cret") }
                hostKey = HostKeyPolicy.AcceptAll
                pool { maxSize = 0; keepAlive = (-1).minutes }
            }
        }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContainingAll(
                "endpoint host is blank",
                "endpoint port 70000",
                "proxy port -1",
                "auth user is blank",
                "maxSize 0",
                "keepAlive must be positive",
            )
    }

    /**
     * Zero would turn every caller that did not find a session already free straight away into a
     * failure, which reads in the log as a pool that is broken rather than one that is busy.
     */
    @Test
    fun `an acquire timeout that admits nobody is refused, and has a default that admits somebody`() {
        assertThatThrownBy { minimalConnector { pool { acquireTimeout = Duration.ZERO } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("acquireTimeout must be positive")

        assertThat(minimalConnector { }.pool.acquireTimeout).isEqualTo(30.seconds)
    }

    /**
     * The proxy on this path drops a tunnel it has not seen traffic on for five minutes, and it
     * does so without telling either end. A connector configured to go quiet for longer than that,
     * or to keep a parked session for longer, would be holding sessions the network had already
     * taken away and would find out one caller at a time.
     */
    @Test
    fun `I14_a keepalive or an idle timeout that outlasts the path's idle cutoff is refused`() {
        assertThatThrownBy { minimalConnector { pool { keepAlive = 5.minutes; idleCutoff = 5.minutes } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("keepAlive 5m is not shorter than idleCutoff 5m")

        assertThatThrownBy { minimalConnector { pool { idleTimeout = 6.minutes; idleCutoff = 5.minutes } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("idleTimeout 6m is not shorter than idleCutoff 5m")

        // Both defaults sit under the cutoff, so a connector nobody tuned is already correct.
        val defaults = minimalConnector { }.pool
        assertThat(defaults.keepAlive).isLessThan(defaults.idleCutoff)
        assertThat(defaults.idleTimeout).isLessThan(defaults.idleCutoff)
    }

    @Test
    fun `a pool told to keep more sessions ready than it may hold is refused`() {
        assertThatThrownBy { minimalConnector { pool { maxSize = 2; minIdle = 3 } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("minIdle 3 is more than maxSize 2")

        assertThatThrownBy { minimalConnector { pool { maxLifetimeJitter = 1.5 } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("maxLifetimeJitter 1.5 is outside 0.0..1.0")

        assertThat(minimalConnector { }.pool.minIdle).isZero()
    }

    private fun minimalConnector(extra: SftpConnectorBuilder.() -> Unit): SftpConnectorConfig =
        sftpConnector("vendor-drop") {
            endpoint { host = "sftp.example" }
            auth { password("etl", "s3cret") }
            hostKey = HostKeyPolicy.Strict(KNOWN_HOSTS)
            extra()
        }

    /**
     * The warning is the deliverable here, not a value on the config, so the test reads what an
     * operator would read. The test binding writes to standard error and looks it up on every
     * call, so swapping the stream around the build is enough to capture it.
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
        private val KNOWN_HOSTS: Path = Path.of("/etc/etl/known_hosts")
    }
}
