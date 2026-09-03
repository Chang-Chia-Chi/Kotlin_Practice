package sftp.connector.config

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.error.ConfigurationError
import sftp.connector.source.AllOf
import sftp.connector.source.MinAge
import sftp.connector.source.SizeStable
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Files
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
            pool { cancelGrace = 45.seconds }
        }

        assertThat(config.name).isEqualTo("vendor-drop")
        assertThat(config.endpoint).isEqualTo(Endpoint("sftp.example", 2222, HttpConnectProxy("proxy.internal", 3128)))
        assertThat(config.hostKey).isEqualTo(HostKeyPolicy.Strict(KNOWN_HOSTS))
        assertThat(config.pool.cancelGrace).isEqualTo(45.seconds)
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

    /**
     * A staging directory that is missing or read-only makes every download fail, and it fails the
     * same way every time. Found at deployment it costs a restart; found an hour into a run, after
     * a listing and a lease and most of a transfer, it costs the run.
     */
    @Test
    fun `a staging directory the connector cannot write a download into is refused`(@TempDir usable: Path) {
        assertThatThrownBy { minimalConnector { polling { staging { dir = usable.resolve("not-created-yet") } } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("is not a directory that exists")

        assertThatThrownBy { minimalConnector { polling { staging { dir = Files.createFile(usable.resolve("a-file")) } } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("is not a directory that exists")

        val configured = minimalConnector { polling { staging { dir = usable; digest = Digest.MD5 } } }
        assertThat(configured.polling.staging.dir).isEqualTo(usable)
        assertThat(configured.polling.staging.digest).isEqualTo(Digest.MD5)
    }

    @Test
    fun `a connector nobody configured for staging still has somewhere to put a download`() {
        val defaults = minimalConnector { }.polling.staging

        // Whatever the JVM was given as its temp directory: it exists and is writable wherever the
        // connector runs, which is what makes the rule above pass without anyone setting anything.
        assertThat(Files.isDirectory(defaults.dir)).isTrue()
        assertThat(Files.isWritable(defaults.dir)).isTrue()
        assertThat(defaults.digest).isEqualTo(Digest.SHA256)
    }

    /**
     * The one rule about post-processing that can be decided without asking the server, and the
     * reason it is decided here: an action that files a message back into the directory it came
     * out of would hand the same file to every poll for as long as the connector ran, and nothing
     * about that looks like a failure from the outside.
     */
    @Test
    fun `an action target that is the watched directory itself is refused`() {
        assertThatThrownBy { minimalConnector { polling { directories("/drop"); onAck = move("/drop") } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("onAck moves files to /drop, which is the directory they were watched in")

        // The same fault in the spelling the comparison above cannot see: "." is not the string
        // "/drop" and never will be, but it resolves onto it, so it is refused for naming no
        // folder rather than for being equal to one.
        assertThatThrownBy { minimalConnector { polling { directories("/drop"); onNack = move(".") } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("onNack moves files to \".\", which names no folder")

        assertThatThrownBy { minimalConnector { polling { directories("/drop"); onNack = move("") } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("names no folder")

        // And the ordinary arrangement is accepted: a folder under the watched directory.
        val configured = minimalConnector { polling { directories("/drop"); onAck = move("temp/") } }
        assertThat(configured.polling.actionTargetsUnder("/drop")).containsExactly("/drop/temp")
    }

    @Test
    fun `a move target starting with a slash is that path, and any other is under the directory it came from`() {
        val config = minimalConnector {
            polling { directories("/drop", "/inbox"); onAck = move("done/"); onNack = move("/quarantine") }
        }

        assertThat(config.polling.actionTargetsUnder("/drop")).containsExactly("/drop/done", "/quarantine")
        assertThat(config.polling.actionTargetsUnder("/inbox")).containsExactly("/inbox/done", "/quarantine")
    }

    @Test
    fun `a connector nobody configured for polling leaves the server alone and still checks itself`() {
        val defaults = minimalConnector { }.polling

        assertThat(defaults.directories).isEmpty()
        assertThat(defaults.onAck).isEqualTo(PostAction.Noop)
        assertThat(defaults.onNack).isEqualTo(PostAction.Noop)
        assertThat(defaults.createActionTargets).isTrue()
        assertThat(defaults.startupProbe).isTrue()
    }

    /**
     * The poll's own knobs. A zero of either count is a poll that can never hand over a file, and
     * the shipped readiness check is the heuristic the design names: a size that held still, and
     * a minute of nobody touching it.
     */
    @Test
    fun `a poll that could hand over nothing is refused, and the defaults are the documented heuristic`() {
        assertThatThrownBy { minimalConnector { polling { maxInFlight = 0; maxFilesPerPoll = 0 } } }
            .isInstanceOf(ConfigurationError::class.java)
            .hasMessageContaining("maxInFlight 0")
            .hasMessageContaining("maxFilesPerPoll 0")

        val defaults = minimalConnector { }.polling
        assertThat(defaults.maxInFlight).isEqualTo(16)
        assertThat(defaults.maxFilesPerPoll).isEqualTo(1000)
        assertThat(defaults.recursive).isFalse()
        val heuristic = defaults.readiness as AllOf
        assertThat(heuristic.checks).hasSize(2)
        assertThat(heuristic.checks[0]).isInstanceOfSatisfying(SizeStable::class.java) {
            assertThat(it.checks).isEqualTo(2)
            assertThat(it.interval).isEqualTo(10.seconds)
        }
        assertThat(heuristic.checks[1]).isInstanceOfSatisfying(MinAge::class.java) { assertThat(it.duration).isEqualTo(1.minutes) }
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
