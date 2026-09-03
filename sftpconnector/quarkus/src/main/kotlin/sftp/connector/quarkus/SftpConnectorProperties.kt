package sftp.connector.quarkus

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import sftp.connector.client.Overwrite
import sftp.connector.config.Digest
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.OverlapPolicy
import sftp.connector.config.PollingBuilder
import sftp.connector.config.PostAction
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.source.ReadinessCheck
import sftp.connector.source.plus
import java.nio.file.Path
import java.util.Optional
import kotlin.time.toKotlinDuration
import java.time.Duration as JavaDuration

/**
 * `sftp.connector.*` from the host's application properties, exactly as it was written and not a
 * word more.
 *
 * **Nothing here is validated, and that is the design.** Every value is optional, including the
 * ones the connector cannot run without, so that a property nobody set arrives as "absent" rather
 * than as a substitute the operator never chose. What a valid configuration is has one definition,
 * in the connector's own builder, and [toConnectorConfig] is where these values are put to it: an
 * operator with four faults in their properties file then hears about all four in one exception
 * and corrects them in one pass, which is the whole reason the builder collects faults rather than
 * throwing at the first.
 *
 * The consequence to keep in mind when adding a knob: an absent property must leave the builder's
 * own default in place, never restate it. A default written twice is a default that will one day
 * disagree with itself, and the copy here would be the one no test covers.
 *
 * Durations are `java.time.Duration`, which is what the host's configuration layer knows how to
 * parse - `30s`, `4m`, `PT1M30S` - and are handed to the builder as `kotlin.time.Duration`, which
 * is what the connector speaks.
 */
@ConfigMapping(prefix = "sftp.connector")
interface SftpConnectorProperties {

    /** Tags every metric and log line this connector produces. */
    fun name(): Optional<String>

    fun endpoint(): EndpointProperties
    fun auth(): AuthProperties
    fun hostKey(): HostKeyProperties
    fun pool(): PoolProperties
    fun polling(): PollingProperties
    fun resilience(): ResilienceProperties

    interface EndpointProperties {
        fun host(): Optional<String>
        fun port(): Optional<Int>

        /** Only set on a network that reaches the server through an HTTP CONNECT proxy. */
        fun proxy(): ProxyProperties
    }

    interface ProxyProperties {
        fun host(): Optional<String>
        fun port(): Optional<Int>
    }

    interface AuthProperties {
        fun user(): Optional<String>
        fun password(): Optional<String>
    }

    /**
     * How the server's identity is checked. There is no default and there will not be one: the
     * absent case is a refusal at start-up, because the alternative is a deployment that silently
     * accepts any key from anything sitting on the network path.
     */
    interface HostKeyProperties {
        fun policy(): Optional<HostKeyKind>

        /** Required by [HostKeyKind.STRICT], and the only thing it needs. */
        fun knownHosts(): Optional<Path>
    }

    interface PoolProperties {
        fun maxSize(): Optional<Int>
        fun minIdle(): Optional<Int>
        fun acquireTimeout(): Optional<JavaDuration>
        fun connectTimeout(): Optional<JavaDuration>
        fun keepAlive(): Optional<JavaDuration>
        fun cancelGrace(): Optional<JavaDuration>
        fun drainTimeout(): Optional<JavaDuration>
        fun idleTimeout(): Optional<JavaDuration>
        fun idleCutoff(): Optional<JavaDuration>
        fun maxLifetime(): Optional<JavaDuration>
        fun maxLifetimeJitter(): Optional<Double>
        fun validationBypass(): Optional<JavaDuration>
        fun leakDetectionThreshold(): Optional<JavaDuration>
        fun housekeepingInterval(): Optional<JavaDuration>
    }

    interface PollingProperties {
        fun directories(): Optional<List<String>>
        fun onAck(): ActionProperties
        fun onNack(): ActionProperties
        fun createActionTargets(): Optional<Boolean>
        fun startupProbe(): Optional<Boolean>
        fun maxInFlight(): Optional<Int>
        fun maxFilesPerPoll(): Optional<Int>
        fun recursive(): Optional<Boolean>
        fun overlap(): Optional<OverlapPolicy>
        fun readiness(): ReadinessProperties
        fun staging(): StagingProperties
    }

    /** What becomes of a file the consumer has answered for. */
    interface ActionProperties {
        fun kind(): Optional<ActionKind>

        /** Where [ActionKind.MOVE] puts the file. */
        fun target(): Optional<String>

        @WithDefault("REFUSE")
        fun overwrite(): Overwrite
    }

    /**
     * What a listed file has to pass before it is handed over. Naming any one of these replaces
     * the connector's default outright rather than adding to it - a deployment that says "a file
     * is ready once its marker is there" means that and not "that as well as a minute of age".
     * Naming several composes them, and every one has to pass.
     */
    interface ReadinessProperties {
        /** How many times the size has to be seen unchanged. Setting it turns the check on. */
        fun sizeStableChecks(): Optional<Int>

        @WithDefault("PT10S")
        fun sizeStableInterval(): JavaDuration

        fun minAge(): Optional<JavaDuration>
        fun markerFile(): Optional<String>
    }

    interface StagingProperties {
        fun dir(): Optional<Path>
        fun digest(): Optional<Digest>
    }

    interface ResilienceProperties {
        fun operationTimeout(): Optional<JavaDuration>
        fun transferTimeout(): Optional<JavaDuration>
        fun maxConcurrentTransfers(): Optional<Int>
        fun retry(): RetryProperties
        fun circuitBreaker(): CircuitBreakerProperties
    }

    interface RetryProperties {
        fun maxAttempts(): Optional<Int>
        fun backoffInitial(): Optional<JavaDuration>
        fun backoffMax(): Optional<JavaDuration>
        fun backoffJitter(): Optional<Boolean>
    }

    interface CircuitBreakerProperties {
        fun failureRateThreshold(): Optional<Int>
        fun slidingWindow(): Optional<Int>
        fun waitInOpen(): Optional<JavaDuration>
    }
}

/** The host-key policies a properties file can name. */
enum class HostKeyKind { STRICT, ACCEPT_ALL }

/** The post-actions a properties file can name, one spelling each. */
enum class ActionKind { MOVE, DELETE, NOOP }

/**
 * Puts what the host wrote to the connector's builder, which is the only thing that decides
 * whether it describes a connector that can run.
 *
 * Every property that was not set is simply not mentioned, so the builder's default stands. That
 * includes the ones with no default: an absent host-key policy reaches the builder as the absence
 * it is and comes back as a refusal naming it, rather than as a quiet `AcceptAll`.
 *
 * @throws sftp.connector.error.ConfigurationError listing every fault in the properties at once.
 */
fun SftpConnectorProperties.toConnectorConfig(): SftpConnectorConfig =
    sftpConnector(name().orElse("")) {
        endpoint {
            endpoint().host().ifPresent { host = it }
            endpoint().port().ifPresent { port = it }
            val proxied = endpoint().proxy()
            if (proxied.host().isPresent || proxied.port().isPresent) {
                // Opened only when something in it was set, because an empty proxy block is a
                // fault in the builder - it reads as "there is a proxy" and means the opposite -
                // and a host that never mentioned a proxy has not made that mistake.
                proxy { httpConnect(proxied.host().orElse(""), proxied.port().orElse(0)) }
            }
        }
        if (auth().user().isPresent || auth().password().isPresent) {
            auth { password(auth().user().orElse(""), auth().password().orElse("")) }
        }
        hostKey = hostKey().policy().map { policyFor(it, hostKey().knownHosts()) }.orElse(null)
        pool {
            pool().maxSize().ifPresent { maxSize = it }
            pool().minIdle().ifPresent { minIdle = it }
            pool().acquireTimeout().ifPresent { acquireTimeout = it.toKotlinDuration() }
            pool().connectTimeout().ifPresent { connectTimeout = it.toKotlinDuration() }
            pool().keepAlive().ifPresent { keepAlive = it.toKotlinDuration() }
            pool().cancelGrace().ifPresent { cancelGrace = it.toKotlinDuration() }
            pool().drainTimeout().ifPresent { drainTimeout = it.toKotlinDuration() }
            pool().idleTimeout().ifPresent { idleTimeout = it.toKotlinDuration() }
            pool().idleCutoff().ifPresent { idleCutoff = it.toKotlinDuration() }
            pool().maxLifetime().ifPresent { maxLifetime = it.toKotlinDuration() }
            pool().maxLifetimeJitter().ifPresent { maxLifetimeJitter = it }
            pool().validationBypass().ifPresent { validationBypass = it.toKotlinDuration() }
            pool().leakDetectionThreshold().ifPresent { leakDetectionThreshold = it.toKotlinDuration() }
            pool().housekeepingInterval().ifPresent { housekeepingInterval = it.toKotlinDuration() }
        }
        polling {
            polling().directories().ifPresent { directories(*it.toTypedArray()) }
            actionFrom(polling().onAck())?.let { onAck = it }
            actionFrom(polling().onNack())?.let { onNack = it }
            polling().createActionTargets().ifPresent { createActionTargets = it }
            polling().startupProbe().ifPresent { startupProbe = it }
            polling().maxInFlight().ifPresent { maxInFlight = it }
            polling().maxFilesPerPoll().ifPresent { maxFilesPerPoll = it }
            polling().recursive().ifPresent { recursive = it }
            polling().overlap().ifPresent { overlap = it }
            readinessFrom(polling().readiness())?.let { readiness = it }
            staging {
                polling().staging().dir().ifPresent { dir = it }
                polling().staging().digest().ifPresent { digest = it }
            }
        }
        resilience {
            resilience().operationTimeout().ifPresent { operationTimeout = it.toKotlinDuration() }
            resilience().transferTimeout().ifPresent { transferTimeout = it.toKotlinDuration() }
            resilience().maxConcurrentTransfers().ifPresent { transfers -> bulkhead { maxConcurrentTransfers = transfers } }
            retry {
                resilience().retry().maxAttempts().ifPresent { maxAttempts = it }
                val described = resilience().retry()
                if (described.backoffInitial().isPresent ||
                    described.backoffMax().isPresent ||
                    described.backoffJitter().isPresent
                ) {
                    // The three are one value. Naming any of them replaces the whole curve, so
                    // the two nobody mentioned come from the default curve rather than from
                    // numbers repeated here.
                    backoff = exponential(
                        initial = described.backoffInitial().map { it.toKotlinDuration() }.orElse(backoff.initial),
                        max = described.backoffMax().map { it.toKotlinDuration() }.orElse(backoff.max),
                        jitter = described.backoffJitter().orElse(backoff.jitter),
                    )
                }
            }
            circuitBreaker {
                resilience().circuitBreaker().failureRateThreshold().ifPresent { failureRateThreshold = it }
                resilience().circuitBreaker().slidingWindow().ifPresent { slidingWindow = it }
                resilience().circuitBreaker().waitInOpen().ifPresent { waitInOpen = it.toKotlinDuration() }
            }
        }
    }

private fun policyFor(kind: HostKeyKind, knownHosts: Optional<Path>): HostKeyPolicy = when (kind) {
    // A strict policy with no file named reaches the transport as the empty path, which is not a
    // file of recorded keys and cannot be one, so the first handshake fails and the deployment
    // stops. Standing in a location nobody named - the account's own ~/.ssh/known_hosts, say -
    // would be worse: it is the one substitution that could silently succeed against the wrong
    // recorded key.
    HostKeyKind.STRICT -> HostKeyPolicy.Strict(knownHosts.orElse(Path.of("")))
    HostKeyKind.ACCEPT_ALL -> HostKeyPolicy.AcceptAll
}

/** Null when the host said nothing about this action, so the builder's own default stands. */
private fun actionFrom(action: SftpConnectorProperties.ActionProperties): PostAction? =
    when (action.kind().orElse(null)) {
        null -> null
        ActionKind.NOOP -> PostAction.Noop
        ActionKind.DELETE -> PostAction.Delete
        // An absent target is passed through as the empty string rather than guessed at: the
        // builder reads it as an action that names no folder to move files to, and says so.
        ActionKind.MOVE -> PostAction.Move(action.target().orElse(""), action.overwrite())
    }

/** Null when the host named no check at all, so the connector's own heuristic stands. */
private fun PollingBuilder.readinessFrom(described: SftpConnectorProperties.ReadinessProperties): ReadinessCheck? =
    listOfNotNull(
        described.sizeStableChecks().map { sizeStable(it, described.sizeStableInterval().toKotlinDuration()) }
            .orElse(null),
        described.minAge().map { minAge(it.toKotlinDuration()) }.orElse(null),
        described.markerFile().map { markerFile(it) }.orElse(null),
    ).reduceOrNull { checked, next -> checked + next }
