package sftp.connector.config

import org.slf4j.LoggerFactory
import sftp.connector.client.Overwrite
import sftp.connector.error.ConfigurationError
import sftp.connector.source.MarkerFile
import sftp.connector.source.MinAge
import sftp.connector.source.ReadinessCheck
import sftp.connector.source.SizeStable
import sftp.connector.source.plus
import java.nio.file.Files
import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

@DslMarker
annotation class SftpDsl

/**
 * Describes one connector and hands back an immutable configuration, or refuses.
 *
 * ```
 * val config = sftpConnector("vendor-drop") {
 *     endpoint { host = "sftp.example"; proxy { httpConnect("proxy.internal", 3128) } }
 *     auth { password("etl", secret) }
 *     hostKey = HostKeyPolicy.Strict(Path("/etc/etl/known_hosts"))
 * }
 * ```
 *
 * Every value is checked here, and all the faults found are reported in one exception rather
 * than one per attempt, so a misconfigured deployment is corrected in a single pass.
 */
fun sftpConnector(name: String, configure: SftpConnectorBuilder.() -> Unit): SftpConnectorConfig =
    SftpConnectorBuilder(name).apply(configure).build()

@SftpDsl
class SftpConnectorBuilder internal constructor(private val name: String) {

    private var endpoint: EndpointBuilder? = null
    private var auth: AuthBuilder? = null
    private val pool = PoolBuilder()
    private val polling = PollingBuilder()
    private val resilience = ResilienceBuilder()

    /** Required. There is deliberately no default; see [HostKeyPolicy]. */
    var hostKey: HostKeyPolicy? = null

    fun endpoint(configure: EndpointBuilder.() -> Unit) {
        endpoint = EndpointBuilder().apply(configure)
    }

    fun auth(configure: AuthBuilder.() -> Unit) {
        auth = AuthBuilder().apply(configure)
    }

    fun pool(configure: PoolBuilder.() -> Unit) {
        pool.apply(configure)
    }

    fun polling(configure: PollingBuilder.() -> Unit) {
        polling.apply(configure)
    }

    fun resilience(configure: ResilienceBuilder.() -> Unit) {
        resilience.apply(configure)
    }

    internal fun build(): SftpConnectorConfig {
        val faults = mutableListOf<String>()

        // Not "tags every metric and log line": meters are tagged by endpoint (spec 13) and most
        // lines name a path and a server rather than a connector. The name is what a start-up
        // refusal, a shutdown and every probe message are addressed to, which is where it is read.
        if (name.isBlank()) faults += "the connector has no name, and its name is how its start-up, shutdown and probe messages say which connector they are about"

        val describedEndpoint = endpoint
        if (describedEndpoint == null) {
            faults += "no endpoint block, so the connector has no server to reach"
        } else {
            faults.checkAddress("endpoint", describedEndpoint.host, describedEndpoint.port)
            val proxy = describedEndpoint.configuredProxy
            if (proxy != null) {
                faults.checkAddress("proxy", proxy.host, proxy.port)
            } else if (describedEndpoint.proxyBlockOpened) {
                // An empty proxy block reads as "there is a proxy" and would otherwise mean the
                // opposite. On a network that only reaches the server through one, connecting
                // direct fails at deployment rather than here.
                faults += "the proxy block names no proxy: call httpConnect(host, port) inside it, or drop the block"
            }
        }

        val credential = auth?.method
        if (credential == null) faults += "no auth block, so the connector has no credential to log in with"
        if (credential is AuthMethod.Password && credential.user.isBlank()) faults += "auth user is blank"

        val policy = hostKey
        if (policy == null) {
            faults += "hostKey is unset: choose Strict(knownHosts) to verify the server, " +
                "or AcceptAll to accept any key and the impersonation risk that comes with it"
        }

        if (pool.maxSize < 1) faults += "pool maxSize ${pool.maxSize} leaves the connector no session to work with"
        // Every duration below is a length of time the connector waits or allows, and zero or less
        // is not a shorter one: a zero acquire timeout refuses every caller that did not find a
        // session already free, and a zero lifetime retires each session the moment it opens. Both
        // read in a log as a pool that is broken rather than one that is busy.
        listOf(
            "acquireTimeout" to pool.acquireTimeout,
            "connectTimeout" to pool.connectTimeout,
            "keepAlive" to pool.keepAlive,
            "cancelGrace" to pool.cancelGrace,
            "drainTimeout" to pool.drainTimeout,
            "idleTimeout" to pool.idleTimeout,
            "idleCutoff" to pool.idleCutoff,
            "maxLifetime" to pool.maxLifetime,
            "leakDetectionThreshold" to pool.leakDetectionThreshold,
            "housekeepingInterval" to pool.housekeepingInterval,
        ).forEach { (knob, value) ->
            if (value <= Duration.ZERO) faults += "pool $knob must be positive, not $value"
        }
        // The one duration left out above. Zero is the reading "prove every session before lending
        // it", which is a choice someone may want. Below zero is not a shorter window, it is no
        // reading at all.
        if (pool.validationBypass < Duration.ZERO) {
            faults += "pool validationBypass cannot be negative, and ${pool.validationBypass} is"
        }
        if (pool.maxLifetimeJitter !in 0.0..1.0) {
            faults += "pool maxLifetimeJitter ${pool.maxLifetimeJitter} is outside 0.0..1.0, so a session's " +
                "lifetime would be shorter than maxLifetime or more than twice it"
        }
        // Closing waits the drain and then one grace for what it had to cut, so a drain shorter
        // than the grace is a shutdown that gives up on its sessions before it has given them the
        // chance it promised.
        if (pool.drainTimeout <= pool.cancelGrace) {
            faults += "pool drainTimeout ${pool.drainTimeout} must be longer than cancelGrace ${pool.cancelGrace}, " +
                "since a closing connector waits the drain first and gives the sessions it had to cut one grace after it"
        }
        if (pool.minIdle < 0) faults += "pool minIdle ${pool.minIdle} is not a number of sessions"
        if (pool.minIdle > pool.maxSize) {
            faults += "pool minIdle ${pool.minIdle} is more than maxSize ${pool.maxSize}, so the pool would " +
                "spend its life trying to reach a number of idle sessions it is not allowed to hold"
        }
        // The two below are the same rule read twice, and both are about the shortest patience on
        // the network path. Something has to happen on a session before that runs out: either the
        // connector speaks, or it lets go of a session it would otherwise hand to a caller as
        // healthy after the path had already dropped it.
        if (pool.keepAlive >= pool.idleCutoff) {
            faults += "pool keepAlive ${pool.keepAlive} is not shorter than idleCutoff ${pool.idleCutoff}, so a " +
                "session would go quiet for longer than the path allows and be cut while the pool still counted on it"
        }
        if (pool.idleTimeout >= pool.idleCutoff) {
            faults += "pool idleTimeout ${pool.idleTimeout} is not shorter than idleCutoff ${pool.idleCutoff}, so " +
                "the pool would keep parked sessions the path had already dropped"
        }

        polling.watched.forEachIndexed { position, directory ->
            if (directory.isBlank()) faults += "polling directory ${position + 1} of ${polling.watched.size} is blank"
        }
        // Zero of either is a poll that can never hand over a file, which reads in a log as a
        // directory that is always empty rather than as a knob that was set wrong.
        if (polling.maxInFlight < 1) faults += "polling maxInFlight ${polling.maxInFlight} would let no file be handed over"
        if (polling.maxFilesPerPoll < 1) faults += "polling maxFilesPerPoll ${polling.maxFilesPerPoll} would read no entries"
        // An action that files a message back into the folder it came out of would hand the same
        // file to the next poll, and the poll after that, for as long as the connector runs. The
        // check is per watched directory because a relative target resolves against each of them.
        listOf("onAck" to polling.onAck, "onNack" to polling.onNack).forEach { (knob, action) ->
            val move = action as? PostAction.Move ?: return@forEach
            // Nothing left once the separators and the here-and-above dots come off means the
            // target names no folder at all. "." is the worst of those, because it reads like a
            // path and resolves onto the watched directory itself, which the check below would
            // then miss - the two spellings are not equal as strings.
            if (move.target.trim('/', '.', ' ').isEmpty()) {
                faults += "polling $knob moves files to \"${move.target}\", which names no folder to move them to"
                return@forEach
            }
            polling.watched.filter { it.isNotBlank() }.forEach { directory ->
                val target = move.targetUnder(directory)
                if (target == directory.trimEnd('/')) {
                    faults += "polling $knob moves files to $target, which is the directory they were watched in, " +
                        "so acting on a file would move it onto itself and every later poll would find it again"
                }
            }
        }

        val retry = resilience.retry
        if (retry.maxAttempts < 1) faults += "retry maxAttempts ${retry.maxAttempts} would send nothing at all"
        if (retry.backoff.initial <= Duration.ZERO) faults += "retry backoff initial ${retry.backoff.initial} must be positive"
        if (retry.backoff.max < retry.backoff.initial) {
            faults += "retry backoff max ${retry.backoff.max} is shorter than its initial ${retry.backoff.initial}"
        }
        val breaker = resilience.circuitBreaker
        if (breaker.failureRateThreshold !in 1..100) {
            faults += "circuitBreaker failureRateThreshold ${breaker.failureRateThreshold} is outside 1..100"
        }
        if (breaker.slidingWindow < 1) faults += "circuitBreaker slidingWindow ${breaker.slidingWindow} holds no calls to judge by"
        if (breaker.waitInOpen <= Duration.ZERO) faults += "circuitBreaker waitInOpen must be positive, not ${breaker.waitInOpen}"
        // Unset, it is four, or the whole pool when the pool is smaller than that: the number
        // is a share of the pool, and a pool of one has nothing to leave for the lister.
        val transfers = resilience.bulkhead.maxConcurrentTransfers ?: minOf(DEFAULT_CONCURRENT_TRANSFERS, pool.maxSize)
        if (transfers < 1) faults += "bulkhead maxConcurrentTransfers $transfers would let no file move"
        if (transfers > pool.maxSize) {
            faults += "bulkhead maxConcurrentTransfers $transfers is more than pool maxSize ${pool.maxSize}, so that many " +
                "could never run at once"
        }
        // The clock on a call starts before its session is borrowed, so a limit that can run out
        // while the caller is still queued reports a full pool as a server that stopped answering
        // - and counts it against the breaker, which is the one thing a full pool must never do.
        listOf("operationTimeout" to resilience.operationTimeout, "transferTimeout" to resilience.transferTimeout)
            .forEach { (knob, value) ->
                if (value <= pool.acquireTimeout) {
                    faults += "resilience $knob $value is not longer than pool acquireTimeout ${pool.acquireTimeout}, so a " +
                        "caller queued for a session would be reported as the server timing out"
                }
            }

        // Checked here rather than at the first download, because a staging directory that is
        // missing or read-only makes every download fail and the fault is the same one every time.
        // Finding that at deployment costs a restart; finding it an hour into a run costs the run.
        val stagingDir = polling.staging.dir
        if (!Files.isDirectory(stagingDir)) {
            faults += "staging dir $stagingDir is not a directory that exists, so downloads would have nowhere to land"
        } else if (!Files.isWritable(stagingDir)) {
            faults += "staging dir $stagingDir cannot be written to by this process, so no download could be staged in it"
        }

        if (faults.isNotEmpty()) {
            throw ConfigurationError("connector \"$name\" cannot start: ${faults.joinToString("; ")}")
        }

        // Past that throw the three required blocks are present, because their absence is on
        // the fault list.
        checkNotNull(describedEndpoint)
        checkNotNull(credential)
        checkNotNull(policy)

        val resolvedEndpoint =
            Endpoint(describedEndpoint.host, describedEndpoint.port, describedEndpoint.configuredProxy)

        if (policy is HostKeyPolicy.AcceptAll) {
            LOG.warn(
                "Connector \"{}\" accepts any host key from {}:{}. The server's identity is never " +
                    "verified, so anything on the network path can impersonate it. Record the " +
                    "server's key and switch to a strict policy when you can.",
                name,
                resolvedEndpoint.host,
                resolvedEndpoint.port,
            )
        }

        return SftpConnectorConfig(
            name = name,
            endpoint = resolvedEndpoint,
            auth = credential,
            hostKey = policy,
            pool = PoolConfig(
                maxSize = pool.maxSize,
                minIdle = pool.minIdle,
                acquireTimeout = pool.acquireTimeout,
                connectTimeout = pool.connectTimeout,
                keepAlive = pool.keepAlive,
                cancelGrace = pool.cancelGrace,
                drainTimeout = pool.drainTimeout,
                idleTimeout = pool.idleTimeout,
                idleCutoff = pool.idleCutoff,
                maxLifetime = pool.maxLifetime,
                maxLifetimeJitter = pool.maxLifetimeJitter,
                validationBypass = pool.validationBypass,
                leakDetectionThreshold = pool.leakDetectionThreshold,
                housekeepingInterval = pool.housekeepingInterval,
            ),
            polling = PollingConfig(
                directories = polling.watched.toList(),
                onAck = polling.onAck,
                onNack = polling.onNack,
                createActionTargets = polling.createActionTargets,
                startupProbe = polling.startupProbe,
                staging = StagingConfig(dir = polling.staging.dir, digest = polling.staging.digest),
                maxInFlight = polling.maxInFlight,
                maxFilesPerPoll = polling.maxFilesPerPoll,
                recursive = polling.recursive,
                readiness = polling.readiness,
                overlap = polling.overlap,
            ),
            resilience = ResilienceConfig(
                retry = RetryPolicy(retry.maxAttempts, retry.backoff),
                circuitBreaker = BreakerPolicy(breaker.failureRateThreshold, breaker.slidingWindow, breaker.waitInOpen),
                maxConcurrentTransfers = transfers,
                operationTimeout = resilience.operationTimeout,
                transferTimeout = resilience.transferTimeout,
            ),
        )
    }

    private fun MutableList<String>.checkAddress(what: String, host: String, port: Int) {
        if (host.isBlank()) this += "$what host is blank"
        if (port !in PORTS) this += "$what port $port is outside $PORTS"
    }

    private companion object {
        private val LOG = LoggerFactory.getLogger(SftpConnectorBuilder::class.java)
        private val PORTS = 1..65535
        private const val DEFAULT_CONCURRENT_TRANSFERS = 4
    }
}

@SftpDsl
class EndpointBuilder internal constructor() {
    var host: String = ""
    var port: Int = 22

    internal var configuredProxy: HttpConnectProxy? = null
    internal var proxyBlockOpened: Boolean = false
        private set

    fun proxy(configure: ProxyBuilder.() -> Unit) {
        proxyBlockOpened = true
        configuredProxy = ProxyBuilder().apply(configure).configured
    }
}

@SftpDsl
class ProxyBuilder internal constructor() {
    internal var configured: HttpConnectProxy? = null

    fun httpConnect(host: String, port: Int) {
        configured = HttpConnectProxy(host, port)
    }
}

@SftpDsl
class AuthBuilder internal constructor() {
    internal var method: AuthMethod? = null

    fun password(user: String, secret: String) {
        method = AuthMethod.Password(user, secret)
    }
}

/**
 * Defaults sized for the network this connector was written for: a proxy that drops idle
 * tunnels after five minutes, and an infrastructure team that allows five concurrent sessions.
 */
@SftpDsl
class PoolBuilder internal constructor() {
    var maxSize: Int = 5
    var minIdle: Int = 0
    var acquireTimeout: Duration = 30.seconds
    var connectTimeout: Duration = 10.seconds
    var keepAlive: Duration = 30.seconds
    var cancelGrace: Duration = 5.seconds
    var drainTimeout: Duration = 30.seconds
    var idleTimeout: Duration = 4.minutes
    var idleCutoff: Duration = 5.minutes
    var maxLifetime: Duration = 30.minutes
    var maxLifetimeJitter: Double = 0.1
    var validationBypass: Duration = 500.milliseconds
    var leakDetectionThreshold: Duration = 10.minutes
    var housekeepingInterval: Duration = 30.seconds
}

@SftpDsl
class PollingBuilder internal constructor() {

    internal val staging = StagingBuilder()

    internal val watched = mutableListOf<String>()

    /** What becomes of a file the consumer has finished with. Defaults leave the server alone. */
    var onAck: PostAction = PostAction.Noop
    var onNack: PostAction = PostAction.Noop

    /**
     * On by default, because a connector configured to move files into a folder and refusing to
     * make it is a connector that fails on its first ack over something it could have arranged.
     * An account not allowed to create directories turns it off and has them made upstream.
     */
    var createActionTargets: Boolean = true

    /**
     * On by default. The alternative to checking whether a move works at start-up is discovering
     * it at the first file, which on an hourly pipeline can be an hour after anyone was watching.
     */
    var startupProbe: Boolean = true

    var maxInFlight: Int = 16
    var maxFilesPerPoll: Int = 1000
    var recursive: Boolean = false

    /** Whether a tick that finds the last one still running waits it out or runs alongside it. */
    var overlap: OverlapPolicy = OverlapPolicy.SKIP

    /**
     * The default is a heuristic, and an honest one: a file whose size has held still for ten
     * seconds and that nobody has touched for a minute is probably finished. An uploader that
     * stalls mid-file passes it. The only check that cannot be fooled is a marker the uploader
     * writes when it is done, which needs the uploader's cooperation - ask for it.
     */
    var readiness: ReadinessCheck = sizeStable(checks = 2, interval = 10.seconds) + minAge(1.minutes)

    /**
     * The directories this connector takes files from. Naming them here is what lets start-up
     * check them; the call that starts a poll names one of them again.
     */
    fun directories(vararg paths: String) {
        watched += paths
    }

    /** Ready once the size has been seen unchanged [checks] times, at least [interval] apart. */
    fun sizeStable(checks: Int, interval: Duration): ReadinessCheck = SizeStable(checks, interval)

    /** Ready once the file was last modified at least [duration] ago. */
    fun minAge(duration: Duration): ReadinessCheck = MinAge(duration)

    /** Ready once `<name><suffix>` exists beside the file; the markers themselves are never handed over. */
    fun markerFile(suffix: String): ReadinessCheck = MarkerFile(suffix)

    /** Moves the file into [target]. See [PostAction.Move] for where a relative target lands. */
    fun move(target: String, overwrite: Overwrite = Overwrite.REFUSE): PostAction = PostAction.Move(target, overwrite)

    fun delete(): PostAction = PostAction.Delete

    fun noop(): PostAction = PostAction.Noop

    fun staging(configure: StagingBuilder.() -> Unit) {
        staging.apply(configure)
    }
}

/**
 * Defaults for a flaky network in front of a healthy server: three tries a call, a breaker that
 * opens when half of the last twenty calls failed and probes again after a minute, four transfers
 * at once out of a pool of five.
 */
@SftpDsl
class ResilienceBuilder internal constructor() {

    internal val retry = RetryBuilder()
    internal val circuitBreaker = CircuitBreakerBuilder()
    internal val bulkhead = BulkheadBuilder()

    /** For one try at a single round trip, borrowing the session included. */
    var operationTimeout: Duration = 1.minutes

    /** For one try at moving one whole file, or at a listing read at the consumer's pace. */
    var transferTimeout: Duration = 15.minutes

    fun retry(configure: RetryBuilder.() -> Unit) {
        retry.apply(configure)
    }

    fun circuitBreaker(configure: CircuitBreakerBuilder.() -> Unit) {
        circuitBreaker.apply(configure)
    }

    fun bulkhead(configure: BulkheadBuilder.() -> Unit) {
        bulkhead.apply(configure)
    }
}

@SftpDsl
class RetryBuilder internal constructor() {
    /** Tries in total, the first included. */
    var maxAttempts: Int = 3
    var backoff: Backoff = exponential(1.seconds, max = 30.seconds, jitter = true)

    fun exponential(initial: Duration, max: Duration, jitter: Boolean = true): Backoff = Backoff(initial, max, jitter)
}

@SftpDsl
class CircuitBreakerBuilder internal constructor() {
    /** Percent of the last [slidingWindow] calls that have to fail before the breaker opens. */
    var failureRateThreshold: Int = 50
    var slidingWindow: Int = 20
    var waitInOpen: Duration = 1.minutes
}

@SftpDsl
class BulkheadBuilder internal constructor() {
    /** Unset means four, or as many as the pool holds when that is fewer. */
    var maxConcurrentTransfers: Int? = null
}

@SftpDsl
class StagingBuilder internal constructor() {
    /**
     * The default is the JVM's temp directory, because it is the one local directory that exists
     * and is writable everywhere the connector can run, so a connector nobody has finished
     * configuring still works. A deployment names its own, on a filesystem sized for the files it
     * pulls and cleaned by someone who knows they are there.
     */
    var dir: Path = Path.of(System.getProperty("java.io.tmpdir"))
    var digest: Digest = Digest.SHA256
}
