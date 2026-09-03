package sftp.connector.config

import sftp.connector.client.Overwrite
import sftp.connector.source.ReadinessCheck
import java.nio.file.Path
import kotlin.time.Duration

/**
 * Everything one connector needs to reach one SFTP server, frozen. Produced only by
 * [sftpConnector], which is where the values are checked, so a config that exists at all is a
 * config that passed validation.
 */
data class SftpConnectorConfig(
    val name: String,
    val endpoint: Endpoint,
    val auth: AuthMethod,
    val hostKey: HostKeyPolicy,
    val pool: PoolConfig,
    val polling: PollingConfig,
    val resilience: ResilienceConfig,
)

/** Where the server is, and how the connector gets onto its network. */
data class Endpoint(
    val host: String,
    val port: Int,
    val proxy: HttpConnectProxy? = null,
) {
    /**
     * How this server is named in a log line, a failure and a metric tag. Spelled once, because a
     * service with two connectors tells them apart by this string, and two spellings of it would
     * split one server's numbers across two series on a dashboard.
     */
    val address: String get() = "$host:$port"
}

/** An HTTP proxy that tunnels the SSH connection with a CONNECT request. */
data class HttpConnectProxy(val host: String, val port: Int)

sealed interface AuthMethod {
    /**
     * Not a data class: the generated `toString` would print the secret into the first log line
     * that mentions the configuration.
     */
    class Password(val user: String, val secret: String) : AuthMethod {
        override fun toString(): String = "Password(user=$user, secret=***)"
    }
}

/**
 * What the connector does when the server presents its host key. There is no default: an
 * operator who wants no verification has to say so, and then hears about it.
 */
sealed interface HostKeyPolicy {
    /** Accept only a key already recorded in [knownHosts]. */
    data class Strict(val knownHosts: Path) : HostKeyPolicy

    /**
     * Accept whatever key arrives. Anyone able to sit on the network path can then impersonate
     * the server, which is why choosing this is loud rather than silent.
     */
    data object AcceptAll : HostKeyPolicy
}

/** How the pool is sized, how long its sessions live, and how it keeps them alive. */
data class PoolConfig(
    val maxSize: Int,
    /**
     * How many open sessions the pool keeps ready even when nobody is asking. Sessions cost a
     * handshake, so a job that polls on a timer pays for one every tick unless something holds
     * them open between ticks. Zero means the pool opens only what callers ask for.
     */
    val minIdle: Int,
    /**
     * How long a caller waits at the door before being told the pool is full. It bounds the
     * caller, not the server: a poll that would otherwise queue behind work that is not
     * finishing gives up, reports why, and lets the next tick start over.
     */
    val acquireTimeout: Duration,
    val connectTimeout: Duration,
    /**
     * How often a session with nothing to say speaks anyway. It has to be short enough that the
     * proxy and the server never see the tunnel go quiet for as long as they are willing to wait.
     *
     * It is also, and less obviously, **the number an SLA has to be sized against**. The SSH
     * library implements the keepalive by making it the socket's read timeout and giving up once
     * one probe has gone unanswered, so a call against a server that accepted a request and then
     * went quiet ends after twice this - and nothing else ever ends it, since a blocked socket
     * read notices neither an interrupted thread nor a cancelled coroutine.
     */
    val keepAlive: Duration,
    /**
     * How long a caller that has been cancelled waits for the call it left behind to stop by
     * itself, before the session carrying it is destroyed to get the thread back. It is the price
     * of a cancellation that nothing gentler reaches: one handshake, paid to put a bound on
     * something that otherwise has none.
     */
    val cancelGrace: Duration,
    /**
     * How long a closing connector waits for the sessions that are out on lease to come back on
     * their own before it cuts them apart. Closing is bounded by this plus one [cancelGrace] -
     * the grace is what the cut calls are given to hand their sessions back - so this must be
     * the longer of the two. At the file sizes in scope a transfer is not worth an unbounded
     * shutdown; one that outlasts the drain is cut and its partial file removed.
     */
    val drainTimeout: Duration,
    /** How long a session may sit unused before the pool hangs up on it, down to [minIdle]. */
    val idleTimeout: Duration,
    /**
     * The shortest time anything on the network path tolerates a silent connection: the proxy
     * here, five minutes. Everything the connector does on an idle session has to happen inside
     * it, which is why it is a configured fact rather than a knob to tune.
     */
    val idleCutoff: Duration,
    /**
     * How long a session is used before it is retired healthy. Long-lived sessions accumulate
     * everything nobody thought about - a server rotating keys, a proxy restarting, a firewall
     * forgetting the flow - and a session replaced on a schedule fails at a moment of the
     * pool's choosing rather than in the middle of a caller's work.
     */
    val maxLifetime: Duration,
    /**
     * Spreads the retirements. Each session gets its own lifetime, uniformly somewhere in
     * `[maxLifetime, maxLifetime x (1 + this)]`, so a pool that filled in one burst at startup
     * does not empty in one burst half an hour later.
     */
    val maxLifetimeJitter: Double,
    /**
     * How long a session may have been sitting before the pool asks the server whether it is
     * still there. A session handed straight back to the next caller was proved good moments
     * ago; one that has been parked may have been dropped by anything on the path without
     * either end noticing. Zero asks every time.
     */
    val validationBypass: Duration,
    /** How long a caller may hold a session before the pool reports where it was taken. */
    val leakDetectionThreshold: Duration,
    /** How often the pool looks over what it holds and retires, reports and refills. */
    val housekeepingInterval: Duration,
)

/** What the connector does with a watched directory, and where the files it takes from one go. */
data class PollingConfig(
    /**
     * The directories this connector takes files from. They are named here rather than only at the
     * call that starts watching one, because the connector has to be able to check them before it
     * is asked to do anything: a directory that is not there, or an action target on the wrong
     * disk, is a fault worth hearing about at start-up rather than at the first file an hour later.
     */
    val directories: List<String>,
    /** What happens to a file once the consumer says it is done with it. */
    val onAck: PostAction,
    /** What happens to a file the consumer says it could not process. */
    val onNack: PostAction,
    /**
     * Whether the connector creates the folders its actions move files into. Off for an account
     * that is not allowed to create directories; the folders then have to exist already, and the
     * start-up check says so if they do not.
     */
    val createActionTargets: Boolean,
    /**
     * Whether start-up moves a marker file into each action target and back. It is the only check
     * that proves a move will actually work, and it is a knob because it writes to the server:
     * an account that is watched, audited or simply not welcome to leave files in a folder turns
     * it off and accepts finding out at the first ack instead.
     */
    val startupProbe: Boolean,
    val staging: StagingConfig,
    /**
     * How many files may be handed to the consumer and not yet acked or nacked, across every
     * directory this connector polls. Once that many are out, the listing waits for one to come
     * back before handing over the next. It is the one knob that protects whatever is downstream
     * from a directory that filled up while nobody was polling.
     */
    val maxInFlight: Int,
    /** How many entries one poll reads before stopping, however many more the directory holds. */
    val maxFilesPerPoll: Int,
    /**
     * Whether a poll walks into subdirectories. The folders its own actions move files into are
     * left out of the walk whatever this says, so a file that has been dealt with is never found
     * again by the poll that dealt with it.
     */
    val recursive: Boolean,
    /**
     * What a listed file has to pass before it is handed over. A check may keep memory between
     * polls - watching a size hold still is only possible by remembering it - so the instance here
     * belongs to this one connector.
     */
    val readiness: ReadinessCheck,
    /** What a watch's tick does when the tick before it is still running. */
    val overlap: OverlapPolicy,
) {
    /**
     * The folders files from [directory] are moved into, once, however many actions aim at the
     * same one.
     */
    fun actionTargetsUnder(directory: String): List<String> =
        listOf(onAck, onNack)
            .filterIsInstance<PostAction.Move>()
            .map { it.targetUnder(directory) }
            .distinct()
}

/**
 * What a watch does when its interval comes round and the previous tick has not finished - a
 * long listing, a slow readiness check, or a consumer still working through the last batch.
 */
enum class OverlapPolicy {
    /**
     * Report the tick as skipped and let the running one finish. The default, because two
     * listings of one directory at once buy nothing: the running tick is already handing over
     * everything it found, and a file it holds would not be handed over twice anyway.
     */
    SKIP,

    /**
     * Start the tick alongside the running one. A file the first tick is still holding is not
     * handed over again; what a second tick can add is files that arrived since the first listed.
     */
    PROCEED,
}

/**
 * What becomes of a file after the consumer has finished with it.
 *
 * Moving is the usual one, and it is also what makes the connector idempotent enough to be useful:
 * a file that is no longer in the watched directory is a file no later poll can hand out again.
 */
sealed interface PostAction {

    /**
     * Puts the file somewhere else on the same server.
     *
     * @param target where it goes. A path starting with `/` is that path on the server; anything
     *   else is a folder under the directory the file came from, so one connector watching several
     *   directories files each of them into its own. That is the layout the folder usually wants,
     *   and the lister knows to leave such a folder out of its own results.
     */
    data class Move(val target: String, val overwrite: Overwrite = Overwrite.REFUSE) : PostAction {

        /** Where this action puts a file taken from [directory]. */
        fun targetUnder(directory: String): String =
            if (target.startsWith("/")) target.trimEnd('/')
            else "${directory.trimEnd('/')}/${target.trim('/')}"
    }

    /** Removes the file. Nothing keeps a copy, so the pipeline downstream had better have one. */
    data object Delete : PostAction

    /**
     * Leaves the file where it is. The default, because moving or deleting a file on somebody
     * else's server is not something to do by inheriting a setting.
     */
    data object Noop : PostAction
}

/** Where downloads land, and how their bytes are summed up. */
data class StagingConfig(
    /**
     * Local disk, and the connector's own. It has to be a real filesystem: the download is
     * finished by moving a partial file onto its final name in one step, and a network filesystem
     * is where the meaning of moving and deleting a file stops being obvious.
     */
    val dir: Path,
    val digest: Digest,
)

/**
 * How the bytes of a downloaded file are summed up so the application can tell whether they
 * arrived intact.
 *
 * The choice exists because an upstream that publishes checksums has already picked one, and a
 * digest computed with a different algorithm than the expected value cannot be compared with it.
 */
enum class Digest(internal val algorithmName: String) {
    SHA256("SHA-256"),

    /** Weak against a forger, and still the only thing many upstreams publish. */
    MD5("MD5"),
}

/**
 * How the connector behaves when the server does not: how often it tries again, when it stops
 * trying for a while, how many transfers it runs at once, and how long it gives one call.
 */
data class ResilienceConfig(
    val retry: RetryPolicy,
    val circuitBreaker: BreakerPolicy,
    /**
     * How many downloads and uploads may be on the wire at once. It is below the pool's size on
     * purpose, so that a batch of transfers never takes every session and leaves the listing
     * with nothing to run on.
     */
    val maxConcurrentTransfers: Int,
    /**
     * How long one try at a single round trip may take, counted from before its session is
     * borrowed. It has to be longer than the wait for a session, or a busy pool would be reported
     * as a server that stopped answering.
     */
    val operationTimeout: Duration,
    /**
     * The same, for one try at something whose length the other end decides: moving a whole
     * file, or listing a directory at the pace its consumer reads it.
     */
    val transferTimeout: Duration,
)

/** How many tries one call gets, and how the waits between them grow. */
data class RetryPolicy(val maxAttempts: Int, val backoff: Backoff)

/**
 * Exponential: each wait is twice the one before, from [initial] up to [max]. With [jitter] each
 * wait is spread randomly around that, so callers that failed together do not all come back in
 * the same instant.
 */
data class Backoff(val initial: Duration, val max: Duration, val jitter: Boolean = true)

/**
 * When the connector stops sending to a server that keeps failing, and for how long. The breaker
 * opens once [failureRateThreshold] percent of the last [slidingWindow] calls failed, stays open
 * for [waitInOpen], then lets one call through to find out whether the server is back.
 */
data class BreakerPolicy(val failureRateThreshold: Int, val slidingWindow: Int, val waitInOpen: Duration)
