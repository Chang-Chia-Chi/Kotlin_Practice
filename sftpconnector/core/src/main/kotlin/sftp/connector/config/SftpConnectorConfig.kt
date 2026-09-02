package sftp.connector.config

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
    val socketTimeout: Duration,
    /**
     * How often a session with nothing to say speaks anyway. It has to be short enough that the
     * proxy and the server never see the tunnel go quiet for as long as they are willing to
     * wait, and it is also what unblocks a read the server has stopped answering.
     */
    val keepAlive: Duration,
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
    val staging: StagingConfig,
)

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
