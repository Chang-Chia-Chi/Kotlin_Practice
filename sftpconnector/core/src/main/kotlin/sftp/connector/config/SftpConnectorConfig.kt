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
)

/** Where the server is, and how the connector gets onto its network. */
data class Endpoint(
    val host: String,
    val port: Int,
    val proxy: HttpConnectProxy? = null,
)

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

/**
 * The session settings. Sizes and lifetimes join this as the pool that uses them is built;
 * what is here is what opening and holding a single session already needs.
 */
data class PoolConfig(
    val maxSize: Int,
    val connectTimeout: Duration,
    val socketTimeout: Duration,
    val keepAlive: Duration,
)
