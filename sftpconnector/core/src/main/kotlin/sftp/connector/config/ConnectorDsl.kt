package sftp.connector.config

import org.slf4j.LoggerFactory
import sftp.connector.error.ConfigurationError
import kotlin.time.Duration
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

    internal fun build(): SftpConnectorConfig {
        val faults = mutableListOf<String>()

        if (name.isBlank()) faults += "the connector has no name, and its name tags every metric and log line it produces"

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
        if (pool.connectTimeout <= Duration.ZERO) faults += "pool connectTimeout must be positive, not ${pool.connectTimeout}"
        if (pool.socketTimeout <= Duration.ZERO) faults += "pool socketTimeout must be positive, not ${pool.socketTimeout}"
        if (pool.keepAlive <= Duration.ZERO) faults += "pool keepAlive must be positive, not ${pool.keepAlive}"

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
                connectTimeout = pool.connectTimeout,
                socketTimeout = pool.socketTimeout,
                keepAlive = pool.keepAlive,
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
    var connectTimeout: Duration = 10.seconds
    var socketTimeout: Duration = 60.seconds
    var keepAlive: Duration = 30.seconds
}
