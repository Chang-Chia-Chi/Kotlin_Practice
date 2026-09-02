package sftp.connector.transport.jsch

import com.jcraft.jsch.ChannelSftp
import com.jcraft.jsch.JSch
import com.jcraft.jsch.ProxyHTTP
import com.jcraft.jsch.Session
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import sftp.connector.config.AuthMethod
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.transport.SftpConnection
import sftp.connector.transport.SftpTransport
import kotlin.time.Duration

/**
 * The transport as JSch implements it, and the only place in the connector that names a JSch
 * type. Everything JSch needs told - the proxy tunnel, the host key policy, the socket timeout,
 * the keepalive that has to fire before the proxy gives up on an idle tunnel - is applied here
 * from the connector's own configuration.
 */
class JschTransport(private val config: SftpConnectorConfig) : SftpTransport {

    /**
     * JSch blocks, so its calls run here instead of on the caller's thread. The width matches
     * the pool, which is the most sessions that can ever exist at once: a server that stops
     * answering can pin these threads and no more, and the rest of the host's IO threads stay
     * free for everything else the service is doing.
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private val io: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(config.pool.maxSize)

    override suspend fun connect(): SftpConnection = withContext(io) {
        val session = openSession()
        val channel = try {
            (session.openChannel("sftp") as ChannelSftp).also { it.connect(config.pool.connectTimeout.toTimeoutMillis()) }
        } catch (failure: Throwable) {
            // A session whose channel never opened is unusable, and left alone it would keep its
            // socket and its reader thread for the life of the process.
            session.disconnect()
            throw failure
        }
        JschConnection(session, channel, io)
    }

    private fun openSession(): Session {
        val jsch = JSch()
        val endpoint = config.endpoint
        val session = when (val credential = config.auth) {
            is AuthMethod.Password ->
                jsch.getSession(credential.user, endpoint.host, endpoint.port)
                    // The bytes, not the String: JSch deprecated taking a String because it
                    // then has to guess an encoding.
                    .apply { setPassword(credential.secret.toByteArray()) }
        }

        when (val policy = config.hostKey) {
            is HostKeyPolicy.Strict -> {
                jsch.setKnownHosts(policy.knownHosts.toString())
                session.setConfig("StrictHostKeyChecking", "yes")
            }

            HostKeyPolicy.AcceptAll -> session.setConfig("StrictHostKeyChecking", "no")
        }

        endpoint.proxy?.let { session.setProxy(ProxyHTTP(it.host, it.port)) }

        // Set before connecting, because that is when JSch reads it. It becomes the socket's
        // read timeout, and so the only thing that ever unblocks a server which accepts a
        // request and then goes quiet: a blocked socket read notices neither an interrupted
        // thread nor a cancelled coroutine.
        session.timeout = config.pool.socketTimeout.toTimeoutMillis()
        session.serverAliveInterval = config.pool.keepAlive.toTimeoutMillis()
        session.connect(config.pool.connectTimeout.toTimeoutMillis())
        return session
    }
}

private class JschConnection(
    private val session: Session,
    private val channel: ChannelSftp,
    private val io: CoroutineDispatcher,
) : SftpConnection {

    override suspend fun realpath(path: String): String = withContext(io) { channel.realpath(path) }

    /**
     * Uncancellable on purpose. A session left half-closed keeps its socket and its reader
     * thread until the process ends, and a caller being cancelled is exactly the moment that is
     * most likely to happen.
     */
    override suspend fun close(): Unit = withContext(io + NonCancellable) {
        channel.disconnect()
        session.disconnect()
    }
}

/** JSch counts timeouts in milliseconds as an `Int`, and treats a negative one as an error. */
private fun Duration.toTimeoutMillis(): Int =
    inWholeMilliseconds.coerceIn(0L, Int.MAX_VALUE.toLong()).toInt()
