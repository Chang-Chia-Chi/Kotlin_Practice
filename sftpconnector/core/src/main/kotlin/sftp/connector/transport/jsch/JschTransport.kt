package sftp.connector.transport.jsch

import com.jcraft.jsch.ChannelSftp
import com.jcraft.jsch.JSch
import com.jcraft.jsch.ProxyHTTP
import com.jcraft.jsch.Session
import com.jcraft.jsch.SftpATTRS
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import sftp.connector.config.AuthMethod
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.error.Attempt
import sftp.connector.transport.Listing
import sftp.connector.transport.RemoteFile
import sftp.connector.transport.SftpConnection
import sftp.connector.transport.SftpTransport
import java.io.InputStream
import java.io.OutputStream
import java.time.Instant
import kotlin.time.Duration

/**
 * The transport as JSch implements it, and the only place in the connector that names a JSch
 * type. Everything JSch needs told - the proxy tunnel, the host key policy, the socket timeout,
 * the keepalive that has to fire before the proxy gives up on an idle tunnel - is applied here
 * from the connector's own configuration.
 *
 * Nothing JSch raises leaves this class: every call goes through the error mapper, so callers
 * above the transport see the connector's own failures and never a type from the SSH library.
 */
class JschTransport(
    private val config: SftpConnectorConfig,
    /** Whatever the host supplies; a private one when the connector is used on its own. */
    meters: MeterRegistry = SimpleMeterRegistry(),
) : SftpTransport {

    private val errors = JschErrorMapper(meters)

    private val endpointLabel = config.endpoint.address

    /**
     * JSch blocks, so its calls run here instead of on the caller's thread. The width matches
     * the pool, which is the most sessions that can ever exist at once: a server that stops
     * answering can pin these threads and no more, and the rest of the host's IO threads stay
     * free for everything else the service is doing.
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private val io: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(config.pool.maxSize)

    /**
     * The transport is told nothing about retries, so every failure it raises reports the first
     * attempt. The layer that decides to try again is the layer that knows which try this is.
     */
    override suspend fun connect(): SftpConnection = withContext(io) {
        errors.translating(Attempt(endpointLabel, "connect")) {
            val session = openSession()
            val channel = try {
                (session.openChannel("sftp") as ChannelSftp)
                    .also { it.connect(config.pool.connectTimeout.toTimeoutMillis()) }
            } catch (failure: Throwable) {
                // A session whose channel never opened is unusable, and left alone it would keep
                // its socket and its reader thread for the life of the process.
                session.disconnect()
                throw failure
            }
            JschConnection(session, channel, io, errors, endpointLabel)
        }
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
    private val errors: JschErrorMapper,
    private val endpoint: String,
) : SftpConnection {

    override suspend fun realpath(path: String): String = withContext(io) {
        errors.translating(Attempt(endpoint, "realpath", path)) { channel.realpath(path) }
    }

    /**
     * JSch hands each entry to a selector as the server's batches arrive, which is what makes a
     * hundred-thousand-entry directory cost the same memory as a ten-entry one. Answering BREAK
     * closes the remote handle cleanly, so the session is still good afterwards.
     */
    override suspend fun list(dir: String, onEntry: (RemoteFile) -> Listing): Unit = withContext(io) {
        errors.translating(Attempt(endpoint, "list", dir)) {
            channel.ls(dir) { entry ->
                when {
                    entry.filename == "." || entry.filename == ".." -> ChannelSftp.LsEntrySelector.CONTINUE
                    onEntry(entry.attrs.describe(dir.asDirectoryOf(entry.filename))) == Listing.CONTINUE ->
                        ChannelSftp.LsEntrySelector.CONTINUE

                    else -> ChannelSftp.LsEntrySelector.BREAK
                }
            }
        }
    }

    override suspend fun stat(path: String): RemoteFile = withContext(io) {
        errors.translating(Attempt(endpoint, "stat", path)) { channel.stat(path).describe(path) }
    }

    override suspend fun readTo(path: String, sink: OutputStream): Unit = withContext(io) {
        errors.translating(Attempt(endpoint, "read", path)) { channel.get(path, sink) }
    }

    override suspend fun writeFrom(path: String, source: InputStream): Unit = withContext(io) {
        errors.translating(Attempt(endpoint, "write", path)) {
            channel.put(source, path, ChannelSftp.OVERWRITE)
        }
    }

    /**
     * JSch sends this as the POSIX rename extension when the server advertised it during the
     * handshake and as a plain rename request otherwise, which is the whole of what the connector
     * has to do to get an atomic replacement where one is available. What the caller above sees is
     * the difference in the answer: a server without the extension refuses when the target is
     * occupied, and it refuses with the same generic status it uses for everything else it will
     * not do.
     */
    override suspend fun rename(from: String, to: String): Unit = withContext(io) {
        errors.translating(Attempt(endpoint, "rename", from)) { channel.rename(from, to) }
    }

    override suspend fun delete(path: String): Unit = withContext(io) {
        errors.translating(Attempt(endpoint, "delete", path)) { channel.rm(path) }
    }

    override suspend fun mkdir(path: String): Unit = withContext(io) {
        errors.translating(Attempt(endpoint, "mkdir", path)) { channel.mkdir(path) }
    }

    /**
     * Uncancellable on purpose. A session left half-closed keeps its socket and its reader
     * thread until the process ends, and a caller being cancelled is exactly the moment that is
     * most likely to happen.
     */
    override suspend fun close(): Unit = withContext(io + NonCancellable) {
        errors.translating(Attempt(endpoint, "close")) {
            try {
                channel.disconnect()
            } finally {
                // Whatever the channel did. A channel that would not close is no reason to leave
                // the session holding its socket and its reader thread for the life of the
                // process, which is the one thing closing exists to prevent.
                session.disconnect()
            }
        }
    }
}

/** JSch counts timeouts in milliseconds as an `Int`, and treats a negative one as an error. */
private fun Duration.toTimeoutMillis(): Int =
    inWholeMilliseconds.coerceIn(0L, Int.MAX_VALUE.toLong()).toInt()

/** Joins a directory to a name in it without doubling the separator a root path already ends with. */
private fun String.asDirectoryOf(name: String): String = if (endsWith("/")) "$this$name" else "$this/$name"

/**
 * SFTP version 3 counts modification times in whole seconds since the epoch, so a file written
 * twice within one second reports one time. Anything comparing mtimes has to allow for that.
 */
private fun SftpATTRS.describe(path: String) = RemoteFile(
    path = path,
    size = size,
    modifiedAt = Instant.ofEpochSecond(mTime.toLong()),
    isDirectory = isDir,
)
