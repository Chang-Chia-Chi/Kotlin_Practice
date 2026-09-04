package sftp.connector.transport.jsch

import com.jcraft.jsch.ChannelSftp
import com.jcraft.jsch.JSch
import com.jcraft.jsch.ProxyHTTP
import com.jcraft.jsch.Session
import com.jcraft.jsch.SftpATTRS
import com.jcraft.jsch.SftpProgressMonitor
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.job
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import sftp.connector.config.AuthMethod
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.error.Attempt
import sftp.connector.error.ServerFailure
import sftp.connector.error.onOneLine
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
     *
     * Either returns a session the caller owns, or throws and the caller owns nothing. The
     * handshake blocks a thread and finishes whether or not the caller is still waiting for it,
     * and when the caller has been cancelled in the meantime the scope that carried the handshake
     * throws its result away on the way back to the caller's dispatcher - a session with a socket
     * and a reader thread that nothing above this method was ever handed and so nothing above it
     * could ever close. So the session is kept hold of here as well, and hung up on when it turns
     * out that nobody is left to hand it to. A caller cancelled before the handshake began never
     * starts one.
     */
    override suspend fun connect(): SftpConnection {
        var opened: SftpConnection? = null
        try {
            return withContext(io) {
                errors.translating(Attempt.inside(endpointLabel, "connect")) {
                    val session = openSession()
                    val channel = try {
                        (session.openChannel("sftp") as ChannelSftp)
                            .also { it.connect(config.pool.connectTimeout.toTimeoutMillis()) }
                    } catch (failure: Throwable) {
                        // A session whose channel never opened is unusable, and left alone it
                        // would keep its socket and its reader thread for the life of the process.
                        session.disconnect()
                        throw failure
                    }
                    JschConnection(session, channel, io, errors, endpointLabel)
                }.also { opened = it }
            }
        } catch (cancelled: CancellationException) {
            opened?.let { orphan ->
                try {
                    withContext(NonCancellable) { orphan.close() }
                } catch (failure: Exception) {
                    // The cancellation is what the caller is owed, and a hang-up that failed on a
                    // session being written off anyway is no reason to hand them something else.
                    LOG.warn("Hanging up a session nobody was left to receive failed, and it is being dropped anyway: {}", failure.message)
                }
            }
            throw cancelled
        }
    }

    private companion object {
        /**
         * How many keepalive probes may go unanswered before the session carrying them is given
         * up on. One, so that a server which has gone quiet is noticed after two intervals: the
         * shortest bound this mechanism can express, and the one every configuration gets, since
         * the interval is the knob and this is not.
         */
        private const val UNANSWERED_KEEPALIVES = 1

        private val LOG = LoggerFactory.getLogger(JschTransport::class.java)
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

        // Set before connecting, because that is when JSch reads them, and together because they
        // are one setting in two halves. JSch implements the keepalive interval *by* making it the
        // socket's read timeout, so a session has no separately settable read timeout at all: this
        // and the number of probes allowed to go unanswered are between them the only thing that
        // ever ends a call against a server which accepted a request and then went quiet. A
        // blocked socket read notices neither an interrupted thread nor a cancelled coroutine, so
        // the bound they set - one interval to send a probe, one more to give up on it - is the
        // number to size against an SLA. The count is pinned rather than left to the library,
        // because that bound is a promise this connector makes and not one it inherits.
        session.serverAliveInterval = config.pool.keepAlive.toTimeoutMillis()
        session.serverAliveCountMax = UNANSWERED_KEEPALIVES
        session.connect(config.pool.connectTimeout.toTimeoutMillis())
        return session
    }
}

internal class JschConnection(
    private val session: Session,
    private val channel: ChannelSftp,
    private val io: CoroutineDispatcher,
    private val errors: JschErrorMapper,
    private val endpoint: String,
) : SftpConnection {

    /**
     * The answer becomes the watched directory every later listing is asked for and every action
     * target is built under, so it is the one server-supplied string that is not a file name and
     * still ends up in front of a path join. A server that answers with something no path can hold
     * is refused here rather than have the connector spend the run quoting it back.
     */
    override suspend fun realpath(path: String): String = withContext(io) {
        val attempt = Attempt.inside(endpoint, "realpath", path)
        val resolved = errors.translating(attempt) { channel.realpath(path) }
        if (resolved.isEmpty() || resolved.any { it.cannotBeInAPath() }) {
            throw ServerFailure(
                attempt,
                ChannelSftp.SSH_FX_FAILURE,
                "the server resolved it to something no path can hold, so nothing was built on it: " +
                    "'${resolved.onOneLine()}'",
            )
        }
        resolved
    }

    /**
     * JSch hands each entry to a selector as the server's batches arrive, which is what makes a
     * hundred-thousand-entry directory cost the same memory as a ten-entry one. Answering BREAK
     * closes the remote handle cleanly, so the session is still good afterwards.
     */
    override suspend fun list(dir: String, onEntry: (RemoteFile) -> Listing): Unit = withContext(io) {
        errors.translating(Attempt.inside(endpoint, "list", dir)) {
            channel.ls(dir.literally()) { entry ->
                val path = dir.entryPathFor(entry.filename)
                when {
                    path == null -> ChannelSftp.LsEntrySelector.CONTINUE
                    onEntry(entry.attrs.describe(path)) == Listing.CONTINUE ->
                        ChannelSftp.LsEntrySelector.CONTINUE

                    else -> ChannelSftp.LsEntrySelector.BREAK
                }
            }
        }
    }

    /**
     * The path of one listed entry, or null when the server did not answer with a name.
     *
     * The connector checks a listed name before joining it to the *staging* directory and did not
     * check it before joining it to the *remote* one, and that asymmetry was the whole of the
     * defect: an entry called `../../../home/etl/.ssh/authorized_keys` became a path the source
     * quotes straight back as the source of a move and the argument of a delete, so a server that
     * named its entries badly could have the account's own files moved somewhere it can read them,
     * or unlinked. The move *target* was never at risk - it is built from the last segment - which
     * is why this reads only as a defect of the source.
     *
     * A name is a name when it is one path segment: not empty, not the two entries the protocol
     * reserves for the directory and its parent, and holding no separator, no NUL and no line
     * break. That is the whole rule, and it is deliberately the same shape as the local one - what
     * is refused is a name that could mean somewhere else, rather than a list of bad characters
     * somebody has to keep up to date.
     *
     * A bad entry is skipped rather than failing the listing, and this is the one place the two
     * readings diverge. Spec 7.4 already makes skipping the listing's way of not handing something
     * over: directories go this way by default and `.` and `..` always have. Failing instead would
     * cost the whole poll of that directory, on every tick, for as long as the entry is there -
     * and the party who can name the entry is the party who would then be choosing when the
     * connector runs. Skipping keeps the rest of the drop moving and leaves the WARN as the
     * record.
     */
    private fun String.entryPathFor(name: String): String? = when {
        // The protocol's own two entries, ordinary in every listing and silent since T6.
        name == "." || name == ".." -> null
        name.isNotEmpty() && name.none { it.cannotBeInAName() } -> asDirectoryOf(name)
        else -> {
            LOG.warn(
                "{} answered a listing of {} with an entry whose name is not a name - it is empty, or holds a " +
                    "separator, a NUL or a line break - so it was skipped instead of becoming a path this " +
                    "connector would quote back at the server: '{}'",
                endpoint,
                this,
                name.onOneLine(),
            )
            null
        }
    }

    override suspend fun stat(path: String): RemoteFile = withContext(io) {
        errors.translating(Attempt.inside(endpoint, "stat", path)) { channel.stat(path.literally()).describe(path) }
    }

    override suspend fun readTo(path: String, sink: OutputStream): Unit =
        transferring(Attempt.inside(endpoint, "read", path)) { channel.get(path.literally(), sink, it) }

    override suspend fun writeFrom(path: String, source: InputStream): Unit =
        transferring(Attempt.inside(endpoint, "write", path)) { channel.put(source, path.literally(), it, ChannelSftp.OVERWRITE) }

    /**
     * Moves the bytes of one file, under a monitor that stops as soon as nobody is waiting for
     * them any more.
     *
     * The monitor is the cheap way out of a transfer that has lost its caller. JSch asks it
     * between chunks, so the news travels within one chunk of bytes rather than at the end of the
     * file, and answering no closes the remote handle cleanly - the session is as good afterwards
     * as it was before, which is the whole difference between this and hanging up on it.
     *
     * A transfer stopped that way has delivered less than the file holds, so it says outright
     * that it was cancelled. Left to return normally it would reach the caller above as a short
     * file, which is a different fault with a different remedy - and this is the layer that knows
     * which of the two it was.
     */
    private suspend fun transferring(attempt: Attempt, move: (SftpProgressMonitor) -> Unit): Unit = withContext(io) {
        errors.translating(attempt) { move(StopWhenNobodyIsWaiting(coroutineContext.job)) }
        coroutineContext.ensureActive()
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
        errors.translating(Attempt.inside(endpoint, "rename", from)) { channel.rename(from.literally(), to.literally()) }
    }

    /** Read the way JSch reads it when deciding which request to send: advertised, at version 1. */
    override val renameReplaces: Boolean = channel.getExtension(POSIX_RENAME) == "1"

    override suspend fun delete(path: String): Unit = withContext(io) {
        errors.translating(Attempt.inside(endpoint, "delete", path)) { channel.rm(path.literally()) }
    }

    override suspend fun mkdir(path: String): Unit = withContext(io) {
        errors.translating(Attempt.inside(endpoint, "mkdir", path)) { channel.mkdir(path) }
    }

    /**
     * Uncancellable on purpose. A session left half-closed keeps its socket and its reader
     * thread until the process ends, and a caller being cancelled is exactly the moment that is
     * most likely to happen.
     */
    override suspend fun close(): Unit = withContext(io + NonCancellable) {
        errors.translating(Attempt.inside(endpoint, "close")) {
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

    /**
     * Closing the socket is what gets the blocked thread back, because that is the one thing a
     * blocking read reacts to. It runs on the caller's own thread rather than on the IO
     * dispatcher: every thread there may be the ones waiting to be rescued.
     */
    override fun abort() {
        try {
            session.disconnect()
        } catch (failure: Exception) {
            // Nobody is going to try again, and the session is being written off either way.
            LOG.warn("Cutting {} loose failed, and it is being written off regardless: {}", endpoint, failure.message)
        }
    }

    private companion object {
        private val LOG = LoggerFactory.getLogger(JschTransport::class.java)

        private const val POSIX_RENAME = "posix-rename@openssh.com"
    }
}

/**
 * Tells JSch to stop a transfer as soon as the coroutine that asked for it has been cancelled.
 *
 * It reads the job rather than being told to stop, because the cancellation it is watching for
 * arrives on a different thread than the one running the transfer, and a job is already the thing
 * both threads agree on. There is nothing to arm and nothing to remember to disarm.
 */
private class StopWhenNobodyIsWaiting(private val caller: Job) : SftpProgressMonitor {

    override fun init(op: Int, src: String?, dest: String?, max: Long) = Unit

    override fun count(count: Long): Boolean = caller.isActive

    override fun end() = Unit
}

/**
 * The path as JSch has to be handed it to take it as a name rather than a pattern.
 *
 * JSch reads `*` and `?` in the last component of the path it is given as wildcards and lists
 * the directory to resolve them, in every operation except mkdir and realpath: a rename onto
 * `l*.csv` landed on whichever neighbour matched and replaced it, a delete of `*.csv` sent one
 * remove per file that matched, and a stat of one answered for another. A backslash is its escape
 * for exactly those, and is stripped on the way out. Every path the connector sends names one
 * thing, so every path is escaped here, before the library sees it; the operations that send the
 * path raw are left raw, because an escape there would be sent to the server as part of the name.
 */
private fun String.literally(): String = replace(PATTERN_CHARACTERS) { "\\" + it.value }

private val PATTERN_CHARACTERS = Regex("""[\\*?]""")

/** JSch counts timeouts in milliseconds as an `Int`, and treats a negative one as an error. */
private fun Duration.toTimeoutMillis(): Int =
    inWholeMilliseconds.coerceIn(0L, Int.MAX_VALUE.toLong()).toInt()

/** Joins a directory to a name in it without doubling the separator a root path already ends with. */
private fun String.asDirectoryOf(name: String): String = if (endsWith("/")) "$this$name" else "$this/$name"

/**
 * What no path this connector will quote back at a server may hold: a NUL, which ends the name
 * early for everything downstream that reads a C string, and the line breaks and the other
 * control characters, which would let whoever names a file write lines into this connector's log.
 * No name a filesystem was meant to carry holds one, so a server sending one is saying something
 * other than a name.
 */
private fun Char.cannotBeInAPath(): Boolean = isISOControl()

/** The same, plus the separator that would make one name into several. */
private fun Char.cannotBeInAName(): Boolean = this == '/' || cannotBeInAPath()

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
