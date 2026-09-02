package sftp.connector.transport.jsch

import com.jcraft.jsch.ChannelSftp
import com.jcraft.jsch.JSchException
import com.jcraft.jsch.JSchHostKeyException
import com.jcraft.jsch.JSchProxyException
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import sftp.connector.error.Attempt
import sftp.connector.error.AuthenticationFailed
import sftp.connector.error.ConnectFailed
import sftp.connector.error.HostKeyRejected
import sftp.connector.error.NoSuchFile
import sftp.connector.error.PermissionDenied
import sftp.connector.error.ServerFailure
import sftp.connector.error.SessionLost
import sftp.connector.error.SftpException
import sftp.connector.error.Unknown
import java.io.IOException
import kotlin.coroutines.cancellation.CancellationException
import com.jcraft.jsch.SftpException as JschStatusException

/**
 * Turns whatever JSch throws into the connector's own failures.
 *
 * It is the only thing standing between a JSch exception and the rest of the connector, and it is
 * why the pool, the client and the source can decide what to do about a failure without ever
 * naming the library that raised it. Run every JSch call through [translating]: it is the whole
 * of this class's surface, and going through it is what makes forgetting the cancellation rule
 * impossible rather than merely unlikely.
 *
 * The wordings below were read off the pinned JSch by triggering each condition against a real
 * server, not remembered from its source - the one exception is called out where it sits. That
 * matters because they are free text: this fork rejects an unrecognised host key with "reject
 * HostKey:", where older versions said "UnknownHostKey:", and a table written from memory would
 * have classified a man-in-the-middle as an unrecognised failure.
 *
 * A wording that is not here is not a gap to be guessed at. It becomes [Unknown], which retries,
 * warns with the raw text and counts itself, so the row gets written from what the server
 * actually said the first time it says it.
 */
class JschErrorMapper(private val meters: MeterRegistry) {

    /**
     * Runs [block] and lets nothing out of it except a connector failure - or a cancellation,
     * which passes through untouched.
     */
    fun <T> translating(attempt: Attempt, block: () -> T): T =
        try {
            block()
        } catch (cancelled: CancellationException) {
            // Cancellation is the caller changing its mind, not the server failing. Wrapping it
            // would hide a cancelled coroutine from the machinery that has to see one, and would
            // put a retry ladder in front of work nobody wants done any more.
            throw cancelled
        } catch (failure: Exception) {
            throw classify(failure, attempt)
        }

    private fun classify(failure: Exception, attempt: Attempt): SftpException {
        // Already classified, by an inner call that went through here first. Classifying it again
        // would bury a decided failure - a rejected password, say - inside an unrecognised one,
        // turning a fatal error into a retry and adding a count to a meter whose whole value is
        // that a non-zero reading means a real wording is missing from the table.
        if (failure is SftpException) return failure

        if (failure is JschStatusException) return fromStatusCode(failure, attempt)

        // Both of these have a type of their own, so they are recognised by what they are rather
        // than by how they read, and a rewording cannot silently reclassify them.
        if (failure is JSchHostKeyException) {
            return HostKeyRejected(
                attempt,
                "the server presented a host key the connector will not accept: ${failure.message}",
                failure,
            )
        }
        if (failure is JSchProxyException) {
            return ConnectFailed(attempt, "the proxy did not open a tunnel: ${failure.message}", failure)
        }

        // Everything else JSch raises is a JSchException carrying free text and nothing else.
        val message = failure.message
        if (failure !is JSchException || message == null) return unknown(failure, attempt)

        return when {
            // "Auth fail for methods 'password,keyboard-interactive,publickey'"
            message.startsWith("Auth fail") || message.startsWith("Auth cancel") ->
                AuthenticationFailed(attempt, "the server rejected the credential: $message", failure)

            // What JSch says when asked to open a channel on a session that has already gone. The
            // one row here that no test stages, because the transport does not yet expose the
            // call that produces it: opening a second channel on a session held open by a pool.
            message.startsWith("session is down") ->
                SessionLost(attempt, "the session was already gone: $message", failure)

            // A server that authenticates and then refuses the sftp subsystem: SSH works, SFTP
            // does not, and no session ever becomes usable.
            message.startsWith("failed to send channel request") || message.contains("channel is not opened") ->
                ConnectFailed(attempt, "the server would not open an SFTP channel: $message", failure)

            // JSch stringifies the underlying socket failure into its own message and replaces
            // the cause with a copy of itself, so the text is the only place the real fault
            // survives. A refused port, an unresolvable name and a handshake that timed out all
            // arrive this way, and all three mean the same thing: no session.
            message.contains(SOCKET_FAILURE_MARKER) ->
                ConnectFailed(attempt, "the connection could not be established: $message", failure)

            else -> unknown(failure, attempt)
        }
    }

    private fun fromStatusCode(failure: JschStatusException, attempt: Attempt): SftpException {
        // JSch reports a broken connection through the same exception type it uses for the
        // server's own answers, with the generic failure code and the IO error as the cause. Read
        // by code alone, a dead socket and a stalled tunnel both look like the server refusing.
        val cause = failure.cause
        if (cause is IOException) {
            return SessionLost(attempt, "the connection broke under the request: ${cause.message}", failure)
        }

        return when (failure.id) {
            ChannelSftp.SSH_FX_NO_SUCH_FILE ->
                NoSuchFile(attempt, "the server has no such path: ${failure.message}", failure)

            ChannelSftp.SSH_FX_PERMISSION_DENIED ->
                PermissionDenied(attempt, "the server refused on permissions: ${failure.message}", failure)

            else -> ServerFailure(
                attempt,
                failure.id,
                "the server refused the request with status ${failure.id}: ${failure.message}",
                failure,
            )
        }
    }

    /**
     * The safety net, and the only place a wording nobody has read reaches the rest of the
     * connector. It is loud on purpose: the counter turns "a wording we have never seen" into
     * something a dashboard shows, and the log line carries the raw text so adding the row is a
     * copy rather than a reconstruction.
     */
    private fun unknown(failure: Exception, attempt: Attempt): Unknown {
        val raw = failure.message ?: failure.toString()
        LOG.warn(
            "No mapping for this failure, so it is being treated as retryable and the session " +
                "discarded. Add the wording to the connector's error table so the next occurrence " +
                "is classified. endpoint={}, op={}, type={}, message: {}",
            attempt.endpoint,
            attempt.operation,
            failure.javaClass.name,
            raw,
        )
        Counter.builder(UNMAPPED_ERRORS)
            .tag("endpoint", attempt.endpoint)
            .register(meters)
            .increment()
        return Unknown(attempt, raw, failure)
    }

    private companion object {
        private val LOG = LoggerFactory.getLogger(JschErrorMapper::class.java)

        /**
         * Any `java.net` exception name appearing inside a JSch message. JSch builds its message
         * by stringifying whatever it caught, so the package name is what survives when the
         * exception type does not.
         */
        private const val SOCKET_FAILURE_MARKER = "java.net."

        private const val UNMAPPED_ERRORS = "sftp_error_unmapped_total"
    }
}
