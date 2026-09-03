package sftp.connector.transport

import java.io.InputStream
import java.io.OutputStream

/**
 * Opens sessions to one SFTP server.
 *
 * This is the seam. Above it everything works in paths and strings; which SSH library dials the
 * socket, which threads it blocks and how a call in flight is cancelled are the adapter's
 * business and nobody else's. That is what lets the pool, the client and the source be tested
 * against a scripted transport that never opens a socket, and lets the SSH library be replaced
 * without any of them noticing.
 */
interface SftpTransport {

    /**
     * Opens one session and returns it ready to use. Suspends for the whole handshake, which on
     * this network means a proxy tunnel, a key exchange, authentication and a channel open, and
     * throws if any of that fails.
     */
    suspend fun connect(): SftpConnection
}

/**
 * Everything one session can be asked to do, and nothing about how long it lives.
 *
 * The split is what lets a borrowed session be handed to somebody who did not open it. A caller
 * that got its session from the pool must not end it - the pool lends the same session out again
 * afterwards, and a caller that hung up on it would break the next caller's work rather than its
 * own. So it is given this, which offers the operations and does not offer the hang-up.
 *
 * The channel underneath serializes whatever is asked of it, so a session belongs to one caller
 * at a time. That is the pool's job to arrange.
 */
interface SftpSession {

    /**
     * Resolves [path] to the absolute path the server knows it by, following symbolic links.
     * Also the cheapest round trip there is, which is why it doubles as the liveness check for
     * a session that has been sitting idle.
     */
    suspend fun realpath(path: String): String

    /**
     * Reports the entries of [dir] to [onEntry] one at a time, as the server sends them, and
     * returns once the directory is exhausted or [onEntry] answers [Listing.STOP].
     *
     * A callback rather than a returned collection, because a directory can hold a hundred
     * thousand files and the caller may want the first thousand. Handing back a list would mean
     * holding all of them, and a caller that wanted the first thousand would still have paid for
     * every one. Here the caller decides after each entry whether there is any point in the next.
     *
     * `.` and `..` are never reported: they are an artifact of how directories are stored and
     * nothing above this seam should have to know they exist. Directories themselves are, and it
     * is the caller that decides what to do about them.
     *
     * [onEntry] runs on whichever thread this implementation reads the server on, and it holds
     * that read up for as long as it takes. Blocking in it is how a caller applies backpressure to
     * the server; doing anything slow in it that is not backpressure holds a session open for the
     * length of it.
     */
    suspend fun list(dir: String, onEntry: (RemoteFile) -> Listing)

    /**
     * What the server currently says about one path.
     *
     * @throws sftp.connector.error.NoSuchFile when there is nothing there. A missing path is
     *   reported rather than returned as an absence, because "not there" and "there and refused"
     *   are different answers and only the caller knows which of them matters to what it is doing.
     */
    suspend fun stat(path: String): RemoteFile

    /**
     * Copies the whole of [path] into [sink] and returns when the last byte has been written.
     *
     * The transfer is one call rather than a stream the caller pumps, because every byte crosses
     * a blocking socket read and pumping it from above would put those reads on whatever thread
     * the caller happened to be on. [sink] is left open: whoever supplied it knows what else has
     * to happen to it.
     */
    suspend fun readTo(path: String, sink: OutputStream)

    /**
     * Writes the whole of [source] to [path], replacing whatever was there, and returns when the
     * last byte has been acknowledged.
     *
     * The mirror of [readTo], and one call for the same reason: every byte crosses a blocking
     * socket write, and handing back a stream for the caller to pump would put those writes on
     * whatever thread the caller happened to be on. [source] is left open, because whoever
     * supplied it knows what else has to happen to it.
     *
     * A path that already holds a file is truncated. Refusing to disturb one is a policy, and it
     * is decided above this seam because the decision is the caller's rather than the server's.
     */
    suspend fun writeFrom(path: String, source: InputStream)

    /**
     * Moves [from] to [to] in one request.
     *
     * What happens to a file already at [to] is the server's answer and not this one's, and the
     * two possible answers are opposites. A server offering the POSIX rename extension replaces it
     * with no moment in between, whether or not that was wanted; one without the extension refuses
     * outright, and a caller that wanted the replacement has to clear the way itself and accept the
     * gap that opens while it does. So a caller that cares either way decides above this seam
     * rather than sending the request and reading the answer.
     *
     * @throws sftp.connector.error.NoSuchFile when [from] is not there - which is the answer a
     *   retry needs, because it means either that nothing was ever there or that an earlier
     *   attempt at this same rename already landed.
     */
    suspend fun rename(from: String, to: String)

    /**
     * Whether a [rename] onto an occupied path replaces what is there rather than being refused.
     *
     * True on a server offering the POSIX rename extension, which the SSH library uses on its own
     * once the server has advertised it. It is a fact about the server that a caller replacing a
     * file has to know: on such a server a refused rename was never about the target being in the
     * way, so there is nothing to clear and no reason to send it again - and on a server without
     * it, clearing the target and sending again is the only way to replace.
     */
    val renameReplaces: Boolean

    /**
     * Removes the file at [path].
     *
     * @throws sftp.connector.error.NoSuchFile when there is nothing there. Whether that is a
     *   failure or the outcome already achieved is the caller's to say.
     */
    suspend fun delete(path: String)

    /**
     * Creates the directory [path], and only that one: a missing parent is a failure here rather
     * than something to fill in, because inventing intermediate directories is a decision and the
     * server was not asked to take it.
     */
    suspend fun mkdir(path: String)
}

/**
 * One live session, from the point of view of whoever opened it and will therefore close it.
 *
 * The pool holds these. Everyone it lends to holds an [SftpSession] instead.
 */
interface SftpConnection : SftpSession {

    /**
     * Ends the session and releases the thread and socket behind it. Calling it more than once
     * is harmless.
     */
    suspend fun close()

    /**
     * Destroys the session from underneath whatever is using it, so that a call blocked on a
     * socket nobody is answering gets its thread back.
     *
     * This is the violent one, and it is not [close] in a hurry. [close] is the orderly hang-up on
     * a session nobody is using; this one is called from a *different* thread than the one it is
     * rescuing, while a call is still in flight on the session, and the session is unusable
     * afterwards by design. Whoever calls it is choosing to pay for a new handshake in exchange
     * for a bound on something that has none.
     *
     * It does not suspend, and implementations must not put it on the connector's IO dispatcher:
     * that dispatcher is exactly as wide as the pool, so the moment it is worth aborting anything
     * is the moment every thread on it may already be blocked. It must not throw either - there is
     * nothing a caller could usefully do about an abort that failed, and it is already the last
     * resort.
     */
    fun abort()
}
