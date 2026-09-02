package sftp.connector.transport

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
 * One live session carrying exactly one SFTP channel.
 *
 * The channel serializes whatever is asked of it, so a connection belongs to one caller at a
 * time. That is the pool's job to arrange.
 */
interface SftpConnection {

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
     * Ends the session and releases the thread and socket behind it. Calling it more than once
     * is harmless.
     */
    suspend fun close()
}
