package sftp.connector.transport

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
     * Ends the session and releases the thread and socket behind it. Calling it more than once
     * is harmless.
     */
    suspend fun close()
}
