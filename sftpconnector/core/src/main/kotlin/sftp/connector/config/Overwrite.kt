package sftp.connector.config

/**
 * What an operation does about a file already sitting at the path it is aiming for.
 *
 * It reads like a flag and it is not one. SFTP version 3 has no way to say "put this here and
 * replace whatever is there" - the POSIX rename extension adds one, and a server without it can
 * only be told to clear the path and then aim at it again. So [REPLACE] is not a bit set on a
 * request; it is a short sequence of requests with a gap in the middle, and the gap is the part
 * a caller has to know about. Each operation's own documentation says what its gap looks like,
 * because they are not the same gap.
 */
enum class Overwrite {

    /**
     * Leave whatever is there alone and fail instead.
     *
     * Always the connector's own decision, taken before the request goes out, and on every
     * operation it is a look followed by a request - so a writer arriving between the two still
     * wins. It cannot be left to the server, because a server offering the POSIX rename extension
     * replaces the target without being asked and reports success. A server without the extension
     * refuses the request as well, which closes the race there and only there.
     */
    REFUSE,

    /**
     * Take the path for this file, whatever was there before.
     *
     * On a rename against a server offering the POSIX rename extension this happens in one request
     * with no moment in between. Against a server without it, the target is deleted and the rename
     * is sent again - and between those two requests the path holds nothing at all, so anything
     * watching that path can see it empty and anything that fails in the gap leaves it empty.
     * On an upload the file is truncated and refilled, so the path holds a partial file for the
     * length of the transfer.
     */
    REPLACE,
}
