package sftp.connector.error

/**
 * One try at one operation against one server.
 *
 * It is what an operator needs to act on a failure and what a stack trace alone never says: which
 * of several connectors this was, what it was doing, which file it was doing it to, and whether
 * this was the first go or the last. Every failure that happens while the connector is running
 * carries one, and puts it in the exception message, so a single log line is enough to place the
 * failure without reading the lines around it.
 */
data class Attempt(
    /** How the server is addressed, as `host:port`. */
    val endpoint: String,
    /** What was being asked of it: `connect`, `list`, `download`. */
    val operation: String,
    /** The remote path involved, when the operation has one. */
    val path: String? = null,
    /** Counting from one. */
    val number: Int = 1,
) {
    internal fun describe(detail: String): String = buildString {
        append(detail)
        append(" (endpoint=").append(endpoint)
        append(", op=").append(operation)
        if (path != null) append(", path=").append(path)
        append(", attempt=").append(number)
        append(')')
    }
}

/**
 * Every failure the connector reports, whatever raised it underneath.
 *
 * The hierarchy is sealed so that handling one is a `when` the compiler checks: add a case here
 * and every place that reacts to failures is named by the build rather than found in production.
 * Callers do not read the class to decide what to do, though - they read [disposition], which
 * turns the whole of this hierarchy into six answers.
 */
sealed class SftpException(message: String, cause: Throwable?) : RuntimeException(message, cause) {

    /** What to do about it. */
    abstract val disposition: Disposition
}

/**
 * Something went wrong that trying again might survive: a dropped connection, a busy server, a
 * file that was not there this second.
 *
 * [poisons] says whether the session it happened on is still trustworthy. It is the one fact a
 * recoverable failure knows that its class does not already imply, and it is what separates the
 * two retry dispositions.
 */
sealed class Recoverable(
    val attempt: Attempt,
    detail: String,
    cause: Throwable?,
    val poisons: Boolean,
) : SftpException(attempt.describe(detail), cause) {

    override val disposition: Disposition
        get() = if (poisons) Disposition.RETRY_ON_A_FRESH_SESSION else Disposition.RETRY_ON_THIS_SESSION
}

/** No session was established: the proxy refused, the address did not resolve, the handshake never finished. */
class ConnectFailed(attempt: Attempt, detail: String, cause: Throwable? = null) :
    Recoverable(attempt, detail, cause, poisons = true)

/** A session that was working stopped working underneath the call. */
class SessionLost(attempt: Attempt, detail: String, cause: Throwable? = null) :
    Recoverable(attempt, detail, cause, poisons = true)

/**
 * The operation ran past the time allowed for it. The session is left poisoned because the
 * request may still be in flight on it, and a reply arriving later would be read as the answer to
 * whatever the next caller asked.
 */
class OperationTimeout(attempt: Attempt, detail: String, cause: Throwable? = null) :
    Recoverable(attempt, detail, cause, poisons = true)

/**
 * The server answered with a failure of its own. Poisoning is the cautious reading of a status
 * code that means only "no": the server has not said what state it left the channel in.
 */
class ServerFailure(attempt: Attempt, val statusCode: Int, detail: String, cause: Throwable? = null) :
    Recoverable(attempt, detail, cause, poisons = true)

/**
 * A failure whose wording the connector does not recognise.
 *
 * It is deliberately the mildest thing an unrecognised message can become: recoverable, so a
 * rewording in a new library release degrades to a retry instead of stopping the connector, and
 * poisoning, because a message nobody has read cannot be evidence that the session is healthy.
 * [rawMessage] is kept exactly as it arrived so that adding the wording to the table is a copy
 * rather than a reconstruction.
 */
class Unknown(attempt: Attempt, val rawMessage: String, cause: Throwable? = null) :
    Recoverable(attempt, "unrecognised failure: $rawMessage", cause, poisons = true)

/**
 * The server refused on permissions. The session is fine - it was the request that was refused -
 * and retrying in a hundred milliseconds asks the same question of the same unchanged server, so
 * this one waits for the next poll instead.
 */
class PermissionDenied(attempt: Attempt, detail: String, cause: Throwable? = null) :
    Recoverable(attempt, detail, cause, poisons = false) {
    override val disposition: Disposition get() = Disposition.RETRY_ON_THE_NEXT_TICK
}

/**
 * The path is not there. Ordinary rather than exceptional on a directory another system is
 * writing into, so the session stays in the pool and the caller decides what a missing file means
 * for the operation it was doing.
 */
class NoSuchFile(attempt: Attempt, detail: String, cause: Throwable? = null) :
    Recoverable(attempt, detail, cause, poisons = false)

/**
 * Retrying is pointless: the credential is wrong, the server is not who it claims to be, or the
 * configuration cannot start a connector. Someone has to change something before the next attempt
 * can go differently.
 */
sealed class Fatal(message: String, cause: Throwable?) : SftpException(message, cause) {
    final override val disposition: Disposition get() = Disposition.STOP_THE_CONNECTOR
}

/** The server would not accept the credential. */
class AuthenticationFailed(val attempt: Attempt, detail: String, cause: Throwable? = null) :
    Fatal(attempt.describe(detail), cause)

/**
 * The key the server presented is not the key the connector was told to expect. Treated as fatal
 * rather than retried, because the one explanation that must never be retried past is that
 * something else is answering on the server's address.
 */
class HostKeyRejected(val attempt: Attempt, detail: String, cause: Throwable? = null) :
    Fatal(attempt.describe(detail), cause)

/**
 * Configuration that cannot start a connector.
 *
 * Thrown while the configuration block is being built, so an unreachable endpoint description or
 * an impossible timeout surfaces at assembly time rather than on the first connect attempt an
 * hour into a run. It carries no [Attempt] because nothing was attempted: there was no connector
 * yet to attempt anything with.
 */
class ConfigurationError(message: String) : Fatal(message, null)

/**
 * No session came free in time. Nothing was asked of the server, so there is nothing to retry and
 * nothing to hold against it; the next poll starts over.
 */
class PoolExhausted(val attempt: Attempt) : SftpException(
    attempt.describe("no session became free before the acquire timeout ran out"),
    null,
) {
    override val disposition: Disposition get() = Disposition.FAIL_THE_ATTEMPT
}

/** The breaker is open, so the connector deliberately sent nothing. */
class CircuitOpen(val attempt: Attempt) : SftpException(
    attempt.describe("the circuit breaker is open, so nothing was sent to the server"),
    null,
) {
    override val disposition: Disposition get() = Disposition.SKIP_THE_TICK
}
