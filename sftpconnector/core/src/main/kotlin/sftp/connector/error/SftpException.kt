package sftp.connector.error

import sftp.connector.pool.PoolStats
import kotlin.time.Duration

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
 * The server answered with a failure of its own.
 *
 * The session survives it. A well-formed status reply proves the channel parsed the request and
 * answered, which is the definition of a healthy channel; the refusal was of this one request. It
 * is also the ordinary answer from a server without the POSIX rename extension, so discarding the
 * session here would cost a handshake on every overwrite rename against such a server. Breakage
 * that only looks like this arrives carrying an IO error and is classified as a lost session
 * before it ever reaches this class.
 */
class ServerFailure(attempt: Attempt, val statusCode: Int, detail: String, cause: Throwable? = null) :
    Recoverable(attempt, detail, cause, poisons = false)

/**
 * The bytes that arrived do not add up to the file the server said was there.
 *
 * Every other recoverable failure repeats something the wire reported. This one is the connector
 * checking its own work, and it is worth its own name because of what an operator does next:
 * calling it a lost session sends them to the network and the proxy, when the evidence in hand is
 * that a file changed size underneath a transfer - which is what a stalled or still-writing
 * uploader produces, and the one observation that would tell a maintainer their readiness
 * convention is not holding.
 *
 * It poisons all the same. A short read and a half-dead session look identical from where the
 * count is taken, and the safe reading costs one handshake on an event that should be rare.
 */
class IncompleteTransfer(attempt: Attempt, detail: String, cause: Throwable? = null) :
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
 * Most of these are thrown while the configuration block is being built, so an unreachable
 * endpoint description or an impossible timeout surfaces at assembly time rather than on the first
 * connect attempt an hour into a run. The rest come from the checks start-up runs against the
 * server, which are the ones that catch a configuration only the server can refute: a directory
 * that is not there, a folder the account may not create, a move the server cannot make. Those
 * carry [cause], because the server's own answer is evidence and paraphrasing it loses detail.
 *
 * It carries no [Attempt] even then. An attempt says which of several tries this was and invites
 * another; a configuration fault is a statement that no number of tries will do.
 */
class ConfigurationError(message: String, cause: Throwable? = null) : Fatal(message, cause)

/**
 * No session came free in time. Nothing was asked of the server, so there is nothing to retry and
 * nothing to hold against it; the next poll starts over.
 *
 * What an operator needs from this at three in the morning is not that the pool was full - that is
 * implied by the class - but which of three quite different things made it full, because the three
 * have three different remedies. So the numbers are the message, and [explainExhaustion] states
 * out loud which of the three they describe.
 */
class PoolExhausted(
    val attempt: Attempt,
    /** What the pool looked like the instant the wait ran out. Absent when no pool raised this. */
    val stats: PoolStats? = null,
    /** The acquire timeout that ran out, which is the whole time this caller queued for. */
    val waited: Duration = Duration.ZERO,
    /** How often room came free while this caller queued. Zero means nothing moved at all. */
    val roomFreedWhileWaiting: Long = 0,
) : SftpException(attempt.describe(explainExhaustion(stats, waited, roomFreedWhileWaiting)), null) {
    override val disposition: Disposition get() = Disposition.FAIL_THE_ATTEMPT
}

/**
 * Turns the pool's counts into the sentence an operator can act on.
 *
 * Sessions stuck opening mean the handshake is not completing, which is a server or a network
 * fault that no amount of extra pool size will help. Nothing coming free in the whole wait means
 * the work already holding the sessions is not finishing, so the thing to look at is that work.
 * Room coming free and being taken by somebody else means this caller lost races it kept
 * entering, which is a pool short of what its load asks of it.
 */
private fun explainExhaustion(stats: PoolStats?, waited: Duration, freed: Long): String {
    if (stats == null) return "no session became free before the acquire timeout ran out"
    val reading = when {
        stats.connecting > 0 && stats.connecting >= stats.inUse ->
            "most of the pool is stuck opening sessions, so look at the server and the network rather than at maxSize"

        freed == 0L ->
            "nothing came free at all, so the work already holding the sessions is not finishing"

        else ->
            "room came free and other callers took it, so the pool is short of what this load asks of it"
    }
    return "no session came free in $waited. ${stats.inUse} in use, ${stats.connecting} still opening, " +
        "${stats.idle} idle, ${stats.pending} waiting including this one, " +
        "room came free $freed times while it waited; $reading"
}

/**
 * The path the operation was aiming at is occupied, and the overwrite policy said not to replace
 * what is there.
 *
 * It sits beside [PoolExhausted] and [CircuitOpen] rather than under [Recoverable] because it is
 * the same shape of failure as those two: real, and nobody's session's fault, and no reason to try
 * again. Trying again cannot help - the file in the way will still be in the way - and holding it
 * against the server would charge the connector for doing exactly what it was configured to do,
 * which is how a pipeline behaving correctly opens its own circuit breaker.
 *
 * The session is untouched, because under a refusing policy nothing was ever sent.
 */
class OverwriteRefused(val attempt: Attempt, detail: String) :
    SftpException(attempt.describe(detail), null) {
    override val disposition: Disposition get() = Disposition.ACCEPT_THE_REFUSAL
}

/** The breaker is open, so the connector deliberately sent nothing. */
class CircuitOpen(val attempt: Attempt) : SftpException(
    attempt.describe("the circuit breaker is open, so nothing was sent to the server"),
    null,
) {
    override val disposition: Disposition get() = Disposition.SKIP_THE_TICK
}
