package sftp.connector.error

/**
 * What the connector does about a failure.
 *
 * Every failure answers four questions at once - retry or not, count it against the breaker or
 * not, what becomes of the session it was holding, and what the watching consumer sees - and the
 * four answers only make sense together. A caller that had to combine them itself would be
 * deciding, on its own, something the failure already knows; the day the combination changed,
 * every one of those callers would be a place to remember. So the failure answers, and the
 * caller obeys.
 *
 * Each constant is one whole way a failure can end. Read one to learn everything.
 */
enum class Disposition(
    val retry: Retry,
    val countsAgainstTheBreaker: Boolean,
    val lease: LeaseFate,
    val watch: WatchReaction,
) {
    /**
     * The session is suspect - it timed out, died, or answered something the connector could not
     * read - so the work is worth another go but not on this connection.
     */
    RETRY_ON_A_FRESH_SESSION(Retry.IMMEDIATELY, true, LeaseFate.EVICTED, WatchReaction.REPORT_THE_FAILURE),

    /**
     * The server refused this request and the connection carrying it is fine, so the retry costs
     * no handshake.
     */
    RETRY_ON_THIS_SESSION(Retry.IMMEDIATELY, true, LeaseFate.RETURNED, WatchReaction.REPORT_THE_FAILURE),

    /**
     * Retrying within seconds cannot help, because whatever has to change - a permission, an
     * ownership, a mount - changes at human speed. Waiting a whole poll interval turns a hot
     * retry loop against an unchanging server into one attempt per tick.
     */
    RETRY_ON_THE_NEXT_TICK(Retry.AFTER_A_FULL_TICK, true, LeaseFate.RETURNED, WatchReaction.REPORT_THE_FAILURE),

    /**
     * Nothing the connector can do will make the next attempt go differently, so it stops rather
     * than hammering a server that will keep saying no. The breaker is left untouched on purpose:
     * a breaker exists to stop traffic to a server in trouble, and a rejected password says
     * nothing about the server's health.
     */
    STOP_THE_CONNECTOR(Retry.NEVER, false, LeaseFate.EVICTED, WatchReaction.STOP),

    /**
     * The connector never reached the server, so there is nothing to retry and nothing the
     * server did wrong. The next tick tries again from the top.
     */
    FAIL_THE_ATTEMPT(Retry.NEVER, false, LeaseFate.NONE_HELD, WatchReaction.REPORT_THE_FAILURE),

    /**
     * Deliberately sending nothing. This is the breaker doing its job, not a fault, and reporting
     * it as a failure would make a working safety mechanism look like an outage.
     */
    SKIP_THE_TICK(Retry.NEVER, false, LeaseFate.NONE_HELD, WatchReaction.REPORT_A_SKIP),

    /**
     * The connector refused on its own instruction and there is nothing more to decide. Asking
     * again would put the same question to a server that never heard the first one, and counting
     * it against the breaker would charge the connector for obeying its own configuration. The
     * session held while the refusal was decided is untouched and goes straight back.
     */
    ACCEPT_THE_REFUSAL(Retry.NEVER, false, LeaseFate.RETURNED, WatchReaction.REPORT_THE_FAILURE),
}

/** Whether the operation is worth attempting again, and how soon. */
enum class Retry {
    IMMEDIATELY,

    /** Not before the next poll; a fast retry would only ask the same unchanged question again. */
    AFTER_A_FULL_TICK,

    NEVER,
}

/** What becomes of the pooled session the failing operation was holding. */
enum class LeaseFate {
    /** Still healthy; the next caller can have it. */
    RETURNED,

    /** Closed and discarded rather than handed on, because its state is no longer trustworthy. */
    EVICTED,

    /** There was no session: the failure happened before one was ever handed out. */
    NONE_HELD,
}

/** What a consumer collecting from `watch` sees when the failure reaches it. */
enum class WatchReaction {
    REPORT_THE_FAILURE,

    /** This tick did nothing, and that was the intended behaviour rather than a fault. */
    REPORT_A_SKIP,

    /** The flow ends carrying the error; there is no point polling again. */
    STOP,
}
