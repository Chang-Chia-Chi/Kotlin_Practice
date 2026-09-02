package sftp.connector.pool

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import sftp.connector.transport.SftpConnection

/**
 * Where one pooled session is in its life.
 *
 * Three of these are transitional, and they are the whole reason the pool never dials, checks or
 * hangs up on a server while holding its lock. An entry parked in [Connecting], [Validating] or
 * [Evicting] is one that some caller has already claimed and is about to do slow work on: the
 * claim was made under the lock in an instant, and the work happens after it was let go. Without
 * those three the only way to keep two callers off the same session would be to hold the lock for
 * the length of a network round trip, and one unresponsive server would then stop the whole pool.
 */
enum class EntryState {
    /** Claimed, and a session is being opened for it. It has no connection yet. */
    Connecting,

    /** Open, healthy and available. The only state in which an entry sits in the idle deque. */
    Idle,

    /** Lent to exactly one caller. */
    InUse,

    /** Claimed by an acquire that is asking the server whether the session is still there. */
    Validating,

    /** Taken out of service, with its connection still to be closed. */
    Evicting,

    /** Finished. Nothing reaches this entry again. */
    Closed,
}

/**
 * Why a session left the pool, in the words an operator reads off the eviction counter.
 *
 * The five are a closed set, and naming them here rather than spelling a string at each place a
 * session is thrown away is what keeps the counter's labels from drifting apart: two sites
 * writing "poison" and "poisoned" would split one number in half on the dashboard.
 */
internal enum class Retirement {
    /** It reached the end of the lifetime it was given when it was opened. */
    LIFETIME,

    /** It sat unused for longer than the pool keeps a spare. */
    IDLE,

    /** Something that happened on it proved it was no longer trustworthy. */
    POISONED,

    /** It was asked whether it was still there and it did not answer. */
    VALIDATION,

    /** The connector is shutting down and is not keeping anything. */
    SHUTDOWN,
    ;

    /** The value the counter is tagged with. */
    val label: String get() = name.lowercase()
}

/**
 * The pool's record of one session.
 *
 * The entry outlives any one borrowing of it, which is what lets the pool talk about a session it
 * has not opened yet, or one it has decided to throw away but not yet hung up on. Its [state] is
 * published as a flow because an entry changes hands between coroutines and the change is worth
 * watching rather than polling for.
 *
 * Everything mutable here is written only while the registry's lock is held, so an entry is not
 * itself thread-safe and is not meant to be handled outside the pool.
 */
class PoolEntry internal constructor(
    /** Counts from one within a pool. It is what a log line uses to follow one session over time. */
    val id: Long,
    /**
     * The moment this session stops being reusable however healthy it looks, as milliseconds on
     * the pool's clock. It is this session's own moment rather than a shared one, so a pool that
     * filled in one burst does not retire everything it holds in another.
     */
    internal val expiresAt: Long,
) {

    private val mutableState = MutableStateFlow(EntryState.Connecting)

    val state: StateFlow<EntryState> get() = mutableState

    internal var connection: SftpConnection? = null

    /** When it went on the shelf. Meaningless while somebody is holding it. */
    internal var idleSince: Long = 0

    /** When the caller currently holding it took it. Meaningless while it is on the shelf. */
    internal var borrowedAt: Long = 0

    /**
     * Where the lease was taken, captured as a stack trace because that is the only thing that
     * points at the code holding a session too long. Nothing is thrown; it is carried so a leak
     * report can name a line rather than a session number.
     */
    internal var borrower: Throwable? = null

    /** A leak is worth saying once. Said every housekeeping round, it would bury the log. */
    internal var leakReported = false

    internal fun claimedBy(borrower: Throwable, now: Long) {
        this.borrower = borrower
        borrowedAt = now
        leakReported = false
    }

    internal fun moveTo(next: EntryState) {
        mutableState.value = next
    }

    override fun toString(): String = "session #$id (${mutableState.value})"
}

/** What the pool holds at one instant, counted in one pass so the numbers agree with each other. */
data class PoolStats(
    val idle: Int,
    val inUse: Int,
    val connecting: Int,
    /**
     * Callers queued at the door, holding no session. They are not part of [total] - the pool is
     * not responsible for them, it is what they are waiting on - but they are the difference
     * between a pool that is merely full and one that is short.
     */
    val pending: Int = 0,
) {
    /** Every session the pool is responsible for. Bounded by `maxSize`; that bound is I1. */
    val total: Int get() = idle + inUse + connecting

    override fun toString(): String = "idle=$idle, inUse=$inUse, connecting=$connecting, pending=$pending"
}
