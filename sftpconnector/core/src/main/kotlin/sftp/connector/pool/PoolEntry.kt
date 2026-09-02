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
) {

    private val mutableState = MutableStateFlow(EntryState.Connecting)

    val state: StateFlow<EntryState> get() = mutableState

    internal var connection: SftpConnection? = null

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
