package sftp.connector.pool

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import sftp.connector.transport.SftpConnection

/**
 * Every session the pool has created and not yet finished with, and the one lock over all of it.
 *
 * This class decides; it never acts. Each of its methods answers a question - which session should
 * this caller get, may this one go back on the shelf - and returns before anything slow happens.
 * The slow part is the caller's to do afterwards, and by then the lock is long gone.
 *
 * That split is not a convention anyone has to remember. The lock is private to this class, so no
 * code elsewhere can hold it, and this class is given no transport, so there is nothing here to
 * dial a server with. Opening a session while the pool is locked is therefore not something that
 * can be written by mistake: it would first have to be made possible.
 *
 * A registry rather than a queue of live sessions, because the interesting operations are
 * "retire this particular session" and "how many are there", and a queue answers neither. Expiry
 * is a state change on an entry, not a drain-and-refill.
 */
internal class SessionRegistry {

    private val mutex = Mutex()

    /** Everything alive: connecting, idle, lent out or being checked. Insertion-ordered for logs. */
    private val entries = LinkedHashSet<PoolEntry>()

    /**
     * The idle ones, newest at the end. Taking from the end reuses the session whose socket was
     * proved good most recently, and leaves the coldest one to age out.
     */
    private val idle = ArrayDeque<PoolEntry>()

    private var sessionsCreated = 0L

    /**
     * Claims a session for one caller and says what has to happen before it can be used.
     *
     * The claim is what makes an entry exclusive: it leaves the idle deque here and comes back
     * only when it is handed back, so two callers cannot be looking at the same session.
     */
    suspend fun checkOut(): Checkout = mutex.withLock {
        val warm = idle.removeLastOrNull()
        if (warm == null) {
            val fresh = PoolEntry(++sessionsCreated)
            entries += fresh
            Checkout.Dial(fresh)
        } else {
            warm.moveTo(EntryState.InUse)
            Checkout.Reuse(warm)
        }
    }

    /** The session opened. */
    suspend fun filled(entry: PoolEntry, connection: SftpConnection) = mutex.withLock {
        entry.connection = connection
        entry.moveTo(EntryState.InUse)
    }

    /**
     * Takes [entry] back. A [healthy] one goes on the shelf; any other is dropped from the pool
     * and its connection is returned for the caller to close, which is the one piece of I/O a
     * handback can imply and the reason this returns anything at all.
     */
    suspend fun handBack(entry: PoolEntry, healthy: Boolean): SftpConnection? = mutex.withLock {
        if (healthy) {
            entry.moveTo(EntryState.Idle)
            idle.addLast(entry)
            null
        } else {
            entry.moveTo(EntryState.Evicting)
            entries -= entry
            idle.remove(entry)
            entry.connection.also { entry.connection = null }
        }
    }

    /** The connection is closed and the entry is finished. */
    suspend fun closed(entry: PoolEntry) = mutex.withLock {
        entry.moveTo(EntryState.Closed)
    }

    /**
     * One consistent count of everything. Taken under the lock, so the numbers describe a moment
     * that really existed rather than three reads of a moving target.
     */
    suspend fun stats(): PoolStats = mutex.withLock {
        var inUse = 0
        var connecting = 0
        entries.forEach {
            when (it.state.value) {
                // A session being checked is a session someone is holding, not a spare.
                EntryState.InUse, EntryState.Validating -> inUse++
                EntryState.Connecting -> connecting++
                else -> Unit
            }
        }
        PoolStats(idle = idle.size, inUse = inUse, connecting = connecting)
    }
}

/**
 * What an acquire must do with the session it just claimed.
 *
 * An answer rather than a session and a flag, because the caller then has no decision left to
 * take: it does the one thing its answer names. The work an answer implies is network work, and
 * none of it has happened yet when the answer is handed over.
 */
internal sealed interface Checkout {

    val entry: PoolEntry

    /** One that was already open and is free again. */
    class Reuse(override val entry: PoolEntry) : Checkout

    /** Nothing was on the shelf, so this one has to be opened. */
    class Dial(override val entry: PoolEntry) : Checkout
}
