package sftp.connector.pool

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import sftp.connector.config.PoolConfig
import sftp.connector.transport.SftpConnection
import java.time.Clock
import kotlin.random.Random

/**
 * Every session the pool has created and not yet finished with, and the one lock over all of it.
 *
 * This class decides; it never acts. Each of its methods answers a question - which session should
 * this caller get, may this one go back on the shelf, what is worth doing this minute - and returns
 * before anything slow happens. The slow part is the caller's to do afterwards, and by then the
 * lock is long gone.
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
internal class SessionRegistry(
    private val config: PoolConfig,
    private val clock: Clock,
    /**
     * How many callers are queued for a session right now. The registry cannot see them - they
     * hold nothing it is keeping track of - but a count of what the pool holds that left them out
     * would describe a full pool and a busy one identically.
     */
    private val pendingWaiters: () -> Int,
) {

    private val mutex = Mutex()

    /** Everything alive: connecting, idle, lent out or being checked. Insertion-ordered for logs. */
    private val entries = LinkedHashSet<PoolEntry>()

    /**
     * The idle ones, newest at the end. Taking from the end reuses the session whose socket was
     * proved good most recently, and leaves the coldest one to age out.
     */
    private val idle = ArrayDeque<PoolEntry>()

    private var sessionsCreated = 0L

    @Volatile
    private var published = PoolStats(idle = 0, inUse = 0, connecting = 0)

    /**
     * The last count taken, answered without waiting for the lock.
     *
     * A metrics gauge is sampled from a thread that cannot suspend and must not be made to wait
     * on the pool, so it reads this instead of asking. Nothing here goes stale between two of the
     * methods below, because nothing else changes what they count: the reading is not out of
     * date, it is simply still true. Only the waiters move on their own, so they are counted
     * fresh on every read.
     */
    val lastCount: PoolStats get() = published.copy(pending = pendingWaiters())

    /**
     * Claims a session for one caller and says what has to happen before it can be used.
     *
     * The claim is what makes an entry exclusive: it leaves the idle deque here and comes back
     * only when it is handed back, so two callers cannot be looking at the same session.
     *
     * A session that has been parked for a while is claimed but not yet handed over, because
     * nothing on this network says when a tunnel is dropped and a session that looks open can
     * turn out not to be. [borrower] is where the claim was made, kept in case the caller never
     * gives the session back.
     */
    suspend fun checkOut(borrower: Throwable): Checkout = mutex.withLock {
        val now = clock.millis()
        val warm = idle.removeLastOrNull()
        val checkout = if (warm == null) {
            val fresh = PoolEntry(++sessionsCreated, now + lifetime())
            entries += fresh
            fresh.claimedBy(borrower, now)
            Checkout.Dial(fresh)
        } else {
            warm.claimedBy(borrower, now)
            if (now - warm.idleSince >= config.validationBypass.inWholeMilliseconds) {
                warm.moveTo(EntryState.Validating)
                Checkout.Prove(warm)
            } else {
                warm.moveTo(EntryState.InUse)
                Checkout.Reuse(warm)
            }
        }
        recount()
        checkout
    }

    /** The session opened. */
    suspend fun filled(entry: PoolEntry, connection: SftpConnection) = mutex.withLock {
        entry.connection = connection
        entry.moveTo(EntryState.InUse)
        recount()
    }

    /** The session answered, so it is sound and the caller may have it. */
    suspend fun proved(entry: PoolEntry) = mutex.withLock {
        entry.moveTo(EntryState.InUse)
        recount()
    }

    /**
     * Takes [entry] back, either because its holder is finished with it or because [failed] says
     * what went wrong with it.
     *
     * A session that has outlived its lifetime is retired here even when the caller had no
     * complaint, and that is the only reason this method needs the clock. Handing back is the one
     * way onto the shelf, so a session past its lifetime cannot reach another caller by any route.
     *
     * Returns the session to hang up on, or null when it went back on the shelf. Returning it
     * rather than closing it is how the one piece of I/O a handback can imply gets carried out of
     * the lock.
     */
    suspend fun handBack(entry: PoolEntry, failed: Retirement?): Retired? = mutex.withLock {
        val now = clock.millis()
        val reason = failed ?: Retirement.LIFETIME.takeIf { now >= entry.expiresAt }
        val outcome = if (reason == null) {
            entry.moveTo(EntryState.Idle)
            entry.idleSince = now
            entry.borrower = null
            idle.addLast(entry)
            null
        } else {
            retire(entry, reason)
        }
        recount()
        outcome
    }

    /** The connection is closed and the entry is finished. */
    suspend fun closed(entry: PoolEntry) = mutex.withLock {
        entry.moveTo(EntryState.Closed)
        recount()
    }

    /**
     * Decides one round of housekeeping in a single pass: what to hang up on, who has been
     * holding a session too long, and how many more to open.
     *
     * All of that is one decision because it is taken against one reading of the pool. Retiring
     * sessions and then asking separately how many are left would read a pool that had moved in
     * between, and the answer would be a top-up for a shape the pool was never in.
     *
     * [takeRoom] reserves capacity for one session the pool means to open on its own initiative,
     * and answers false when there is none to spare. A session opened for the shelf holds that
     * room until it is parked, so a caller cannot slip in and open one beyond what the server was
     * promised while this one is still being dialled.
     *
     * It runs with the lock held, which is safe for the same reason nothing else here is dangerous:
     * it cannot suspend. Every operation this connector can perform against a server is a suspend
     * function, so the type of this parameter is what makes a round trip from inside the lock
     * impossible to write rather than merely unwise.
     */
    suspend fun sweep(takeRoom: () -> Boolean): Housekeeping = mutex.withLock {
        val now = clock.millis()
        val retired = mutableListOf<Retired>()
        var spares = idle.size

        // Front to back is coldest first, so the sessions that go are the ones that have been
        // sitting longest, and the minimum is made up of the freshest ones the pool has.
        idle.toList().forEach { entry ->
            val reason = when {
                now >= entry.expiresAt -> Retirement.LIFETIME
                spares > config.minIdle && now - entry.idleSince >= config.idleTimeout.inWholeMilliseconds ->
                    Retirement.IDLE

                else -> return@forEach
            }
            retired += retire(entry, reason)
            spares--
        }

        // The stack trace is taken here rather than read afterwards, because the caller could give
        // the session back in between and a leak report without the trace that took it says only
        // what the pool's own numbers already said.
        val leaking = entries
            .filter {
                !it.leakReported &&
                    it.state.value in HOLDABLE &&
                    now - it.borrowedAt >= config.leakDetectionThreshold.inWholeMilliseconds
            }
            .map {
                it.leakReported = true
                Leak(it, heldForMillis = now - it.borrowedAt, borrower = it.borrower)
            }

        val toOpen = mutableListOf<PoolEntry>()
        // Bounded twice over: by the minimum the pool was told to keep, and by the room it has.
        // Only the second bound is what keeps the pool inside the size the server agreed to,
        // because the sessions being counted here are not held by any caller.
        while (spares + toOpen.size < config.minIdle && entries.size < config.maxSize) {
            if (!takeRoom()) break
            val fresh = PoolEntry(++sessionsCreated, now + lifetime())
            // The pool is holding this one on its own behalf, and from now: a spare left at the
            // beginning of time would look, to the next round, like a lease nobody gave back.
            fresh.borrowedAt = now
            entries += fresh
            toOpen += fresh
        }

        recount()
        Housekeeping(retired, toOpen, leaking)
    }

    /**
     * One consistent count of everything. Taken under the lock, so the numbers describe a moment
     * that really existed rather than three reads of a moving target.
     */
    suspend fun stats(): PoolStats = mutex.withLock { recount() }

    /**
     * Drops [entry] out of the pool for good. Leaving the deque and going out of service happen
     * together, under one lock, so there is no instant in which a session the pool has given up
     * on is available to anybody.
     */
    private fun retire(entry: PoolEntry, reason: Retirement): Retired {
        entry.moveTo(EntryState.Evicting)
        entries -= entry
        idle.remove(entry)
        entry.borrower = null
        val connection = entry.connection
        entry.connection = null
        return Retired(entry, connection, reason)
    }

    /** This session's own lifetime: the configured one, plus its share of the jitter window. */
    private fun lifetime(): Long {
        val base = config.maxLifetime.inWholeMilliseconds
        val window = (base * config.maxLifetimeJitter).toLong()
        return base + if (window > 0) Random.nextLong(window + 1) else 0
    }

    /** Counts what is here and publishes it. Called only with the lock held. */
    private fun recount(): PoolStats {
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
        published = PoolStats(idle = idle.size, inUse = inUse, connecting = connecting)
        return published.copy(pending = pendingWaiters())
    }

    private companion object {
        /** The states in which some caller is holding the session and could be holding it too long. */
        private val HOLDABLE = setOf(EntryState.InUse, EntryState.Validating)
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

    /** One that was open moments ago and is free again. Hand it straight over. */
    class Reuse(override val entry: PoolEntry) : Checkout

    /** Nothing was on the shelf, so this one has to be opened. */
    class Dial(override val entry: PoolEntry) : Checkout

    /** One that has been parked long enough to be worth a question before it is trusted. */
    class Prove(override val entry: PoolEntry) : Checkout
}

/** A session the pool has finished with: hang up on it, and say why it went. */
internal class Retired(
    val entry: PoolEntry,
    /** Null when there was never a session to close, which is an entry whose dial never landed. */
    val connection: SftpConnection?,
    val reason: Retirement,
)

/** A session out on lease for longer than anyone meant, and the stack trace that took it. */
internal class Leak(val entry: PoolEntry, val heldForMillis: Long, val borrower: Throwable?)

/** One round of housekeeping, decided but not yet carried out. */
internal class Housekeeping(
    val retired: List<Retired>,
    /** Registered and holding room, waiting to be dialled and put on the shelf. */
    val toOpen: List<PoolEntry>,
    /** Held past the leak threshold, and not reported before. */
    val leaking: List<Leak>,
)
