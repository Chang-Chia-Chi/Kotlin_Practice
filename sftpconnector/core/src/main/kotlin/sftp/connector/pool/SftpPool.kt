package sftp.connector.pool

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.error.Attempt
import sftp.connector.error.CurrentAttempt
import sftp.connector.error.LeaseFate
import sftp.connector.error.PoolExhausted
import sftp.connector.error.SftpException
import sftp.connector.transport.SftpConnection
import sftp.connector.transport.SftpSession
import sftp.connector.transport.SftpTransport
import java.time.Clock
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * A bounded set of sessions to one server, lent out one caller at a time.
 *
 * Opening a session to this server costs a tunnel through the proxy, a key exchange, an
 * authentication and a channel open, and the server forks a process for it besides. So sessions
 * are kept and reused, and the pool's whole job is to make reuse safe: never two callers on one
 * session, never more sessions than the server was promised, and never a session handed on after
 * something proved it was no longer trustworthy.
 *
 * Capacity is a semaphore taken before anything else, so a caller that cannot be served waits at
 * the door rather than inside. The registry behind it decides what each caller gets; the network
 * work that decision implies is done here, out in the open, with no lock held.
 */
class SftpPool(
    private val transport: SftpTransport,
    config: SftpConnectorConfig,
    /** Whatever the host supplies; a private one when the connector is used on its own. */
    meterRegistry: MeterRegistry = SimpleMeterRegistry(),
    /** Injected so a test can age a session without the suite waiting half an hour for it. */
    clock: Clock = Clock.systemUTC(),
) {

    /**
     * Callers that found the pool full and are queued for room. A caller served straight away is
     * never one of these, so the count is a measure of contention rather than of traffic.
     */
    private val waiting = AtomicInteger()

    private val settings = config.pool

    private val endpoint = config.endpoint.address

    private val registry = SessionRegistry(settings, endpoint, clock) { waiting.get() }

    private val meters = PoolMeters(meterRegistry, endpoint) { registry.lastCount }

    /**
     * The bound on everything. Held from before an entry exists until after it is handed back, so
     * a session being opened occupies capacity just as much as one being used - which is what
     * stops a burst of callers from all deciding at once that the pool looks empty.
     */
    private val capacity = Semaphore(settings.maxSize)

    private val acquireTimeout = settings.acquireTimeout

    /**
     * Every time room came free over the pool's life, whether a session was given back or one
     * that never opened stopped taking up space. A waiter reads it twice and reports the
     * difference, which is what separates a pool that is short from one that is stuck.
     */
    private val roomFreed = AtomicLong()

    /** What the pool holds right now. One consistent reading, not three separate ones. */
    suspend fun stats(): PoolStats = registry.stats()

    private val ladder = CancellationLadder(settings.cancelGrace)

    /**
     * Borrows a session, and gives it back however [block] ends.
     *
     * This is the way to use the pool. A lease released by hand is released on one path and
     * forgotten on another, and a session that is never handed back is capacity the pool has lost
     * for the life of the process.
     *
     * It is also the only way to be cancelled safely. A cancelled caller leaves behind whatever
     * blocking call it was in the middle of, and that call keeps the session until it stops; the
     * ladder is what makes it stop, within a bound, and what knows whether stopping it cost the
     * session its life.
     */
    suspend fun <T> withLease(block: suspend (Lease) -> T): T {
        val lease = acquire()
        return try {
            ladder.carry(lease.entry) { block(lease) }.also { lease.release() }
        } catch (failure: Throwable) {
            lease.releaseAfter(failure)
            throw failure
        }
    }

    /**
     * Borrows a session, waiting up to the acquire timeout for one to come free.
     *
     * For callers that cannot express their work as one block - a lease held across a handover, or
     * released by something other than the code that took it. Everyone else wants [withLease].
     *
     * @throws PoolExhausted when the wait runs out, carrying what the pool looked like at that
     *   moment and what that means - or at once, with `closing` set, when the pool is closing
     *   and no session is ever coming.
     */
    suspend fun acquire(): Lease {
        // The door is not even queued at while the pool is closing. The wait would end the
        // same way once room came free, only later, and a shutdown is waiting on that room.
        if (registry.closing) throw refusedWhileClosing()
        admit()
        val borrower = Throwable("this is where the session was taken, not a failure")
        var claimed: PoolEntry? = null
        try {
            while (true) {
                val checkout = registry.checkOut(borrower) ?: throw refusedWhileClosing()
                claimed = checkout.entry
                when (checkout) {
                    is Checkout.Dial -> dial(checkout.entry)

                    is Checkout.Prove -> if (!proves(checkout.entry)) {
                        // The room this caller was given stays with it. Losing it here because a
                        // session turned out to be dead would shrink the pool every time the
                        // network dropped one, which is exactly when it is needed most.
                        claimed = null
                        continue
                    }

                    is Checkout.Reuse -> Unit
                }
                // A caller that has already been cancelled will not release what it is handed, so
                // it is turned away here instead - while the pool can still put the session back
                // itself. So is a caller whose pool started closing while its session was being
                // opened or proved: the drain has counted the entry, and the session goes the way
                // of everything else the pool holds rather than out to a caller.
                coroutineContext.ensureActive()
                if (registry.closing) throw refusedWhileClosing()
                return Lease(
                    this,
                    checkout.entry,
                    checkNotNull(checkout.entry.connection) { "${checkout.entry} was lent out without a connection" },
                )
            }
        } catch (failure: Throwable) {
            // Cancellation lands here too, and the permit has to go back on that path as much as
            // on any other. Under NonCancellable because giving it back means taking the
            // registry's lock, and a cancelled coroutine cannot wait for a lock.
            withContext(NonCancellable) {
                claimed?.let { discard(it, Retirement.POISONED) }
                freeRoom()
            }
            throw failure
        }
    }

    /**
     * Opens the session an empty-handed entry is waiting for.
     *
     * Once the session exists the entry has to be told about it whether the caller that asked for
     * it is still around or not. A connection the pool never recorded is a socket and a reader
     * thread that nothing will ever close.
     */
    private suspend fun dial(entry: PoolEntry) {
        val opened = transport.connect()
        meters.sessionOpened()
        withContext(NonCancellable) {
            // A pool that has written the entry off in the meantime - a shutdown that ran out of
            // patience before the handshake finished - wants nothing opened for it, and this is
            // the only place that knows the session exists.
            registry.filled(entry, opened)?.let { finish(it) }
        }
    }

    private suspend fun refusedWhileClosing() = exhausted(closing = true)

    /**
     * The refusal, carrying the counts as they are the moment it is built.
     *
     * They are copied into the exception rather than referred to from it. What a refused caller
     * needs is what the pool looked like at the instant it gave up, and by the time the failure is
     * read the pool has moved on; and a failure that travels up a stack and into a log carrying a
     * pool type is one that cannot be caught without knowing the pool exists.
     */
    private suspend fun exhausted(
        waited: Duration = Duration.ZERO,
        roomFreedWhileWaiting: Long = 0,
        closing: Boolean = false,
    ): PoolExhausted {
        val stats = stats()
        return PoolExhausted(
            attempt = queuedAttempt(),
            inUse = stats.inUse,
            connecting = stats.connecting,
            idle = stats.idle,
            pending = stats.pending,
            waited = waited,
            roomFreedWhileWaiting = roomFreedWhileWaiting,
            closing = closing,
        )
    }

    /** The operation that was queued, when the caller said which; the pool's own name for what it was doing otherwise. */
    private suspend fun queuedAttempt(): Attempt = coroutineContext[CurrentAttempt]?.attempt ?: Attempt(endpoint, "acquire")

    /**
     * Asks a parked session whether it is still there, and replaces it when it is not.
     *
     * One round trip against a handshake, a key exchange, an authentication and a forked process
     * on a server that starts refusing connections when too many are half-open: proving a session
     * is the cheap answer and opening one is the expensive one, so the pool proves first and opens
     * only when the answer is no.
     *
     * The round trip goes up the ladder like any other, because it is one: a caller that gives up
     * while the pool is asking a dead server whether a session is still there is a caller waiting
     * on a blocked socket read, and it has no more idea than any other that it is doing so.
     */
    private suspend fun proves(entry: PoolEntry): Boolean {
        val connection = checkNotNull(entry.connection) { "$entry was parked without a session to prove" }
        try {
            ladder.carry(entry) { connection.realpath(".") }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (failure: Exception) {
            LOG.debug("{} no longer answers and is being replaced before the caller sees it: {}", entry, failure.message)
            withContext(NonCancellable) { discard(entry, Retirement.VALIDATION) }
            return false
        }
        registry.proved(entry)
        return true
    }

    /**
     * Waits for room, or explains why there was none.
     *
     * A caller that queues forever is worse than one that fails: the poll it belongs to never
     * ends, the next tick piles up behind it, and the log says nothing at all. So the wait is
     * bounded, and what it cost is measured - both how long the successful ones took, and how
     * the pool looked to the one that gave up.
     */
    private suspend fun admit() {
        val queued = meters.startWaiting()
        // Taken without queueing, which is what happens on a pool that is not full. Counting such
        // a caller among the waiters would leave the pending gauge ticking on a healthy pool, and
        // a gauge that moves when nothing is wrong is one nobody can alert on.
        if (!capacity.tryAcquire()) {
            val freedBefore = roomFreed.get()
            waiting.incrementAndGet()
            // Whether the permit was taken is read off this flag and never off the wait's own
            // answer. The semaphore gives a permit back itself when the caller is cancelled while
            // waiting for it, but not when the cancellation lands in the instant after it was
            // granted: the wait then throws with the permit already taken and its answer thrown
            // away, and a permit lost there is capacity the pool has lost until restart.
            var granted = false
            try {
                withTimeoutOrNull(acquireTimeout) {
                    capacity.acquire()
                    granted = true
                }
                if (!granted) {
                    meters.turnedAway()
                    // Read while this caller still counts among the waiters: the statistics
                    // describe the pool as the refused caller found it, itself included.
                    throw exhausted(
                        waited = acquireTimeout,
                        roomFreedWhileWaiting = roomFreed.get() - freedBefore,
                    )
                }
            } catch (failure: Throwable) {
                if (granted) freeRoom()
                throw failure
            } finally {
                waiting.decrementAndGet()
            }
        }
        meters.admitted(queued)
    }

    /**
     * Gives back the room one caller was occupying. Both paths that let a caller go run through
     * here, so the count of rooms that came free cannot drift from the permits that did.
     */
    private fun freeRoom() {
        roomFreed.incrementAndGet()
        capacity.release()
    }

    /**
     * Stops lending, lets what is out come back, cuts what does not, and hangs up on everything.
     *
     * Returns within the drain timeout plus one cancel grace, whatever the sessions are doing,
     * and leaves every entry closed. Acquire fails at once from the first instant; a session
     * handed back from then on is retired as `shutdown` rather than shelved. The drain is the
     * time a caller mid-operation is given to finish on its own. What is still out after it is
     * cut apart - all of it at once, since a cut is a socket close that does not wait for
     * anything - and given one grace to hand its session back the way a cut call does, through
     * the failure the cut raises. What has not come back even then is written off: its session
     * was destroyed by the cut, and its holder finds nothing left to decide when it finally
     * hands the entry back.
     *
     * The cut comes before the orderly hang-ups, and not only for the sake of the bound: the
     * hang-ups run on the transport's IO dispatcher, which is exactly as wide as this pool, and a
     * pool whose every session is blocked is a dispatcher with no thread free to hang up on
     * anything. Cutting frees those threads first. The hang-ups then run side by side, so a
     * peer that makes one of them slow is paid for once rather than once per session.
     *
     * The call cannot be cancelled, and does not need to be: it is bounded already, and a pool
     * left half-closed is sockets and threads for the life of the process. A caller that has
     * been cancelled by the time it calls this still closes everything.
     */
    suspend fun close(): Unit = withContext(NonCancellable) {
        registry.beginClosing()
        // Counted before the wait, because after it the number is gone and a shutdown nobody can
        // reconstruct is a shutdown nobody can tell from a hang: a minute of silence reads the
        // same whether one lease was out or forty, and whether they came back or were cut.
        val outWhenTheDrainBegan = registry.held().size
        val drained = settled(within = settings.drainTimeout)
        if (!drained) {
            cutEverythingHeld()
            settled(within = settings.cancelGrace)
        }
        val retired = registry.closeEverything()
        coroutineScope { retired.forEach { launch { finish(it) } } }
        LOG.info(
            "The pool to {} is closed. {} lease(s) were out when the drain began; it {}; {} session(s) were hung up on.",
            endpoint,
            outWhenTheDrainBegan,
            if (drained) "settled within the ${settings.drainTimeout} allowed" else "did not settle in ${settings.drainTimeout}, so what was still out was cut",
            retired.size,
        )
    }

    /** Waits, up to [within], for every session to be back on the shelf and every retired one hung up on. */
    private suspend fun settled(within: Duration): Boolean = withTimeoutOrNull(within) {
        while (!registry.isQuiet()) delay(SETTLE_POLL)
    } != null

    private suspend fun cutEverythingHeld() {
        val held = registry.held()
        if (held.isEmpty()) return
        LOG.warn(
            "{} still out when the pool to {} had to close; each is being cut apart to get its thread back, " +
                "and its operation will fail with a lost session.",
            held.size,
            endpoint,
        )
        held.forEach { it.cutLoose() }
    }

    /**
     * Keeps the pool in the shape its configuration describes, for as long as the coroutine
     * running it lives. Cancel it to stop.
     *
     * A pool nobody is asking anything of still drifts: sessions age past the lifetime they were
     * given, spares sit until the proxy quietly drops them, and a caller that took a session and
     * never gave it back leaves the pool one smaller with nothing in the log to say so. This is
     * what notices. It belongs to whoever owns the connector's scope, because a coroutine started
     * by a constructor is one nothing can stop.
     */
    suspend fun housekeep(): Nothing {
        while (true) {
            delay(settings.housekeepingInterval)
            try {
                sweep()
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (failure: Exception) {
                // One bad round is not a reason to stop looking after the pool for the rest of
                // the process's life, which is what letting this out would mean.
                LOG.warn("A housekeeping round failed and the next one will run as usual: {}", failure.message, failure)
            }
        }
    }

    /**
     * One round: retire what has aged out, report what has been held too long, and open whatever
     * the pool is short of. Every decision is taken in one pass under the lock and carried out
     * here, where nothing is holding it.
     */
    private suspend fun sweep() {
        val round = registry.sweep(capacity::tryAcquire)
        round.leaking.forEach { report(it) }
        // The spares the round reserved are dialled before its retired sessions are hung up on,
        // not after. A retired session holds no pool place - an idle one gave its permit back when
        // it was shelved - so its hang-up queues for an IO thread behind the leases in flight, and
        // while it waits the reserved spares would sit registered as `Connecting` with their
        // permits taken: a caller refused meanwhile would read "most of the pool is stuck opening
        // sessions" for a pool that is in fact stuck hanging up. Dialling first means the spares
        // are `Connecting` only while they really are being opened, and the retired hang-ups run
        // after, when nothing is reserved.
        //
        // Both the hang-ups and the reservations have to survive this coroutine being cancelled -
        // which is what a shutdown does to it - so they run in the finally under NonCancellable: a
        // retired session left unclosed keeps its socket for the life of the process, and a
        // reserved spare not dialled holds room that only being given back ever frees. The spares'
        // rooms are given back first, then the retired sessions are closed, so an Error out of a
        // hang-up cannot strand a permit.
        val toOpen = ArrayDeque(round.toOpen)
        try {
            while (toOpen.isNotEmpty()) openForTheShelf(toOpen.removeFirst())
        } finally {
            withContext(NonCancellable) {
                toOpen.forEach { giveBack(it, Retirement.POISONED) }
                round.retired.forEach { finish(it) }
            }
        }
    }

    /**
     * Opens a session the pool decided it wanted, and parks it for whoever asks next. It holds
     * the room it reserved until it is on the shelf or given up on.
     */
    private suspend fun openForTheShelf(entry: PoolEntry) {
        try {
            dial(entry)
            giveBack(entry, null)
        } catch (failure: Throwable) {
            withContext(NonCancellable) { giveBack(entry, Retirement.POISONED) }
            if (failure is CancellationException) throw failure
            LOG.warn(
                "Opening a spare session failed, so the pool is below the {} it keeps ready and will " +
                    "try again next round: {}",
                settings.minIdle,
                failure.message,
            )
        }
    }

    /**
     * Says where a session was taken that nobody has given back. It is a report and not a repair:
     * a call in flight cannot be interrupted from outside without destroying the session under
     * whoever is using it, so taking the lease back would turn a caller that is merely slow into
     * one that fails.
     */
    private fun report(leak: Leak) {
        meters.leaked()
        LOG.warn(
            "{} has been held longer than {}, for {} so far, and is not being taken back - the pool " +
                "is one session smaller until whoever took it gives it back. The stack trace is " +
                "where it was taken.",
            leak.entry,
            settings.leakDetectionThreshold,
            leak.heldForMillis.milliseconds,
            leak.borrower,
        )
    }

    /** Takes an entry out of the pool for good and closes whatever it was holding. */
    private suspend fun discard(entry: PoolEntry, reason: Retirement) {
        registry.handBack(entry, reason)?.let { finish(it) }
    }

    /** Hangs up on a retired session and records why it went. */
    private suspend fun finish(retired: Retired) {
        retired.connection?.let {
            close(it, retired.entry)
            // Counted only where there was a session to lose. An entry whose dial never landed
            // was never a session, and counting it as one would make a server refusing
            // connections look like a pool throwing away good sessions.
            meters.evicted(retired.reason)
        }
        registry.closed(retired.entry)
    }

    internal suspend fun giveBack(entry: PoolEntry, retire: Retirement?): Unit = withContext(NonCancellable) {
        try {
            registry.handBack(entry, retire)?.let { finish(it) }
        } finally {
            // Last, on both paths. A waiter woken before the session is back on the shelf would
            // find a pool that says it has room and does not.
            freeRoom()
        }
    }

    private suspend fun close(connection: SftpConnection, entry: PoolEntry) {
        try {
            connection.close()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (failure: Exception) {
            // The session is being thrown away regardless; a hang-up that fails changes nothing
            // except that the socket now closes when the process does. An Error is left alone:
            // whatever is wrong with the JVM is not this method's to absorb.
            LOG.warn("Hanging up {} failed, and it is being dropped anyway: {}", entry, failure.message)
        }
    }

    private companion object {
        private val LOG = LoggerFactory.getLogger(SftpPool::class.java)

        /**
         * How often a closing pool looks whether everything is back. A look is one uncontended
         * lock; nothing is signalled from under the registry's lock instead, so that nothing can
         * ever be resumed while it is held.
         */
        private val SETTLE_POLL = 20.milliseconds
    }
}

/**
 * One borrowed session, and the right to give it back exactly once.
 *
 * The lease never asks its holder what state the session is in. A failure already answers that -
 * it is one of the four things every failure decides - so the holder passes the failure and the
 * pool obeys it. Nothing is left for a caller to judge, and so nothing is left for a caller to
 * judge differently from the caller next door.
 */
class Lease internal constructor(
    private val pool: SftpPool,
    internal val entry: PoolEntry,
    /**
     * The operations, and nothing about the session's life: ending it - orderly or by force - is
     * the pool's alone, whether the session is on the shelf or out on this lease.
     */
    val connection: SftpSession,
) {

    private val handedBack = AtomicBoolean(false)

    /**
     * What the pool currently makes of this session, as it changes. A holder that wants to know
     * its session was thrown away rather than parked can watch this instead of asking afterwards.
     */
    val state: StateFlow<EntryState> get() = entry.state

    /**
     * Gives the session back for the next caller, or for retirement if the pool has finished with
     * it. A session that outlived its lifetime while this caller held it goes no further: the pool
     * decides that, because the holder has no way of knowing.
     */
    suspend fun release(): Unit = giveBack(retire = null)

    /**
     * Gives the session back after something went wrong, keeping it only if [failure] says the
     * session itself is still sound. Anything the connector did not classify - an application
     * error, a cancellation, an `Error` - leaves a session nobody has vouched for, and the pool
     * would rather pay for a handshake than hand that on.
     *
     * A failure that says it held no session at all keeps this one. It is saying that whatever
     * went wrong was not about a session, and the way such a failure reaches a lease holder is a
     * second acquire failing inside the first - the exact moment when destroying a healthy
     * session would make the shortage that caused it worse.
     *
     * A cancellation is the one thing that is not a failure at all: it says a caller stopped
     * waiting and nothing whatever about the session. What says something is whether getting the
     * caller's thread back cost the session its life, and the pool records that when it does it.
     */
    suspend fun releaseAfter(failure: Throwable): Unit = giveBack(
        retire = if (failure is CancellationException) {
            Retirement.POISONED.takeIf { entry.unfitAfterCancelling }
        } else {
            when ((failure as? SftpException)?.disposition?.lease) {
                LeaseFate.RETURNED, LeaseFate.NONE_HELD -> null
                LeaseFate.EVICTED, null -> Retirement.POISONED
            }
        },
    )

    private suspend fun giveBack(retire: Retirement?) {
        if (!handedBack.compareAndSet(false, true)) {
            LOG.warn(
                "{} was given back twice. The second one is ignored, but the code that did it is " +
                    "holding a lease it no longer owns and may be using a session lent to someone else. " +
                    "The stack trace is the second hand-back.",
                entry,
                // Nothing is thrown: the stack is the only thing that names the caller, and the
                // line describes a bug that cannot be found without it.
                IllegalStateException("$entry was given back a second time here"),
            )
            return
        }
        pool.giveBack(entry, retire)
    }

    private companion object {
        private val LOG = LoggerFactory.getLogger(Lease::class.java)
    }
}
