package sftp.connector.pool

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.error.Attempt
import sftp.connector.error.LeaseFate
import sftp.connector.error.PoolExhausted
import sftp.connector.error.SftpException
import sftp.connector.transport.SftpConnection
import sftp.connector.transport.SftpTransport
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext

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
) {

    /**
     * Callers that found the pool full and are queued for room. A caller served straight away is
     * never one of these, so the count is a measure of contention rather than of traffic.
     */
    private val waiting = AtomicInteger()

    private val registry = SessionRegistry { waiting.get() }

    private val endpoint = "${config.endpoint.host}:${config.endpoint.port}"

    private val meters = PoolMeters(meterRegistry, endpoint) { registry.lastCount }

    /**
     * The bound on everything. Held from before an entry exists until after it is handed back, so
     * a session being opened occupies capacity just as much as one being used - which is what
     * stops a burst of callers from all deciding at once that the pool looks empty.
     */
    private val capacity = Semaphore(config.pool.maxSize)

    private val acquireTimeout = config.pool.acquireTimeout

    /**
     * Every time room came free over the pool's life, whether a session was given back or one
     * that never opened stopped taking up space. A waiter reads it twice and reports the
     * difference, which is what separates a pool that is short from one that is stuck.
     */
    private val roomFreed = AtomicLong()

    /** What the pool holds right now. One consistent reading, not three separate ones. */
    suspend fun stats(): PoolStats = registry.stats()

    /**
     * Borrows a session, and gives it back however [block] ends.
     *
     * This is the way to use the pool. A lease released by hand is released on one path and
     * forgotten on another, and a session that is never handed back is capacity the pool has lost
     * for the life of the process.
     */
    suspend fun <T> withLease(block: suspend (Lease) -> T): T {
        val lease = acquire()
        return try {
            block(lease).also { lease.release() }
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
     *   moment and what that means.
     */
    suspend fun acquire(): Lease {
        admit()
        var claimed: PoolEntry? = null
        try {
            val checkout = registry.checkOut()
            claimed = checkout.entry
            if (checkout is Checkout.Dial) {
                val opened = transport.connect()
                meters.sessionOpened()
                // Once the session exists the entry has to be told about it whether this caller
                // is still around or not. A connection the pool never recorded is a socket and a
                // reader thread that nothing will ever close.
                withContext(NonCancellable) { registry.filled(checkout.entry, opened) }
            }
            // A caller that has already been cancelled will not release what it is handed, so it
            // is turned away here instead - while the pool can still put the session back itself.
            coroutineContext.ensureActive()
            return Lease(
                this,
                checkout.entry,
                checkNotNull(checkout.entry.connection) { "${checkout.entry} was lent out without a connection" },
            )
        } catch (failure: Throwable) {
            // Cancellation lands here too, and the permit has to go back on that path as much as
            // on any other. Under NonCancellable because giving it back means taking the
            // registry's lock, and a cancelled coroutine cannot wait for a lock.
            withContext(NonCancellable) {
                claimed?.let { discard(it) }
                freeRoom()
            }
            throw failure
        }
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
            try {
                if (withTimeoutOrNull(acquireTimeout) { capacity.acquire() } == null) {
                    meters.turnedAway()
                    throw PoolExhausted(
                        attempt = Attempt(endpoint, "acquire"),
                        stats = stats(),
                        waited = acquireTimeout,
                        roomFreedWhileWaiting = roomFreed.get() - freedBefore,
                    )
                }
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

    /** Takes an entry out of the pool for good and closes whatever it was holding. */
    private suspend fun discard(entry: PoolEntry) {
        registry.handBack(entry, healthy = false)?.let { close(it, entry) }
        registry.closed(entry)
    }

    internal suspend fun giveBack(entry: PoolEntry, healthy: Boolean): Unit = withContext(NonCancellable) {
        try {
            if (healthy) registry.handBack(entry, healthy = true) else discard(entry)
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
    private val entry: PoolEntry,
    val connection: SftpConnection,
) {

    private val handedBack = AtomicBoolean(false)

    /**
     * What the pool currently makes of this session, as it changes. A holder that wants to know
     * its session was thrown away rather than parked can watch this instead of asking afterwards.
     */
    val state: StateFlow<EntryState> get() = entry.state

    /** Gives the session back for the next caller. */
    suspend fun release(): Unit = giveBack(healthy = true)

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
     */
    suspend fun releaseAfter(failure: Throwable): Unit = giveBack(
        healthy = when ((failure as? SftpException)?.disposition?.lease) {
            LeaseFate.RETURNED, LeaseFate.NONE_HELD -> true
            LeaseFate.EVICTED, null -> false
        },
    )

    private suspend fun giveBack(healthy: Boolean) {
        if (!handedBack.compareAndSet(false, true)) {
            LOG.warn(
                "{} was given back twice. The second one is ignored, but the code that did it is " +
                    "holding a lease it no longer owns and may be using a session lent to someone else.",
                entry,
            )
            return
        }
        pool.giveBack(entry, healthy)
    }

    private companion object {
        private val LOG = LoggerFactory.getLogger(Lease::class.java)
    }
}
