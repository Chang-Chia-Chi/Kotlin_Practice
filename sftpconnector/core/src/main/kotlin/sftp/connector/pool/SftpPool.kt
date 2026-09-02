package sftp.connector.pool

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.error.LeaseFate
import sftp.connector.error.SftpException
import sftp.connector.transport.SftpConnection
import sftp.connector.transport.SftpTransport
import java.util.concurrent.atomic.AtomicBoolean

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
) {

    private val registry = SessionRegistry()

    /**
     * The bound on everything. Held from before an entry exists until after it is handed back, so
     * a session being opened occupies capacity just as much as one being used - which is what
     * stops a burst of callers from all deciding at once that the pool looks empty.
     */
    private val capacity = Semaphore(config.pool.maxSize)

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
     * Borrows a session, waiting for one to come free if the pool is at its limit.
     *
     * For callers that cannot express their work as one block - a lease held across a handover, or
     * released by something other than the code that took it. Everyone else wants [withLease].
     */
    suspend fun acquire(): Lease {
        capacity.acquire()
        var claimed: PoolEntry? = null
        try {
            val checkout = registry.checkOut()
            claimed = checkout.entry
            if (checkout is Checkout.Dial) {
                registry.filled(checkout.entry, transport.connect())
            }
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
                capacity.release()
            }
            throw failure
        }
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
            capacity.release()
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
     */
    suspend fun releaseAfter(failure: Throwable): Unit =
        giveBack(healthy = failure is SftpException && failure.disposition.lease == LeaseFate.RETURNED)

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
