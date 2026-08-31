package infra.snapshotcache.spi

import infra.snapshotcache.api.Snapshot
import java.lang.ref.Cleaner
import java.lang.ref.Reference
import java.sql.Connection
import java.time.Instant

/** One shared Cleaner for every handle: a single daemon thread is plenty for a bug-path backstop. */
private val orphanCleaner: Cleaner = Cleaner.create()

/**
 * The [Snapshot] implementation, living at the spi boundary so `core` never names
 * `java.sql` types. `core` constructs it from the lease's
 * [OpenGeneration] and holds it only as [Snapshot]; [onRelease] is core's callback that
 * releases the lease and fires the release-or-orphan event, and is invoked exactly once
 * per handle however close and garbage collection race.
 *
 * [close] is idempotent and closes every connection this handle issued, on every path.
 * If the handle becomes unreachable without close, the shared [Cleaner] runs the same
 * cleanup with `orphaned = true` - a bug signal, never a normal path. The
 * cleanup state deliberately holds no reference back to the handle, or the handle could
 * never become phantom reachable.
 */
internal class SnapshotHandle(
    opened: OpenGeneration,
    override val dataAsOf: Instant,
    onRelease: (orphaned: Boolean) -> Unit,
) : Snapshot {

    override val generation: Long = opened.generation

    private val state = CleanupState(opened, onRelease)
    private val cleanable = orphanCleaner.register(this, state)

    /** Fresh read-only connection into the pinned generation; closed by [close] if the caller forgets. */
    override fun connection(): Connection = state.issue()

    override fun close() {
        try {
            state.explicitClose = true
            cleanable.clean()
        } finally {
            // Keeps `this` reachable until the flag is set and clean() has run, so the
            // Cleaner cannot fire mid-close and misreport an explicit close as an orphan.
            Reference.reachabilityFence(this)
        }
    }

    /**
     * Shared between [close] and the Cleaner. [Cleaner.Cleanable.clean] guarantees [run]
     * executes at most once between the two, so the connections are closed and
     * `onRelease` fires exactly once however the races fall (invariant I6).
     */
    private class CleanupState(
        private val opened: OpenGeneration,
        private val onRelease: (orphaned: Boolean) -> Unit,
    ) : Runnable {

        /** Set by [SnapshotHandle.close] before clean(); still false when the Cleaner got there first. */
        @Volatile
        var explicitClose = false

        /** Guarded by `synchronized(this)`; null marks the handle closed. */
        private var issued: MutableList<Connection>? = mutableListOf()

        fun issue(): Connection = synchronized(this) {
            val open = checkNotNull(issued) { "snapshot is closed" }
            // A long-lived withSnapshot job may issue and close thousands of connections
            // before its lease ends; only the ones still open need closing at cleanup.
            open.removeIf { it.isClosed }
            val connection = opened.connection()
            open += connection
            connection
        }

        override fun run() {
            val toClose = synchronized(this) {
                val open = issued.orEmpty()
                issued = null
                open
            }
            toClose.forEach { connection ->
                runCatching { connection.close() }
            }
            onRelease(!explicitClose)
        }
    }
}
