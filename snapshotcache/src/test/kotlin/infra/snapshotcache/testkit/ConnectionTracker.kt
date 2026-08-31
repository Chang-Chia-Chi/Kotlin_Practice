package infra.snapshotcache.testkit

import java.lang.reflect.Proxy
import java.sql.Connection
import java.util.Collections

/**
 * Issues minimal fake [Connection]s and tracks each one's closed state together with its
 * creation stack (the JVM-side leak detector, test profile only). Unclosed
 * connections at test end are reported by [AccountingFixture] with the stack that
 * created them, pinpointing the leaking line.
 */
class ConnectionTracker {

    private val issued = Collections.synchronizedList(mutableListOf<TrackedConnection>())

    fun issue(label: String): TrackedConnection = TrackedConnection(label).also { issued.add(it) }

    fun unclosed(): List<TrackedConnection> = synchronized(issued) { issued.filterNot { it.isClosed } }
}

/**
 * Tracking wrapper over a minimal [Connection] stub. Only close/isClosed (plus Object
 * methods) are supported; anything else throws, because the fake store is bookkeeping-only.
 * Later phases that need query behavior stub it at the spi boundary, not here.
 */
class TrackedConnection internal constructor(val label: String) {

    /** Captured at issue time; printed verbatim when the connection is never closed. */
    val creationStack: Throwable = Throwable("connection issued: $label")

    @Volatile
    var isClosed: Boolean = false
        private set

    val connection: Connection = Proxy.newProxyInstance(
        Connection::class.java.classLoader,
        arrayOf(Connection::class.java),
    ) { proxy, method, args ->
        when (method.name) {
            "close" -> {
                isClosed = true
                null
            }
            "isClosed" -> isClosed
            "toString" -> "TrackedConnection($label)"
            "hashCode" -> System.identityHashCode(proxy)
            "equals" -> proxy === args!![0]
            else -> throw UnsupportedOperationException(
                "TrackedConnection($label) does not stub Connection.${method.name}",
            )
        }
    } as Connection
}
