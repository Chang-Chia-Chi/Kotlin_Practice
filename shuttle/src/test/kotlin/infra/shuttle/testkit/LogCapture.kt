package infra.shuttle.testkit

import org.jboss.logmanager.ExtLogRecord
import java.util.Collections
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord

/**
 * Every line the `infra.shuttle` loggers emit while this is open, each with the MDC that was set when it
 * was emitted. Surefire starts the module's JVM on the JBoss LogManager (`java.util.logging.manager` in
 * the pom), which is the backend `org.jboss.logging.Logger` binds to, so a handler on the package's logger
 * receives the real `ExtLogRecord`s and reads the MDC straight off them: no mock, no second backend.
 *
 * The level is pinned to INFO for the life of the capture, so a Quarkus test that reconfigured logging
 * earlier in the same JVM cannot silence what a later one asserts on, and `debug` stays off the console.
 */
class LogCapture : AutoCloseable {

    /** One captured record: [level] is the JBoss level, so compare by [Level.intValue]. */
    data class Line(val level: Level, val message: String, val mdc: Map<String, String>)

    private val lines: MutableList<Line> = Collections.synchronizedList(mutableListOf())
    private val logger = org.jboss.logmanager.Logger.getLogger(PACKAGE)
    private val previousLevel = logger.level

    private val handler = object : Handler() {
        override fun publish(record: LogRecord) {
            val ext = record as? ExtLogRecord
            lines += Line(record.level, ext?.formattedMessage ?: record.message.orEmpty(), ext?.mdcCopy.orEmpty())
        }

        override fun flush() = Unit
        override fun close() = Unit
    }

    init {
        handler.level = Level.ALL
        logger.level = Level.INFO
        logger.addHandler(handler)
    }

    override fun close() {
        logger.removeHandler(handler)
        logger.level = previousLevel
    }

    /** Every captured line, in the order it was logged. */
    fun lines(): List<Line> = synchronized(lines) { lines.toList() }

    /** The captured lines at WARN; JBoss's own `Level.WARN` is not `java.util.logging.Level.WARNING`. */
    fun warnings(): List<Line> = lines().filter { it.level.intValue() == Level.WARNING.intValue() }

    private companion object {
        const val PACKAGE = "infra.shuttle"
    }
}
