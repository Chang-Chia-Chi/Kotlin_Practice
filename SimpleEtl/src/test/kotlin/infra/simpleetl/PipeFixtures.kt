package infra.simpleetl

import org.assertj.core.api.Assertions.assertThat
import org.duckdb.DuckDBConnection
import org.jdbi.v3.core.ConnectionFactory
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.sql.CallableStatement
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

/**
 * P3 test support. Written for this phase rather than shared with P1's `Duck` or P2's `Scratch`,
 * both of which belong to phases that may not be edited.
 *
 * Source datasets are built with `CREATE TABLE AS SELECT ... FROM range(n)`: no INSERT into
 * DuckDB (non-negotiable rule 1), no appender inside a fixture, and a million rows cost one
 * statement. Nothing here DELETEs, TRUNCATEs or DROPs a DuckDB dataset (spec 5.5), and nothing
 * creates a TEMP table (spec 7.2).
 */
object Pipe {

    /** The step name every P3 test pipes under, so an error message can be asserted to name it. */
    const val STEP = "load-wip"

    fun openDuck(url: String = "jdbc:duckdb:"): DuckDBConnection =
        DriverManager.getConnection(url) as DuckDBConnection

    fun exec(connection: Connection, vararg sql: String) =
        connection.createStatement().use { statement -> sql.forEach { statement.execute(it) } }

    /**
     * A source table of [rows] rows: `lot_id` BIGINT, `lot_code` VARCHAR, `qty` DECIMAL(18,3),
     * `site` VARCHAR. DuckDB reports every column nullable, so an AUTO target creates all four
     * as null-accepting types (spec 4.6) and no column needs a CAST at the target.
     */
    fun createSourceTable(connection: Connection, table: String, rows: Int, site: String = "F12") =
        exec(
            connection,
            """
            create table $table as
            select cast(i as bigint)              as lot_id,
                   cast('L' || i as varchar)      as lot_code,
                   cast(i * 1.5 as decimal(18,3)) as qty,
                   cast('$site' as varchar)       as site
            from range(0, $rows) t(i)
            """,
        )

    fun rowCount(connection: Connection, table: String): Long =
        scalar(connection, "select count(*) from $table") { it.getLong(1) }

    fun tableExists(connection: Connection, table: String): Boolean =
        scalar(
            connection,
            "select count(*) from information_schema.tables where table_name = '$table'",
        ) { it.getLong(1) } == 1L

    fun strings(connection: Connection, sql: String): List<String?> = column(connection, sql) { it.getString(1) }

    fun longs(connection: Connection, sql: String): List<Long> = column(connection, sql) { it.getLong(1) }

    private fun <T> scalar(connection: Connection, sql: String, read: (ResultSet) -> T): T =
        column(connection, sql, read).first()

    private fun <T> column(connection: Connection, sql: String, read: (ResultSet) -> T): List<T> =
        connection.createStatement().use { statement ->
            statement.executeQuery(sql).use { rs ->
                val out = ArrayList<T>()
                while (rs.next()) out.add(read(rs))
                out
            }
        }

    /**
     * Live heap after a forced collection: what the JVM still holds once garbage is gone. Used
     * only to compare two runs of the same shape, never as an absolute budget. `System.gc()` is
     * a request rather than a command, so it is repeated, and no test asserts on one reading.
     */
    fun liveHeapBytes(): Long {
        repeat(4) { System.gc() }
        val runtime = Runtime.getRuntime()
        return runtime.totalMemory() - runtime.freeMemory()
    }
}

/** Thrown by [ProbeWriter], so a test can assert the pipe propagated this exact failure. */
class TargetFailure(message: String) : RuntimeException(message)

/**
 * A [RowWriter] test double that records the pipe's use of the write seam and can fail on
 * demand, optionally in front of a real writer.
 *
 * [failOnChunk] fails *mid-chunk* rather than before it: half the chunk is handed to the
 * delegate, which flushes it, and only then does the writer throw. That is the shape done-when
 * item 5 names - the target threw with part of the chunk already committed - and it is the shape
 * P2 measured as retaining every flushed row.
 *
 * [failOnClose] fails from `close` instead, which is the case where two failures race to be the
 * one the caller sees. The pipe must report the failure that lost the data and carry the close
 * failure as a suppressed exception, not the other way round.
 *
 * @param delegate a real writer to pass through to, or null to record without writing anything.
 * @param failOnChunk the 1-based chunk on which [write] throws; 0 never throws.
 */
class ProbeWriter(
    private val delegate: RowWriter? = null,
    private val failOnOpen: Boolean = false,
    private val failOnChunk: Int = 0,
    private val failOnClose: Boolean = false,
) : RowWriter {

    var opens = 0
        private set
    var closes = 0
        private set
    var columnsAtOpen: List<ColumnMeta> = emptyList()
        private set

    private val chunks = ArrayList<Int>()

    /** One entry per [write] call, in order, so chunking is observable without a database. */
    val chunkSizes: List<Int> get() = chunks

    val rowsSeen: Int get() = chunks.sum()

    override fun open(columns: List<ColumnMeta>) {
        opens++
        columnsAtOpen = columns
        if (failOnOpen) throw TargetFailure("step '" + Pipe.STEP + "': the target refused to open.")
        delegate?.open(columns)
    }

    override fun write(chunk: List<Row>): Int {
        chunks.add(chunk.size)
        if (chunks.size == failOnChunk) {
            val landed = chunk.take(chunk.size / 2)
            if (landed.isNotEmpty()) delegate?.write(landed)
            throw TargetFailure(
                "step '" + Pipe.STEP + "': the target threw after " + landed.size +
                    " rows of chunk " + failOnChunk + ".",
            )
        }
        return delegate?.write(chunk) ?: chunk.size
    }

    override fun close() {
        closes++
        delegate?.close()
        if (failOnClose) throw TargetFailure("step '" + Pipe.STEP + "': the target threw from close.")
    }
}

/**
 * The leak counter for the source side of the pipe: a [ConnectionFactory] handing Jdbi proxied
 * connections, statements and result sets, counting opens against closes, and recording what the
 * pipe did to the source statement's fetch size.
 *
 * Two fetch-size records, because the drivers differ. [fetchSizesRequested] is every argument
 * passed to `setFetchSize`, captured whether or not the driver honours it - duckdb_jdbc 1.1.3
 * accepts the call and goes on reporting its own 2048, measured on the pinned jar.
 * [fetchSizesAtExecute] is what the statement reported at the moment it was executed, which is
 * the reading that matters on Oracle, whose default is 10.
 *
 * `DatabaseMetaData` is deliberately not proxied: the source path never reads a catalog, so
 * counting a result set a driver opens for its own reasons would only invent a leak.
 *
 * Why this is not P2's `CountingConnections`: that counter has no fetch-size hook, and done-when
 * item 2 needs one. Adding it would mean editing a file P2 froze, so the hook lives here instead.
 * Everything else about the two is the same idea, and the next phase should extend this one
 * rather than write a third.
 */
class RecordingConnections(private val open: () -> Connection) : ConnectionFactory {

    val connectionsOpened = AtomicInteger()
    val connectionsClosed = AtomicInteger()
    val statementsOpened = AtomicInteger()
    val statementsClosed = AtomicInteger()
    val resultSetsOpened = AtomicInteger()
    val resultSetsClosed = AtomicInteger()

    val fetchSizesRequested: MutableList<Int> = Collections.synchronizedList(ArrayList())
    val fetchSizesAtExecute: MutableList<Int> = Collections.synchronizedList(ArrayList())

    override fun openConnection(): Connection {
        connectionsOpened.incrementAndGet()
        return proxy(open(), arrayOf(Connection::class.java), connectionsClosed) as Connection
    }

    /** Opens must be non-zero as well as balanced, so a pipe that never queried cannot pass. */
    fun assertStreamed(path: String) {
        assertThat(connectionsOpened.get()).describedAs("source connections opened, %s", path).isPositive()
        assertThat(statementsOpened.get()).describedAs("source statements opened, %s", path).isPositive()
        assertThat(resultSetsOpened.get()).describedAs("source result sets opened, %s", path).isPositive()
        assertNothingLeaked(path)
    }

    fun assertNothingLeaked(path: String) {
        assertThat(connectionsClosed.get()).describedAs("source connections closed, %s", path)
            .isEqualTo(connectionsOpened.get())
        assertThat(statementsClosed.get()).describedAs("source statements closed, %s", path)
            .isEqualTo(statementsOpened.get())
        assertThat(resultSetsClosed.get()).describedAs("source result sets closed, %s", path)
            .isEqualTo(resultSetsOpened.get())
    }

    private fun proxy(real: Any, faces: Array<Class<*>>, closes: AtomicInteger?): Any =
        Proxy.newProxyInstance(javaClass.classLoader, faces, Counting(real, closes))

    /**
     * A second close() of the same object counts once, so a defensive double close cannot skew a
     * count. Only a Connection's Statements and a Statement's ResultSets are wrapped, so
     * `ResultSet.getStatement()` cannot count the same statement twice.
     */
    private inner class Counting(private val real: Any, private val closes: AtomicInteger?) : InvocationHandler {

        private var counted = false

        override fun invoke(proxy: Any, method: Method, args: Array<out Any?>?): Any? {
            if (method.name == "setFetchSize" && args != null) fetchSizesRequested.add(args[0] as Int)
            val result = try {
                method.invoke(real, *(args ?: emptyArray()))
            } catch (e: InvocationTargetException) {
                throw e.targetException
            }
            if (method.name == "close" && !counted && closes != null) {
                counted = true
                closes.incrementAndGet()
            }
            if (real is Statement && method.name.startsWith("execute")) fetchSizesAtExecute.add(real.fetchSize)
            return when {
                real is Connection && result is Statement -> countStatement(result)
                real is Statement && result is ResultSet -> countResultSet(result)
                else -> result
            }
        }
    }

    private fun countStatement(real: Statement): Statement {
        statementsOpened.incrementAndGet()
        val faces: Array<Class<*>> = when (real) {
            is CallableStatement -> arrayOf(CallableStatement::class.java)
            is PreparedStatement -> arrayOf(PreparedStatement::class.java)
            else -> arrayOf(Statement::class.java)
        }
        return proxy(real, faces, statementsClosed) as Statement
    }

    private fun countResultSet(real: ResultSet): ResultSet {
        resultSetsOpened.incrementAndGet()
        return proxy(real, arrayOf(ResultSet::class.java), resultSetsClosed) as ResultSet
    }
}
