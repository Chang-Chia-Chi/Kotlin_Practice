package infra.simpleetl

import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.ConnectionFactory
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.sql.CallableStatement
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import java.util.concurrent.atomic.AtomicInteger

/**
 * P2 test support. Deliberately separate from P1's `Duck`, which is SELECT-only by design and
 * belongs to a phase that must not be edited.
 *
 * Rows are always built by running a real query through the public [RowMapper], never by
 * reaching for Row's internal constructor: a writer that mishandles a driver's actual value
 * types is exactly the failure this phase exists to catch, and a hand-built Row would hide it.
 *
 * Every DuckDB fixture uses its own connection, so a test cleans up by closing it. Nothing here
 * DELETEs, TRUNCATEs, or DROPs a DuckDB dataset (spec 5.5), and nothing creates a TEMP table.
 */
object Scratch {

    /** The step name every P2 test writes with, so an error message can be asserted to name it. */
    const val STEP = "load-wip"

    /** File mode is a run-lifecycle concern (spec 7.2); a writer test only needs a real engine. */
    fun open(): Connection = DriverManager.getConnection("jdbc:duckdb:")

    fun exec(connection: Connection, vararg sql: String) =
        connection.createStatement().use { statement -> sql.forEach { statement.execute(it) } }

    class Source(val columns: List<ColumnMeta>, val rows: List<Row>) {
        fun column(name: String): ColumnMeta = columns.first { it.name == name }
    }

    /** Runs [sql] and maps every row through the public read seam. */
    fun read(connection: Connection, sql: String, step: String = STEP): Source =
        connection.createStatement().use { statement ->
            statement.executeQuery(sql).use { rs ->
                val mapper = RowMapper(rs.metaData, step)
                val rows = ArrayList<Row>()
                while (rs.next()) rows.add(mapper.map(rs))
                Source(mapper.columns, rows)
            }
        }

    /**
     * The declared type of every column of [table], in catalog order - the reading that the
     * "verified by reading back information_schema" done-when item asks for.
     *
     * duckdb_jdbc 1.1.3 reports data_type with precision and scale included ("DECIMAL(18,3)"),
     * which is what makes a truncated AUTO DDL visible here. It also reports is_nullable
     * correctly, unlike ResultSetMetaData.isNullable, which claims every column is nullable.
     */
    fun declaredTypes(connection: Connection, table: String): List<Pair<String, String>> =
        catalog(connection, table) { it.getString(1) to it.getString(2) }

    fun nullability(connection: Connection, table: String): Map<String, Boolean> =
        catalog(connection, table) { it.getString(1) to (it.getString(3) == "YES") }.toMap()

    fun rowCount(connection: Connection, table: String): Long =
        connection.createStatement().use { statement ->
            statement.executeQuery("select count(*) from $table").use { rs ->
                rs.next()
                rs.getLong(1)
            }
        }

    private fun <T> catalog(connection: Connection, table: String, row: (ResultSet) -> T): List<T> =
        connection.prepareStatement(
            "select column_name, data_type, is_nullable from information_schema.columns " +
                "where table_name = ? order by ordinal_position",
        ).use { statement ->
            statement.setString(1, table)
            statement.executeQuery().use { rs ->
                val out = ArrayList<T>()
                while (rs.next()) out.add(row(rs))
                out
            }
        }
}

/**
 * The leak-counting double for the JDBC writers: a [ConnectionFactory] that hands Jdbi proxied
 * connections, statements, metadata and result sets, and counts opens against closes. All three
 * resources that non-negotiable rule 6 and spec 7.4 name on the JDBC side are counted; the result
 * set arrives through [Connection.getMetaData], which is where a target catalog read opens the
 * only result set P2 production code creates.
 *
 * [assertBalanced] fails when nothing was opened, so the fixture cannot pass by asserting
 * nothing. The failure-path tests are what give it teeth: a writer that only closes on the happy
 * path balances after a successful write and does not balance after an exception.
 *
 * There is no equivalent double for the DuckDB appender. The reason is not that DuckDBAppender
 * resists subclassing - javap on the pinned jar shows a public class implementing AutoCloseable,
 * a public (DuckDBConnection, String, String) constructor and no final methods, so a counting
 * subclass would be trivial. The reason is that nothing can hand one to the writer:
 * DuckDbTableWriter creates its appender internally through DuckDBConnection.createAppender and
 * exposes no injection seam, and DuckDBConnection is public final, so the connection the caller
 * does supply can be neither subclassed nor proxied.
 *
 * Nor is there an observable substitute. Probed on 1.1.3: two appenders on one table are both
 * accepted, close() is idempotent, closing a connection under an open appender is silent, and
 * appending after close crashes the JVM. close() does flush completed but unflushed rows, which
 * would make "was it closed" observable - except that when a row is left half-appended, which is
 * exactly what write() produces when the dispatch throws mid-row, close() discards the whole
 * unflushed buffer including the completed rows. Closed and leaked are indistinguishable on that
 * path, so DuckDbTableWriterRequiredTest proves the exception-path close by the state the writer
 * must leave behind instead.
 */
class CountingConnections(private val open: () -> Connection) : ConnectionFactory {

    val connectionsOpened = AtomicInteger()
    val connectionsClosed = AtomicInteger()
    val statementsOpened = AtomicInteger()
    val statementsClosed = AtomicInteger()
    val resultSetsOpened = AtomicInteger()
    val resultSetsClosed = AtomicInteger()

    override fun openConnection(): Connection {
        connectionsOpened.incrementAndGet()
        return proxy(open(), arrayOf(Connection::class.java), connectionsClosed) { result ->
            when (result) {
                is Statement -> countStatement(result)
                is DatabaseMetaData -> countMetaData(result)
                else -> result
            }
        } as Connection
    }

    /** Both counts must be non-zero, so a writer that never touched the database cannot pass. */
    fun assertBalanced(path: String) {
        assertThat(connectionsOpened.get()).describedAs("connections opened on the %s path", path).isPositive()
        assertThat(statementsOpened.get()).describedAs("statements opened on the %s path", path).isPositive()
        assertNothingLeaked(path)
    }

    /**
     * [assertBalanced] plus the result set the target catalog read must have opened. Separate,
     * because a writer that takes its columns from the source and never reads the catalog opens
     * no result set at all, and a positivity check there would be asserting a resource that does
     * not exist rather than one that leaked.
     */
    fun assertCatalogReadBalanced(path: String) {
        assertBalanced(path)
        assertThat(resultSetsOpened.get()).describedAs("result sets opened on the %s path", path).isPositive()
    }

    fun assertNothingLeaked(path: String) {
        assertThat(connectionsClosed.get()).describedAs("connections closed on the %s path", path)
            .isEqualTo(connectionsOpened.get())
        assertThat(statementsClosed.get()).describedAs("statements closed on the %s path", path)
            .isEqualTo(statementsOpened.get())
        assertThat(resultSetsClosed.get()).describedAs("result sets closed on the %s path", path)
            .isEqualTo(resultSetsOpened.get())
    }

    private fun countStatement(real: Statement): Statement {
        statementsOpened.incrementAndGet()
        val faces: Array<Class<*>> = when (real) {
            is CallableStatement -> arrayOf(CallableStatement::class.java)
            is PreparedStatement -> arrayOf(PreparedStatement::class.java)
            else -> arrayOf(Statement::class.java)
        }
        return proxy(real, faces, statementsClosed) { result ->
            if (result is ResultSet) countResultSet(result) else result
        } as Statement
    }

    /** DatabaseMetaData has no close of its own; it is proxied only to reach its result sets. */
    private fun countMetaData(real: DatabaseMetaData): DatabaseMetaData =
        proxy(real, arrayOf(DatabaseMetaData::class.java), null) { result ->
            if (result is ResultSet) countResultSet(result) else result
        } as DatabaseMetaData

    private fun countResultSet(real: ResultSet): ResultSet {
        resultSetsOpened.incrementAndGet()
        return proxy(real, arrayOf(ResultSet::class.java), resultSetsClosed) { it } as ResultSet
    }

    /** A second close() of the same object counts once, so a defensive double close cannot skew the count. */
    private fun proxy(
        real: Any,
        faces: Array<Class<*>>,
        closes: AtomicInteger?,
        wrap: (Any?) -> Any?,
    ): Any {
        var counted = false
        return Proxy.newProxyInstance(javaClass.classLoader, faces) { _, method: Method, args ->
            val result = try {
                method.invoke(real, *(args ?: emptyArray()))
            } catch (e: InvocationTargetException) {
                throw e.targetException
            }
            if (method.name == "close" && !counted && closes != null) {
                counted = true
                closes.incrementAndGet()
            }
            wrap(result)
        }
    }
}
