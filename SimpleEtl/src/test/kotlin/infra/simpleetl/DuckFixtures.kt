package infra.simpleetl

import java.sql.DriverManager
import java.sql.ResultSet

/**
 * P1 test support. Turns a DuckDB SELECT into real result-set metadata and Rows through the
 * public RowMapper, so the read seam is exercised against a real driver and never a stand-in.
 *
 * Fixtures are pure SELECTs: no table, no INSERT, no appender. Nothing is written, which keeps
 * this file inside P1's "not in scope: writing anything".
 */
object Duck {

    class Read(val columns: List<ColumnMeta>, val rows: List<Row>) {
        val row: Row get() = rows.first()
        fun column(name: String): ColumnMeta = columns.first { it.name == name }
    }

    fun read(sql: String, step: String = "duck-step"): Read =
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(sql).use { rs ->
                    val mapper = RowMapper(rs.metaData, step)
                    val rows = ArrayList<Row>()
                    while (rs.next()) rows.add(mapper.map(rs))
                    Read(mapper.columns, rows)
                }
            }
        }

    fun row(sql: String, step: String = "duck-step"): Row = read(sql, step).row

    /** Raw access, for the cases that assert on driver metadata rather than on a Row. */
    fun <T> withResultSet(sql: String, block: (ResultSet) -> T): T =
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(sql).use(block)
            }
        }
}
