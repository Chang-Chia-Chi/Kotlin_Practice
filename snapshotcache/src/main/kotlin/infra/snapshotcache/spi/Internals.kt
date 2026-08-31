package infra.snapshotcache.spi

import java.sql.ResultSet
import java.sql.Statement

/*
 * Shared internal helpers at the spi boundary. They live here rather than in each file
 * because `core` may depend on `spi` but not the reverse and not on `java.sql` at all,
 * so this is the innermost package both the gate and the DuckDB adapter can reach.
 */

/**
 * Quotes a SQL identifier. Table and column names reach the SQL builders verbatim from
 * `information_schema` and from caller config, so a reserved word (`order`) or a
 * mixed-case name would otherwise be a parse error that the gate reports as bad data.
 */
internal fun ident(name: String): String = "\"${name.replace("\"", "\"\"")}\""

/** Escapes a value being interpolated into a single-quoted SQL literal. */
internal fun literal(value: String): String = value.replace("'", "''")

internal fun <T> Statement.query(sql: String, read: (ResultSet) -> T): T =
    executeQuery(sql).use(read)

internal fun Statement.queryLong(sql: String): Long = query(sql) { rs ->
    check(rs.next()) { "query returned no rows: $sql" }
    rs.getLong(1)
}

internal fun Statement.queryString(sql: String): String = query(sql) { rs ->
    check(rs.next()) { "query returned no rows: $sql" }
    rs.getString(1)
}

internal fun Statement.queryStrings(sql: String): List<String> = query(sql) { rs ->
    val values = mutableListOf<String>()
    while (rs.next()) values += rs.getString(1)
    values
}

/** Failure detail that is never blank, so a report never says just "it failed". */
internal fun Throwable.describe(): String = message ?: toString()
