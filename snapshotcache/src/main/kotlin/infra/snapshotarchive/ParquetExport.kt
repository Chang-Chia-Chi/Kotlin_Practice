package infra.snapshotarchive

import java.nio.file.Path
import java.sql.Connection

/**
 * Exports one table of a snapshot to a local Parquet file, returning the row count written.
 *
 * Parquet rather than a copy of the `.db` file (D36): the framework is pinned to DuckDB
 * 1.1.3 for a CI constraint, and a `.db` archive becomes unreadable the day that pin moves,
 * taking the whole retention window with it. Parquet is per-table, version-independent, and
 * natively diffable by the same engine on the way back in.
 *
 * [connection] is expected to be a snapshot connection - its current database is the
 * generation file, attached READ_ONLY. Writing a file *out* of that database is not a write
 * *into* it, so the read-only attach is untouched; `ParquetExportSpikeTest` pins both halves.
 *
 * The identifier and literal quoting below duplicates `infra.snapshotcache.spi`. That is
 * deliberate: plan 3c fences this package off from `spi`, and two one-line functions are a
 * smaller price than the dependency that fence forbids.
 *
 * Note that the fence is currently a convention, not a gate. `ArchitectureTest` imports
 * only `infra.snapshotcache`, so no rule sees this package yet and nothing would fail the
 * build if a later edit reached into `spi`, `core`, or `org.duckdb` from here. Closing that
 * is ticket 02's first deliverable; until it lands, this comment is the only thing holding
 * the line.
 */
fun exportTable(connection: Connection, table: String, target: Path): Long =
    connection.createStatement().use { statement ->
        statement.execute(
            "COPY (SELECT * FROM ${ident(table)}) TO '${literal(duckDbPath(target))}' (FORMAT PARQUET)",
        )
        // Counted rather than taken from COPY's update count, which 1.1.3 does report but
        // cannot be trusted as an inventory value: an empty table and a driver that stopped
        // classifying COPY as DML both return 0, and nothing downstream could tell them
        // apart. A row_count of 0 recorded for a 1M-row table would be committed into the
        // PENDING manifest row and then "verified" against the real object by ticket 04's
        // watchdog. The extra scan is metadata-cheap next to the export it follows.
        statement.executeQuery("SELECT COUNT(*) FROM ${ident(table)}").use { rows ->
            check(rows.next()) { "row count query returned nothing for table $table" }
            rows.getLong(1)
        }
    }

/** DuckDB takes forward slashes on every platform; a Windows separator would otherwise escape. */
private fun duckDbPath(path: Path): String =
    path.toAbsolutePath().toString().replace(java.io.File.separatorChar, '/')

private fun ident(name: String): String = "\"${name.replace("\"", "\"\"")}\""

private fun literal(value: String): String = value.replace("'", "''")
