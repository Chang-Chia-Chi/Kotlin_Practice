package infra.etl.duckdb

import java.nio.file.Path
import java.sql.Connection

/** Datasets are quoted into DDL and used as file names, so a name must be an identifier and nothing else. */
private val DATASET_NAME = Regex("[A-Za-z_][A-Za-z0-9_]{0,63}")

/**
 * The attempt-suffix scheme: every dataset a run produces inside scratch is written
 * under a physical name carrying its attempt number, and the name later steps use is a view over
 * whichever attempt succeeded.
 *
 * ```
 * attempt 1 fails    ->  table wip_stg__a1                       left in place, unreferenced
 * attempt 2 succeeds ->  table wip_stg__a2
 *                        create or replace view wip_stg as select * from wip_stg__a2
 * ```
 *
 * Nothing is ever dropped, deleted or truncated to make room, because on DuckDB 1.1.3 none of
 * those reclaim any space. The reclamation point is [ScratchDb.close] emptying the
 * directory, once per run. The cost is that a repeatedly failing dataset occupies up to
 * `1 + retries` copies - and how much of an attempt survives depends on where its failure landed,
 * between nothing and one chunk short of everything written.
 *
 * **The stable name resolves identically over a table and over a parquet file.** A `materialize`
 * step may switch `format` without any other step changing, so [publishParquet] wraps
 * `read_parquet` in the same `create or replace view`. Verified on duckdb_jdbc 1.1.3
 * (P4 scratchpad probe): a view over `read_parquet` and a view over the equivalent table report
 * the same column names and the same driver types - `BIGINT`, `VARCHAR`, `DECIMAL(18,3)` - so
 * neither downstream SQL nor the type mapping that reads them can tell them apart.
 *
 * Which attempt to publish, and when, is the step's decision and not this class's: publishing is
 * what makes an attempt the live one, so it happens only after the attempt has succeeded.
 *
 * @param directory the run's scratch directory - the same path the run's [ScratchDb] was given. Parquet
 *   materialisations land there so that they are reclaimed with the database file.
 */
class DatasetNamer(private val directory: Path) {

    /** The physical table one attempt writes into: `wip_stg__a1`. */
    fun physical(dataset: String, attempt: Int): String = "${datasetIdentifier(dataset)}__a${validAttempt(attempt)}"

    /**
     * The physical parquet file one attempt writes into, absolute so that it does not depend on the
     * process working directory. A retry overwrites its own attempt's file rather than adding a
     * table to the database, which is what parquet buys.
     */
    fun parquetPath(dataset: String, attempt: Int): Path =
        directory.toAbsolutePath().resolve("${physical(dataset, attempt)}.parquet")

    /** Points the stable name at [physical]. Later steps reference `dataset`, never the suffixed name. */
    fun publishTable(connection: Connection, dataset: String, attempt: Int) =
        publish(connection, dataset, quoteIdentifier(physical(dataset, attempt)))

    /** Points the stable name at [parquetPath]. Indistinguishable downstream from [publishTable]. */
    fun publishParquet(connection: Connection, dataset: String, attempt: Int) =
        publish(connection, dataset, "read_parquet('${sqlLiteral(parquetPath(dataset, attempt))}')")

    private fun publish(connection: Connection, dataset: String, relation: String) {
        val view = quoteIdentifier(datasetIdentifier(dataset))
        connection.createStatement().use { it.execute("create or replace view $view as select * from $relation") }
    }

    /** Attempts follow the built-in `attempt` task variable, so the first attempt is 1. */
    private fun validAttempt(attempt: Int): Int {
        require(attempt >= 1) { "attempt must be 1 or greater, was $attempt" }
        return attempt
    }
}

/**
 * A dataset name as written in YAML, checked before it is quoted into DDL or turned into a file
 * name. Startup validation guarantees uniqueness within a task (rule 9) but says nothing about the
 * characters, and this is where the name reaches SQL that no prepared statement can parameterise.
 */
internal fun datasetIdentifier(dataset: String): String {
    require(DATASET_NAME.matches(dataset)) {
        "dataset name '$dataset' is not an identifier; expected ${DATASET_NAME.pattern}."
    }
    return dataset
}

/** A DuckDB quoted identifier, so that a dataset or column called `order` is still a legal name. */
internal fun quoteIdentifier(identifier: String): String = "\"${identifier.replace("\"", "\"\"")}\""

/**
 * An absolute path as a DuckDB single-quoted literal. Windows separators need no rewriting -
 * measured on the P4 probe, `SET temp_directory` and `read_parquet` both accept a backslash path
 * verbatim, and DuckDB echoes `temp_directory` back exactly as it was given.
 */
internal fun sqlLiteral(path: Path): String = path.toAbsolutePath().toString().replace("'", "''")
