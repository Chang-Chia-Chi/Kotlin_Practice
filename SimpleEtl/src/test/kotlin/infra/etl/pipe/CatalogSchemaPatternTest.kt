package infra.etl.pipe

import infra.etl.Scratch
import infra.etl.Scratch.STEP
import infra.etl.duckdb.CreateTable
import infra.etl.duckdb.DuckDbTableWriter
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * Review finding M2: `DatabaseMetaData.getColumns` takes the schema as a **pattern**, so `_` is a
 * single-character wildcard there exactly as it is in the table name. `TABLE_NAME` was compared
 * exactly and `TABLE_SCHEM` was not, so `etl_stg.wip` also matched `etl1stg.wip`.
 *
 * Two schemas whose names differ only at that underscore, each holding a `wip` of its own with a
 * different column list. Before the fix the two owners collided and the lookup failed with "2
 * tables named 'etl_stg.wip' are visible ... Qualify the target with its schema" - about a target
 * that already was qualified.
 */
class CatalogSchemaPatternTest {

    private val connection = Scratch.open()

    @AfterEach
    fun closeConnection() = connection.close()

    @Test
    fun aQualifiedTargetTakesItsColumnsFromItsOwnSchemaAndNotFromAWildcardMatch() {
        Scratch.exec(
            connection,
            "create schema etl_stg",
            "create schema etl1stg",
            "create table etl_stg.wip (lot_id BIGINT not null)",
            // Deliberately wider and differently named: if the wrong schema is read, the positional
            // append misaligns rather than merely writing the same thing twice.
            "create table etl1stg.wip (other_a VARCHAR, other_b VARCHAR, other_c VARCHAR)",
        )
        val source = Scratch.read(connection, "select cast(7 as BIGINT) as lot_id")

        DuckDbTableWriter(connection, "etl_stg.wip", CreateTable.REQUIRED, STEP).use {
            it.open(source.columns)
            it.write(source.rows)
        }

        assertEquals(1L, Scratch.rowCount(connection, "etl_stg.wip"))
        assertEquals(0L, Scratch.rowCount(connection, "etl1stg.wip"))
    }
}
