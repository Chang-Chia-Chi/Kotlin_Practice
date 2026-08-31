package infra.etl.duckdb

import infra.etl.Scratchpad
import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir

/**
 * E12: the write-then-publish protocol for scratch datasets, tested where it now lives.
 *
 * `DatasetNamerTest` already proves the naming scheme and that a published view resolves to the
 * attempt that succeeded. What it could not prove is the *protocol* - that a failed attempt
 * publishes nothing - because the sequence lived in four hand-written call sites in `TaskEngine`,
 * so reaching it meant driving a whole engine run and making it fail on schedule. Here it is a
 * block that throws.
 *
 * These tests do not repeat the naming assertions. `DatasetNamer` survives E12 with the interface
 * its own tests pin, and this module wraps it rather than replacing it.
 */
class ScratchDatasetsTest {

    @TempDir
    lateinit var root: Path

    private fun <T> withDatasets(block: (ScratchDatasets, java.sql.Connection) -> T): T =
        ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(root)).use { scratch ->
            block(ScratchDatasets(scratch, DatasetNamer(root)), scratch.connection())
        }

    /**
     * **The failed-attempt case, which is the whole reason this is a module.**
     *
     * The block writes attempt 1's table and then throws, exactly as a step that dies after
     * flushing some rows does. What must not happen is a view: publishing is what makes an attempt
     * the live one, and an attempt that failed is not it.
     *
     * The written table is asserted to survive, because "did not publish" must not be achieved by
     * cleaning up. DuckDB 1.1.3 reclaims nothing, so a failed attempt is left in place and the run
     * directory is deleted whole.
     */
    @Test
    fun aFailedAttemptWritesItsTableAndPublishesNothing() {
        withDatasets { datasets, connection ->
            val failure = assertThrows<IllegalStateException> {
                datasets.attemptTable("wip_stg", 1) { physical ->
                    Scratchpad.createAttemptTable(connection, physical, "a1", rows = 3)
                    error("probe: the step failed after flushing a chunk")
                }
            }

            assertAll(
                { assertEquals("probe: the step failed after flushing a chunk", failure.message) },
                {
                    assertFalse(Scratchpad.tableNames(connection).contains("wip_stg")) {
                        "a failed attempt must publish no stable name; tables were " +
                            "${Scratchpad.tableNames(connection)}"
                    }
                },
                {
                    assertTrue(Scratchpad.viewDefinitions(connection).isEmpty()) {
                        "no view at all may exist after a failed attempt; definitions were " +
                            "${Scratchpad.viewDefinitions(connection)}"
                    }
                },
                {
                    assertEquals(3L, Scratchpad.rowCount(connection, "wip_stg__a1")) {
                        "the failed attempt's rows stay where they are - not publishing is not cleaning up"
                    }
                },
            )
        }
    }

    /**
     * The same guarantee for the parquet half, and the same pair of assertions: no view, and the
     * file this attempt wrote still on disk.
     *
     * The second half is not decoration. "Did not publish" is satisfied trivially by an
     * implementation that deleted the file, and the protocol forbids that - a failed attempt is
     * left in place and the run directory is reclaimed whole. Without it this test would pass
     * against a `finally` that cleaned up, which is the behaviour the protocol rules out.
     */
    @Test
    fun aFailedParquetAttemptPublishesNothingAndLeavesItsFile() {
        withDatasets { datasets, connection ->
            assertThrows<IllegalStateException> {
                datasets.attemptParquet("summary", 1) { path ->
                    Scratchpad.exec(
                        connection,
                        "copy (${Scratchpad.attemptSelect("a1", 3)}) to '${sqlLiteral(path)}' (format parquet)",
                    )
                    error("probe: the step failed after the file was written")
                }
            }

            assertAll(
                {
                    assertTrue(Scratchpad.viewDefinitions(connection).isEmpty()) {
                        "definitions were ${Scratchpad.viewDefinitions(connection)}"
                    }
                },
                {
                    assertTrue(Files.exists(root.resolve("summary__a1.parquet"))) {
                        "the failed attempt's file stays where it is - not publishing is not cleaning up"
                    }
                },
            )
        }
    }

    /**
     * The successful case, both halves, asserted through the stable name only - which is all a
     * later step ever references.
     *
     * The parquet half is what makes the two entry points worth having rather than one with a
     * format flag: `MaterializeFormat` lives in `infra.etl.task`, and an adapter in
     * `infra.etl.duckdb` never names the layer above it.
     */
    @Test
    fun aSucceededAttemptPublishesTheStableNameOverEitherFormat() {
        withDatasets { datasets, connection ->
            datasets.attemptTable("wip_stg", 2) { physical ->
                Scratchpad.createAttemptTable(connection, physical, "a2", rows = 5)
            }
            datasets.attemptParquet("summary", 2) { path ->
                Scratchpad.exec(
                    connection,
                    "copy (${Scratchpad.attemptSelect("p2", 4)}) to '${sqlLiteral(path)}' (format parquet)",
                )
            }

            assertAll(
                { assertEquals(5L, Scratchpad.rowCount(connection, "wip_stg")) },
                { assertEquals(4L, Scratchpad.rowCount(connection, "summary")) },
                {
                    assertTrue(Scratchpad.viewDefinition(connection, "wip_stg").contains("wip_stg__a2")) {
                        "the view must resolve to the attempt that ran"
                    }
                },
                {
                    assertTrue(Scratchpad.viewDefinition(connection, "summary").contains("read_parquet")) {
                        "a parquet dataset is published as a view over the file (spec 5.6)"
                    }
                },
                {
                    assertTrue(Files.exists(root.resolve("summary__a2.parquet"))) {
                        "the file the block was handed is the one the view reads"
                    }
                },
            )
        }
    }

    /**
     * The block's value comes back out, which is not decoration: `pipe` carries out the rows it
     * moved and `cacheCopy` the generation it read, and both are reported after publishing.
     */
    @Test
    fun theBlocksValueIsReturned() {
        withDatasets { datasets, connection ->
            val answer = datasets.attemptTable("wip_stg", 1) { physical ->
                Scratchpad.createAttemptTable(connection, physical, "a1", rows = 2)
                "42 rows"
            }

            assertEquals("42 rows", answer)
        }
    }
}
