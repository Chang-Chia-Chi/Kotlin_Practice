package infra.etl.duckdb

import infra.etl.Scratchpad
import infra.etl.duckdb.DatasetNamer
import infra.etl.duckdb.ScratchDb
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
 * P4, done-when items 4 and 5: the attempt-suffix scheme for datasets written inside scratch, and
 * the stable view that resolves to the winning attempt, table or parquet file alike.
 *
 * This is the phase's core. DuckDB 1.1.3 cannot reclaim space - TRUNCATE is unqualified DELETE,
 * VACUUM does not vacuum deletions, VACUUM FULL is unimplemented, DROP TABLE does not shrink the
 * file - so "the first attempt's table still exists unreferenced" is not an implementation
 * detail that happens to be observable. It is the mechanism: nothing may be deleted inside a
 * live scratch database, and the only reclamation point is closing the instance and deleting
 * the whole file, which [ScratchDbDeletionTest] covers.
 *
 * Both halves of item 4 are asserted. A test that only queried the view would pass against an
 * implementation that dropped the old table on retry, which is exactly what the scheme forbids.
 */
class DatasetNamerTest {

    @TempDir
    lateinit var root: Path

    /** The attempt-suffixed physical names, spelled out: `wip_stg__a1`, `wip_stg__a2`. */
    @Test
    fun physicalNamesAreAttemptSuffixed() {
        val namer = DatasetNamer(root)

        assertAll(
            { assertEquals("wip_stg__a1", namer.physical("wip_stg", 1)) },
            { assertEquals("wip_stg__a2", namer.physical("wip_stg", 2)) },
            { assertEquals("summary__a4", namer.physical("summary", 4)) },
        )
    }

    /**
     * The dataset name reaches SQL as an identifier and the filesystem as a file name, and no
     * prepared statement can parameterise either. Startup validation rule 9 guarantees only that
     * names are unique within a task; nothing in the documents constrains their characters, so
     * this check is the whole of the defence.
     *
     * `../../evil` is the one that matters: [DatasetNamer.parquetPath] resolves the name against
     * the scratch directory, so without the check a task file could write a parquet file outside
     * the run's own directory - and could then have it deleted by the run's cleanup sweep.
     */
    @Test
    fun aDatasetNameThatIsNotAnIdentifierIsRejected() {
        val namer = DatasetNamer(root)

        assertThrows<IllegalArgumentException>(
            { "path traversal: this name becomes a file path as well as an identifier" },
        ) { namer.physical("../../evil", 1) }
        listOf("wip stg", "wip\"stg", "1abc", "", "x".repeat(65)).forEach { name ->
            assertThrows<IllegalArgumentException>({ "dataset name '$name'" }) { namer.physical(name, 1) }
        }

        assertAll(
            { assertEquals("wip_stg__a1", namer.physical("wip_stg", 1)) },
            { assertEquals("_x__a1", namer.physical("_x", 1)) },
            {
                val longest = namer.physical("x".repeat(64), 1)
                assertTrue(longest.startsWith("x".repeat(64) + "__a")) { "was: $longest" }
            },
        )
    }

    /** Attempts are the built-in `attempt` variable, which starts at 1. */
    @Test
    fun anAttemptBelowOneIsRejected() {
        assertThrows<IllegalArgumentException> { DatasetNamer(root).physical("wip_stg", 0) }
    }

    /** A parquet dataset is written as `COPY (<sql>) TO '<scratchDir>/<output>__a<n>.parquet'`. */
    @Test
    fun parquetPathIsTheAttemptSuffixedFileInTheScratchDirectory() {
        val namer = DatasetNamer(root)

        val path = namer.parquetPath("summary", 2)

        assertAll(
            { assertEquals("summary__a2.parquet", path.fileName.toString()) },
            { assertEquals(root.toAbsolutePath(), path.toAbsolutePath().parent) },
        )
    }

    /**
     * Done-when 4, in full. Attempt 1 writes `wip_stg__a1` and then fails, so no view is
     * published; attempt 2 writes `wip_stg__a2` and publishes. Afterwards:
     *
     *  - `wip_stg` yields attempt 2's rows, identified by the marker column rather than by a row
     *    count the two attempts could share;
     *  - `wip_stg__a1` is still in the catalog with attempt 1's rows intact;
     *  - nothing references it - checked against every view definition in the database, not just
     *    against the one this test published, so a stray alias would be caught too.
     */
    @Test
    fun afterAFailedAttemptAndARetry_theViewResolvesToAttemptTwo_andAttemptOneSurvivesUnreferenced() {
        ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(root)).use { scratch ->
            val namer = DatasetNamer(root)
            val connection = scratch.connection()

            // Attempt 1: the dataset lands, the step then fails, so the view is never published.
            Scratchpad.createAttemptTable(connection, namer.physical("wip_stg", 1), "a1", rows = 3)

            // Attempt 2: a fresh physical name, nothing dropped, deleted or truncated.
            Scratchpad.createAttemptTable(connection, namer.physical("wip_stg", 2), "a2", rows = 5)
            namer.publishTable(connection, "wip_stg", 2)

            assertAll(
                {
                    assertEquals(
                        listOf(
                            listOf("a2-0"), listOf("a2-1"), listOf("a2-2"), listOf("a2-3"), listOf("a2-4"),
                        ),
                        Scratchpad.grid(connection, "select lot_code from wip_stg order by lot_id").rows,
                    ) { "later steps read the stable name and must see attempt 2" }
                },
                {
                    val tables = Scratchpad.tableNames(connection)
                    assertTrue(tables.containsAll(listOf("wip_stg__a1", "wip_stg__a2"))) {
                        "spec 5.5: a failed attempt is left in place, nothing is deleted; tables were $tables"
                    }
                },
                {
                    assertEquals(
                        listOf(listOf("a1-0"), listOf("a1-1"), listOf("a1-2")),
                        Scratchpad.grid(connection, "select lot_code from wip_stg__a1 order by lot_id").rows,
                    ) { "attempt 1's rows must be intact, not merely a table of the same name" }
                },
                {
                    val definition = Scratchpad.viewDefinition(connection, "wip_stg")
                    assertTrue(definition.contains("wip_stg__a2")) {
                        "the stable view must resolve to attempt 2, was: $definition"
                    }
                },
                {
                    val definitions = Scratchpad.viewDefinitions(connection)
                    assertTrue(definitions.none { it.contains("wip_stg__a1") }) {
                        "attempt 1 must be unreferenced by every view, not just by wip_stg; " +
                            "view definitions were $definitions"
                    }
                },
            )
        }
    }

    /**
     * Publishing twice must repoint the stable name rather than fail or accumulate. This is what
     * makes `retries: 3` cost four copies of a dataset and one view, which is the arithmetic
     * the scratch database's `sizeLimit` is derived from.
     */
    @Test
    fun republishingTheStableNameRepointsItAndLeavesOneView() {
        ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(root)).use { scratch ->
            val namer = DatasetNamer(root)
            val connection = scratch.connection()

            (1..3).forEach { attempt ->
                Scratchpad.createAttemptTable(
                    connection,
                    namer.physical("wip_stg", attempt),
                    "a$attempt",
                    rows = attempt,
                )
                namer.publishTable(connection, "wip_stg", attempt)
            }

            assertAll(
                { assertEquals(3L, Scratchpad.rowCount(connection, "wip_stg")) },
                {
                    val definitions = Scratchpad.viewDefinitions(connection)
                    assertEquals(1, definitions.size) { "was $definitions" }
                },
                {
                    val tables = Scratchpad.tableNames(connection)
                    assertTrue(tables.containsAll(listOf("wip_stg__a1", "wip_stg__a2", "wip_stg__a3"))) {
                        "every attempt's table must survive, was $tables"
                    }
                },
            )
        }
    }

    /**
     * Done-when 5, in its strong form. The same downstream SQL string - a filter, an aggregate
     * and an ordering, not a bare `select *` - runs against a stable view backed by a table and
     * against a stable view backed by a parquet file, in two separate scratch databases, under
     * the same dataset name. The two results are compared as a whole: column names, driver type
     * names and every value.
     *
     * Two databases rather than two dataset names in one, because the claim under test is that
     * `format` can change without touching any other step - the downstream SQL cannot even see
     * which form it got, and giving the two forms different names would hide that.
     */
    @Test
    fun theStableViewResolvesIdenticallyWhetherTheDatasetIsATableOrAParquetFile() {
        val downstream =
            "select lot_id, upper(lot_code) as code, sum(qty) as qty, min(note) as note, " +
                "max(upd_ts) as upd_ts from summary where lot_id < 4 group by 1, 2 order by 1"

        val asTable = Files.createDirectories(root.resolve("as-table"))
        val asParquet = Files.createDirectories(root.resolve("as-parquet"))

        val fromTable = ScratchDb(asTable, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(asTable)).use { scratch ->
            val namer = DatasetNamer(asTable)
            val connection = scratch.connection()
            Scratchpad.createAttemptTable(connection, namer.physical("summary", 1), "s", rows = 6)
            namer.publishTable(connection, "summary", 1)
            Scratchpad.grid(connection, downstream)
        }

        val fromParquet = ScratchDb(asParquet, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(asParquet)).use { scratch ->
            val namer = DatasetNamer(asParquet)
            val connection = scratch.connection()
            val file = namer.parquetPath("summary", 1)
            Scratchpad.exec(
                connection,
                "COPY (${Scratchpad.attemptSelect("s", 6)}) TO '${Scratchpad.sqlPath(file)}' (FORMAT PARQUET)",
            )
            assertTrue(Files.exists(file)) { "spec 5.6's COPY must land at the namer's path: $file" }
            namer.publishParquet(connection, "summary", 1)

            assertAll(
                {
                    val definition = Scratchpad.viewDefinition(connection, "summary")
                    assertAll(
                        {
                            assertTrue(definition.contains("read_parquet")) {
                                "the parquet form of the stable view goes through read_parquet, was: $definition"
                            }
                        },
                        {
                            assertTrue(definition.contains("summary__a1.parquet")) {
                                "the stable view must name the attempt's parquet file, was: $definition"
                            }
                        },
                    )
                },
                {
                    val tables = Scratchpad.tableNames(connection)
                    assertFalse("summary__a1" in tables) {
                        "spec 5.6: a parquet dataset never becomes a table in the scratch file, tables were $tables"
                    }
                },
            )

            Scratchpad.grid(connection, downstream)
        }

        assertAll(
            {
                assertTrue(fromParquet.rows.isNotEmpty()) {
                    "the fixture produced no rows to compare"
                }
            },
            {
                assertTrue(fromParquet.rows.flatten().any { it == null }) {
                    "no null survived into the compared grid, so the nullable column proves nothing; " +
                        "values were ${fromParquet.rows.flatten()}"
                }
            },
            {
                val types = fromParquet.header.map { it.second }
                assertTrue("TIMESTAMP" in types) { "no TIMESTAMP survived into the compared grid, was $types" }
            },
            {
                assertEquals(fromTable, fromParquet) {
                    "identical downstream SQL, identical answer, regardless of physical format"
                }
            },
        )
    }
}
