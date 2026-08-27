package infra.etl.task

import infra.etl.Etl
import infra.etl.TaskHarness
import infra.etl.duckdb.CreateTable
import infra.etl.pipe.CanonicalType
import infra.etl.pipe.ColumnMeta
import infra.etl.pipe.RowTransform
import java.nio.file.Files
import java.nio.file.Path
import kotlin.streams.toList
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir

/**
 * Two review findings that only an assembled run can show.
 *
 * - **M6**: [infra.etl.duckdb.ScratchDb] empties the directory it is handed and leaves the
 *   directory itself, which is right for a directory its caller owns. The engine resolves a fresh
 *   one per runId, so nothing ever reused it and every run left an empty directory behind - some
 *   52,000 a year at spec 8.1's ten-minute cadence, on the volume spec 7.2 sizes.
 * - **M1**: a transform may add a Row key, and `JdbcTableWriter` binds only the columns it was
 *   opened with. An added column that `transform.addColumns` does not declare was therefore bound
 *   nowhere and silently took the target's database default, against spec 4.4's promise of a
 *   runtime error for a Row key with no matching column.
 */
class ReviewFindingsEngineTest {

    @TempDir
    lateinit var root: Path

    private val harness by lazy { TaskHarness(root) }

    @AfterEach
    fun closeHarness() = harness.close()

    private fun runDirectories(): List<String> =
        if (!Files.isDirectory(harness.scratchRoot)) {
            emptyList()
        } else {
            Files.list(harness.scratchRoot).use { it.toList() }.map { it.fileName.toString() }.sorted()
        }

    /**
     * The run directory is gone, not merely empty. Two runs, because one leftover is a leak only
     * once you know nothing reclaims it later - and because the count is what an operator watching
     * the volume actually sees.
     */
    @Test
    fun aRunLeavesNoScratchDirectoryBehind() {
        val source = harness.datasource("oracle_mes")
        source.exec("create table wip (lot_id BIGINT not null, qty BIGINT not null)", "insert into wip values (1, 7)")
        val task = Etl.task(
            phases = arrayOf(
                Etl.phase("stage", Etl.pipe("load-wip", "oracle_mes", "select lot_id, qty from wip", "wip_stg")),
            ),
        )

        harness.runExpectingSuccess(task)
        harness.runExpectingSuccess(task)

        assertEquals(emptyList<String>(), runDirectories()) {
            "each run resolves its own directory under the scratch root and nothing ever reuses one, " +
                "so a directory left standing is never reclaimed"
        }
    }

    /**
     * The undeclared column is reported, and the declared one still writes. Without the second
     * half this test passes against a writer that refuses every transform.
     */
    @Test
    fun aTransformColumnTheStepDidNotDeclareIsReportedRatherThanDroppedIntoADefault() {
        val source = harness.datasource("oracle_mes")
        source.exec("create table wip (lot_id BIGINT not null)", "insert into wip values (1)")
        val target = harness.datasource("report_oracle")
        target.exec("create table wip_tgt (lot_id BIGINT, row_hash VARCHAR)")
        val addsRowHash = RowTransform { it.with("row_hash", "h1") }

        val outcome = harness.run(
            Etl.task(
                phases = arrayOf(
                    Etl.phase(
                        "publish",
                        PipeStep(
                            name = "load-report",
                            source = PipeSource("oracle_mes", "select lot_id from wip"),
                            target = TableTarget("report_oracle", "wip_tgt", CreateTable.REQUIRED),
                            transform = addsRowHash,
                        ),
                    ),
                ),
            ),
        )

        assertAll(
            { assertEquals(Outcome.FAILED, outcome.outcome) { "an undeclared added column must not pass" } },
            {
                assertTrue(outcome.failure!!.message!!.contains("row_hash")) {
                    "the error must name the column: ${outcome.failure?.message}"
                }
            },
            {
                assertEquals(0L, target.longAt("select count(*) from wip_tgt")) {
                    "and nothing may have been written with row_hash left null"
                }
            },
        )
    }

    @Test
    fun theSameTransformColumnDeclaredInAddColumnsWrites() {
        val source = harness.datasource("oracle_mes")
        source.exec("create table wip (lot_id BIGINT not null)", "insert into wip values (1)")
        val target = harness.datasource("report_oracle")
        target.exec("create table wip_tgt (lot_id BIGINT, row_hash VARCHAR)")
        val addsRowHash = RowTransform { it.with("row_hash", "h1") }

        harness.runExpectingSuccess(
            Etl.task(
                phases = arrayOf(
                    Etl.phase(
                        "publish",
                        PipeStep(
                            name = "load-report",
                            source = PipeSource("oracle_mes", "select lot_id from wip"),
                            target = TableTarget("report_oracle", "wip_tgt", CreateTable.REQUIRED),
                            transform = addsRowHash,
                            addColumns = listOf(ColumnMeta("row_hash", CanonicalType.STRING, true)),
                        ),
                    ),
                ),
            ),
        )

        assertEquals(1L, target.longAt("select count(*) from wip_tgt where row_hash = 'h1'"))
    }
}
