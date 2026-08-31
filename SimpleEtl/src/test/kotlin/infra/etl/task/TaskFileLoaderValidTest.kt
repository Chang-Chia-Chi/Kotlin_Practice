package infra.etl.task

import infra.etl.TaskFiles
import infra.etl.TaskFiles.VALID
import infra.etl.TaskFiles.VALID_REQUIRED
import infra.etl.TaskFiles.loadOne
import infra.etl.TaskFiles.minimal
import infra.etl.TaskFiles.orderedVars
import infra.etl.duckdb.CreateTable
import infra.etl.pipe.CanonicalType
import infra.etl.task.ExportStep
import infra.etl.task.MaterializeFormat
import infra.etl.task.MaterializeStep
import infra.etl.task.PipeStep
import infra.etl.task.SCRATCH
import infra.etl.task.SqlStep
import infra.etl.task.StatementTarget
import infra.etl.task.Step
import infra.etl.task.TableTarget
import infra.etl.task.TaskDefinition
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir

/**
 * P6's canaries: the other half of "every rule has a test with a deliberately broken file".
 *
 * Eighteen tests asserting that eighteen broken files are rejected would all pass against a
 * loader that rejects everything it is handed - including the empty file, including a correct
 * one. These tests are what make that implementation fail, and they are the reason every broken
 * file in `TaskFileLoaderRulesTest` is built by editing one of the baselines asserted here
 * rather than written from scratch.
 *
 * [theBaselineLoadsAndEveryFieldSurvives] does more than assert a clean load. A loader that
 * returned an empty list, or a `TaskDefinition` at every default, would satisfy "it loaded"
 * while carrying none of the file's content - so every field the task schema can express is
 * read back. That also covers six fields P5 recorded as "carried and unused": `enabled`,
 * `cron`, `logging`, `onSuccess`, `onFailure` and `PipeTarget.idempotent` have no other test in
 * the module, and loading is where they are populated.
 */
class TaskFileLoaderValidTest {

    @TempDir
    lateinit var root: Path

    private fun TaskDefinition.step(name: String): Step =
        phases.flatMap { it.steps }.single { it.name == name }

    @Test
    fun theBaselineLoadsAndEveryFieldSurvives() {
        val task = loadOne(root, VALID).single()

        assertAll(
            { assertEquals("wip-summary", task.name) },
            { assertTrue(task.enabled) { "the baseline states no 'enabled', so it must default to true" } },
            { assertEquals("0 */10 * * * ?", task.cron) },
            { assertTrue(task.logging) { "the baseline states no 'logging', so it must default to true" } },
            { assertEquals(5000, task.chunkSize) },
            { assertEquals(4096, task.scratchMemoryLimitMb) },
            { assertEquals("notify-downstream", task.onSuccess) },
            { assertEquals("page-oncall", task.onFailure) },

            { assertEquals(1, task.vars.size) { "was ${task.vars}" } },
            { assertEquals("siteCode", task.vars.single().name) },
            { assertEquals("F12", task.vars.single().value) },

            {
                assertEquals(listOf("extract", "build", "publish"), task.phases.map { it.name }) {
                    "phases run in file order (spec 5.1)"
                }
            },
            {
                assertEquals(
                    listOf(
                        "read-watermark",
                        "load-wip",
                        "build-summary",
                        "index-staging",
                        "publish-summary",
                    ),
                    task.phases.flatMap { it.steps }.map { it.name },
                ) { "steps run in file order within a phase (spec 5.1)" }
            },
        )
    }

    @Test
    fun theExportStepSurvives() {
        val step = loadOne(root, VALID).single().step("read-watermark") as ExportStep

        assertAll(
            { assertEquals("oracle_mes", step.datasource) },
            { assertEquals(listOf("lastTs"), step.vars.map { it.name }) },
            {
                assertTrue(":taskName" in step.vars.single().sql) {
                    "the export query was: ${step.vars.single().sql}"
                }
            },
            {
                assertEquals(0, step.retries) {
                    "an export step has no target, so spec 5.3's scratch default does not apply"
                }
            },
        )
    }

    /**
     * The transform is asserted by identity, not by non-nullness. Rule 4 says the bean name
     * must resolve to a `RowTransform`; a loader that resolved every name to a fresh no-op
     * would satisfy `isNotNull` and silently discard the author's transform at run time.
     */
    @Test
    fun thePipeStepSurvivesIncludingItsTransformAndAddedColumns() {
        val step = loadOne(root, VALID).single().step("load-wip") as PipeStep

        assertAll(
            {
                assertEquals(20000, step.chunkSize) {
                    "the step value wins over the task value (spec 5.2)"
                }
            },
            { assertEquals(3, step.retries) },
            { assertEquals("oracle_mes", step.source.datasource) },
            {
                assertTrue(listOf(":lastTs", ":siteCode").all { it in step.source.sql }) {
                    "the source query was: ${step.source.sql}"
                }
            },
            { assertSame(TaskFiles.WIP_ENRICHER, step.transform) },
            { assertEquals(listOf("row_hash"), step.addColumns.map { it.name }) },
            {
                assertEquals(CanonicalType.STRING, step.addColumns.single().type) {
                    "spec 3.2 writes the DuckDB type name; VARCHAR is CanonicalType.STRING"
                }
            },
        )

        val target = step.target as TableTarget
        assertAll(
            { assertEquals(SCRATCH, target.datasource) },
            { assertEquals("wip_stg", target.table) },
            { assertEquals(CreateTable.AUTO, target.createTable) },
        )
    }

    @Test
    fun theMaterializeAndSqlStepsSurvive() {
        val task = loadOne(root, VALID).single()

        val materialize = task.step("build-summary") as MaterializeStep
        assertAll(
            { assertEquals(SCRATCH, materialize.datasource) },
            { assertEquals("summary", materialize.output) },
            { assertEquals(MaterializeFormat.TABLE, materialize.format) },
            { assertEquals(3, materialize.retries) },
        )

        val sql = task.step("index-staging") as SqlStep
        assertAll(
            { assertEquals(SCRATCH, sql.datasource) },
            { assertEquals(listOf("create index idx_wip_lot on wip_stg (lot_id)"), sql.statements) },
        )
    }

    @Test
    fun theStatementTargetSurvivesWithItsIdempotentAssertion() {
        val step = loadOne(root, VALID).single().step("publish-summary") as PipeStep

        assertEquals(2, step.retries)
        val target = step.target as StatementTarget
        assertAll(
            { assertEquals("report_oracle", target.datasource) },
            {
                assertTrue(listOf(":lot_id", ":qty").all { it in target.sql }) {
                    "the target statement was: ${target.sql}"
                }
            },
            {
                assertTrue(target.idempotent) {
                    "rule 12's assertion by the author, which the framework cannot verify"
                }
            },
        )
    }

    @Test
    fun theRequiredTargetBaselineLoads() {
        val task = loadOne(root, VALID_REQUIRED).single()

        val step = task.phases.flatMap { it.steps }.single { it.name == "load-required" } as PipeStep
        assertAll(
            { assertEquals(CreateTable.REQUIRED, (step.target as TableTarget).createTable) },
            {
                assertEquals(0, step.retries) {
                    "rule 18 permits a REQUIRED scratch target only at retries 0"
                }
            },
        )
    }

    /**
     * The minimal file is what fills the ten-file directory, and it is also the only place an
     * *omitted* `retries` is observed. The default is 3 for a scratch target, and
     * that default is the hazard rule 18 exists to close.
     */
    @Test
    fun theMinimalFileLoadsAndOmittedFieldsTakeTheirDeclaredDefaults() {
        val task = loadOne(root, minimal("task-1")).single()

        assertAll(
            { assertEquals("task-1", task.name) },
            { assertTrue(task.enabled) { "an omitted 'enabled' must default to true" } },
            { assertNull(task.cron) { "an omitted cron was read as ${task.cron}" } },
            { assertEquals(5000, task.chunkSize) },
            {
                assertNull(task.scratchMemoryLimitMb) {
                    "an omitted scratchMemoryLimitMb was read as ${task.scratchMemoryLimitMb}"
                }
            },
            { assertNull(task.onSuccess) { "an omitted onSuccess was read as ${task.onSuccess}" } },
            { assertTrue(task.vars.isEmpty()) { "an omitted vars list was read as ${task.vars}" } },
        )

        val step = task.phases.single().steps.single() as SqlStep
        assertEquals(3, step.retries) {
            "spec 5.3 defaults retries to 3 for a step on the scratch datasource"
        }
    }

    @Test
    fun aVariableExportedBeforeItsUseLoads() {
        val task = loadOne(root, orderedVars(exportFirst = true)).single()

        assertEquals(listOf("read-watermark", "use-watermark"), task.phases.single().steps.map { it.name })
    }

    /**
     * The schema lists `description` as an optional task field and `TaskDefinition` has nowhere to
     * put it, so rule 1's "unknown fields rejected" and the schema disagree on this one key.
     *
     * Isolated deliberately. The baseline above does not carry a `description`, so whichever way
     * this goes it costs one test rather than every test in the phase.
     */
    @Test
    fun aDescriptionIsAcceptedEvenThoughTheModelHasNoFieldForIt() {
        val yaml = TaskFiles.edit(VALID, "name: wip-summary", "name: wip-summary\ndescription: \"a task\"")

        val task = loadOne(root, yaml).single()

        assertEquals("wip-summary", task.name)
    }
}
