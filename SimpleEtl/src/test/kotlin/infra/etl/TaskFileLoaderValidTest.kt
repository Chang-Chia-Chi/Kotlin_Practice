package infra.etl

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
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
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
 * while carrying none of the file's content - so every field spec 3.1 to 3.5 can express is
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

        assertThat(task.name).isEqualTo("wip-summary")
        assertThat(task.enabled).isTrue()
        assertThat(task.cron).isEqualTo("0 */10 * * * ?")
        assertThat(task.logging).isTrue()
        assertThat(task.chunkSize).isEqualTo(5000)
        assertThat(task.scratchMemoryLimitMb).isEqualTo(4096)
        assertThat(task.onSuccess).isEqualTo("notify-downstream")
        assertThat(task.onFailure).isEqualTo("page-oncall")

        assertThat(task.vars).hasSize(1)
        assertThat(task.vars.single().name).isEqualTo("siteCode")
        assertThat(task.vars.single().value).isEqualTo("F12")

        assertThat(task.phases.map { it.name })
            .describedAs("phases run in file order (spec 5.1)")
            .containsExactly("extract", "build", "publish")
        assertThat(task.phases.flatMap { it.steps }.map { it.name })
            .describedAs("steps run in file order within a phase (spec 5.1)")
            .containsExactly(
                "read-watermark",
                "load-wip",
                "build-summary",
                "index-staging",
                "publish-summary",
            )
    }

    @Test
    fun theExportStepSurvives() {
        val step = loadOne(root, VALID).single().step("read-watermark") as ExportStep

        assertThat(step.datasource).isEqualTo("oracle_mes")
        assertThat(step.vars.map { it.name }).containsExactly("lastTs")
        assertThat(step.vars.single().sql).contains(":taskName")
        assertThat(step.retries)
            .describedAs("an export step has no target, so spec 5.3's scratch default does not apply")
            .isEqualTo(0)
    }

    /**
     * The transform is asserted by identity, not by non-nullness. Rule 4 says the bean name
     * must resolve to a `RowTransform`; a loader that resolved every name to a fresh no-op
     * would satisfy `isNotNull` and silently discard the author's transform at run time.
     */
    @Test
    fun thePipeStepSurvivesIncludingItsTransformAndAddedColumns() {
        val step = loadOne(root, VALID).single().step("load-wip") as PipeStep

        assertThat(step.chunkSize)
            .describedAs("the step value wins over the task value (spec 5.2)")
            .isEqualTo(20000)
        assertThat(step.retries).isEqualTo(3)
        assertThat(step.source.datasource).isEqualTo("oracle_mes")
        assertThat(step.source.sql).contains(":lastTs", ":siteCode")
        assertThat(step.transform).isSameAs(TaskFiles.WIP_ENRICHER)
        assertThat(step.addColumns.map { it.name }).containsExactly("row_hash")
        assertThat(step.addColumns.single().type)
            .describedAs("spec 3.2 writes the DuckDB type name; VARCHAR is CanonicalType.STRING")
            .isEqualTo(CanonicalType.STRING)

        val target = step.target as TableTarget
        assertThat(target.datasource).isEqualTo(SCRATCH)
        assertThat(target.table).isEqualTo("wip_stg")
        assertThat(target.createTable).isEqualTo(CreateTable.AUTO)
    }

    @Test
    fun theMaterializeAndSqlStepsSurvive() {
        val task = loadOne(root, VALID).single()

        val materialize = task.step("build-summary") as MaterializeStep
        assertThat(materialize.datasource).isEqualTo(SCRATCH)
        assertThat(materialize.output).isEqualTo("summary")
        assertThat(materialize.format).isEqualTo(MaterializeFormat.TABLE)
        assertThat(materialize.retries).isEqualTo(3)

        val sql = task.step("index-staging") as SqlStep
        assertThat(sql.datasource).isEqualTo(SCRATCH)
        assertThat(sql.statements).containsExactly("create index idx_wip_lot on wip_stg (lot_id)")
    }

    @Test
    fun theStatementTargetSurvivesWithItsIdempotentAssertion() {
        val step = loadOne(root, VALID).single().step("publish-summary") as PipeStep

        assertThat(step.retries).isEqualTo(2)
        val target = step.target as StatementTarget
        assertThat(target.datasource).isEqualTo("report_oracle")
        assertThat(target.sql).contains(":lot_id", ":qty")
        assertThat(target.idempotent)
            .describedAs("rule 12's assertion by the author, which the framework cannot verify")
            .isTrue()
    }

    @Test
    fun theRequiredTargetBaselineLoads() {
        val task = loadOne(root, VALID_REQUIRED).single()

        val step = task.phases.flatMap { it.steps }.single { it.name == "load-required" } as PipeStep
        assertThat((step.target as TableTarget).createTable).isEqualTo(CreateTable.REQUIRED)
        assertThat(step.retries)
            .describedAs("rule 18 permits a REQUIRED scratch target only at retries 0")
            .isEqualTo(0)
    }

    /**
     * The minimal file is what fills the ten-file directory, and it is also the only place an
     * *omitted* `retries` is observed. Spec 5.3 makes the default 3 for a scratch target, and
     * that default is the hazard rule 18 exists to close.
     */
    @Test
    fun theMinimalFileLoadsAndOmittedFieldsTakeTheirDeclaredDefaults() {
        val task = loadOne(root, minimal("task-1")).single()

        assertThat(task.name).isEqualTo("task-1")
        assertThat(task.enabled).isTrue()
        assertThat(task.cron).isNull()
        assertThat(task.chunkSize).isEqualTo(5000)
        assertThat(task.scratchMemoryLimitMb).isNull()
        assertThat(task.onSuccess).isNull()
        assertThat(task.vars).isEmpty()

        val step = task.phases.single().steps.single() as SqlStep
        assertThat(step.retries)
            .describedAs("spec 5.3 defaults retries to 3 for a step on the scratch datasource")
            .isEqualTo(3)
    }

    @Test
    fun aVariableExportedBeforeItsUseLoads() {
        val task = loadOne(root, orderedVars(exportFirst = true)).single()

        assertThat(task.phases.single().steps.map { it.name })
            .containsExactly("read-watermark", "use-watermark")
    }

    /**
     * Spec 3.1 lists `description` as an optional task field and `TaskDefinition` has nowhere to
     * put it, so rule 1's "unknown fields rejected" and the schema disagree on this one key.
     *
     * Isolated deliberately. The baseline above does not carry a `description`, so whichever way
     * this goes it costs one test rather than every test in the phase.
     */
    @Test
    fun aDescriptionIsAcceptedEvenThoughTheModelHasNoFieldForIt() {
        val yaml = TaskFiles.edit(VALID, "name: wip-summary", "name: wip-summary\ndescription: \"a task\"")

        val task = loadOne(root, yaml).single()

        assertThat(task.name).isEqualTo("wip-summary")
    }
}
