package infra.etl

import infra.etl.pipe.CanonicalType
import infra.etl.pipe.ColumnMeta
import infra.etl.pipe.RowTransform
import infra.etl.task.MaterializeFormat
import infra.etl.task.Outcome
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * P5, done-when item 1: **all four step types execute against a definition built in code, with
 * no YAML involved.**
 *
 * Every definition here is assembled from constructors through [Etl]. Nothing in this file parses
 * a string into a task, which is the point of the item: `TaskDefinition` is a public,
 * programmatically constructible type and YAML is only one source of one (spec 2.1). P6 owns
 * loading, and a test here that reached for a file would have failed the item rather than proved
 * it.
 *
 * The shape under test is spec 2.4's shape B - land in scratch, derive inside scratch, publish
 * outward - because it is the only shape that exercises all four types at once and it is the one
 * the framework exists for.
 *
 * Scratch is deleted at run end, so what landed there is read through the ATTACH probe of
 * [Etl.probeScratch], a plain `sql` step. See the KDoc on [Etl] for why the observation has to
 * happen inside the run.
 */
class TaskEngineStepTypesTest {

    @TempDir
    lateinit var root: Path

    /**
     * pipe, materialize, export and sql, in one run, in three phases.
     *
     * The assertions are on data rather than on outcome alone: a `SUCCEEDED` outcome would also
     * be returned by an engine whose executors did nothing at all, which is the failure this
     * item is really guarding against.
     */
    @Test
    fun allFourStepTypesExecuteFromADefinitionBuiltInCode() {
        TaskHarness(root).use { harness ->
            val mes = harness.datasource("oracle_mes")
            mes.createSourceTable("wip", rows = 6, marker = "w")
            val report = harness.datasource("report_oracle")
            val probe = harness.probeFile("shape-b")

            val definition = Etl.task(
                "wip-summary",
                Etl.phase(
                    "extract",
                    Etl.pipe(
                        name = "load-wip",
                        sourceDatasource = "oracle_mes",
                        sql = "select lot_id, lot_code, qty from wip",
                        table = "wip_stg",
                    ),
                ),
                Etl.phase(
                    "build",
                    Etl.materialize(
                        name = "build-summary",
                        output = "summary",
                        sql = "select lot_code, qty from wip_stg where lot_id >= 2",
                    ),
                    Etl.probeScratch("copy-out", probe, "wip_stg", "summary"),
                ),
                Etl.phase(
                    "publish",
                    Etl.export("read-site", "report_oracle", "siteCode" to "select 'F12'"),
                    Etl.sql(
                        "publish",
                        "report_oracle",
                        "create or replace table wip_summary as select :siteCode as site, 4 as rows_seen",
                    ),
                ),
            )

            harness.runExpectingSuccess(definition)

            // pipe: every source row landed under the stable name of spec 5.5
            harness.readProbe(probe) { probeDb ->
                assertThat(Etl.strings(probeDb, "select lot_code from wip_stg order by lot_id"))
                    .describedAs("the pipe step's rows, read through the stable view")
                    .containsExactly("w-0", "w-1", "w-2", "w-3", "w-4", "w-5")

                // materialize: derived inside scratch, four of the six rows
                assertThat(Etl.strings(probeDb, "select lot_code from summary order by lot_code"))
                    .describedAs("the materialize step's output")
                    .containsExactly("w-2", "w-3", "w-4", "w-5")

                // both datasets are reached through a stable view, not a bare table (spec 5.5)
                assertThat(Etl.strings(probeDb, "select view_name from probe_views"))
                    .contains("wip_stg", "summary")
            }

            // export + sql: the exported variable bound into a later step's statement
            assertThat(report.tableExists("wip_summary")).isTrue()
            assertThat(report.strings("select site from wip_summary")).containsExactly("F12")
            assertThat(report.longAt("select rows_seen from wip_summary")).isEqualTo(4L)
        }
    }

    /**
     * Spec 5.6: `format: PARQUET` puts the dataset in a file instead of a table, and the stable
     * name resolves over `read_parquet` so that no other step changes.
     *
     * Asserting the rows alone would pass against an engine that quietly ignored `format`, so the
     * view definition is asserted too - that is the only thing that distinguishes the two.
     */
    @Test
    fun aMaterializeStepCanProduceParquetBehindTheSameStableName() {
        TaskHarness(root).use { harness ->
            val mes = harness.datasource("oracle_mes")
            mes.createSourceTable("wip", rows = 5, marker = "p")
            val probe = harness.probeFile("parquet")

            val definition = Etl.task(
                "wip-parquet",
                Etl.phase(
                    "extract",
                    Etl.pipe("load-wip", "oracle_mes", "select lot_id, lot_code, qty from wip", table = "wip_stg"),
                ),
                Etl.phase(
                    "build",
                    Etl.materialize(
                        name = "build-summary",
                        output = "summary",
                        sql = "select lot_id, lot_code from wip_stg where lot_id < 3",
                        format = MaterializeFormat.PARQUET,
                    ),
                    Etl.probeScratch("copy-out", probe, "summary"),
                ),
            )

            harness.runExpectingSuccess(definition)

            harness.readProbe(probe) { probeDb ->
                assertThat(Etl.strings(probeDb, "select lot_code from summary order by lot_id"))
                    .containsExactly("p-0", "p-1", "p-2")
                assertThat(Etl.strings(probeDb, "select sql from probe_views where view_name = 'summary'").single())
                    .describedAs("the stable view of a PARQUET materialisation reads the file (spec 5.6)")
                    .contains("read_parquet")
            }
        }
    }

    /**
     * Spec 9.1 through Layer 2: a `pipe` step's transform is wired to the pipe. P3 proved
     * `RowPipe` applies a transform; this proves the step carries one to it, which is a different
     * claim and is the seam that has no test otherwise.
     *
     * The transform drops rows rather than adding a column, deliberately: under
     * `createTable: AUTO` an added column is silently dropped, because AUTO's DDL comes from
     * source metadata (P3's handover note, validation rule 14), so an added-column assertion here
     * would be asserting a known gap that belongs to P6.
     */
    @Test
    fun aPipeStepAppliesItsTransform() {
        TaskHarness(root).use { harness ->
            val mes = harness.datasource("oracle_mes")
            mes.createSourceTable("wip", rows = 6, marker = "t")
            val probe = harness.probeFile("transform")

            val evenLotsOnly = RowTransform { row -> if (row.long("lot_id")!! % 2L == 0L) row else null }

            val definition = Etl.task(
                "wip-transform",
                Etl.phase(
                    "extract",
                    Etl.pipe(
                        name = "load-wip",
                        sourceDatasource = "oracle_mes",
                        sql = "select lot_id, lot_code, qty from wip",
                        table = "wip_stg",
                        transform = evenLotsOnly,
                    ),
                    Etl.probeScratch("copy-out", probe, "wip_stg"),
                ),
            )

            harness.runExpectingSuccess(definition)

            harness.readProbe(probe) { probeDb ->
                assertThat(Etl.strings(probeDb, "select lot_code from wip_stg order by lot_id"))
                    .describedAs("the transform dropped the odd rows")
                    .containsExactly("t-0", "t-2", "t-4")
            }
        }
    }

    /**
     * Spec 9.1 and validation rule 14: a column a transform *adds* is not in the source metadata,
     * so under `createTable: AUTO` the generated DDL cannot describe it and the value is silently
     * dropped - measured in P3, and recorded there as Layer 2's to carry. `addColumns` is how
     * Layer 2 carries it.
     *
     * The assertion that matters is the second one. The row count would be identical whether the
     * column landed or was dropped, which is precisely why the gap survived P3 with a green
     * suite; only reading the added column back can tell.
     */
    @Test
    fun aTransformAddedColumnLandsWhenItIsDeclared() {
        TaskHarness(root).use { harness ->
            val mes = harness.datasource("oracle_mes")
            mes.createSourceTable("wip", rows = 4, marker = "h")
            val probe = harness.probeFile("added-column")

            val hashing = RowTransform { row -> row.with("row_hash", "h:${row.long("lot_id")}") }

            val definition = Etl.task(
                "wip-hash",
                Etl.phase(
                    "extract",
                    Etl.pipe(
                        name = "load-wip",
                        sourceDatasource = "oracle_mes",
                        sql = "select lot_id, lot_code, qty from wip",
                        table = "wip_stg",
                        transform = hashing,
                        addColumns = listOf(ColumnMeta("row_hash", CanonicalType.STRING, nullable = true)),
                    ),
                    Etl.probeScratch("copy-out", probe, "wip_stg"),
                ),
            )

            harness.runExpectingSuccess(definition)

            harness.readProbe(probe) { probeDb ->
                assertThat(Etl.strings(probeDb, "select row_hash from wip_stg order by lot_id"))
                    .describedAs("a declared, transform-added column reaches the target under AUTO")
                    .containsExactly("h:0", "h:1", "h:2", "h:3")
            }
        }
    }

    /**
     * The lead's ruling: `CacheCopyStep` belongs to P9 and its executor is a stub throwing
     * `NotImplementedError`. This test does not test the step; it tests that the stub is a stub.
     *
     * A stub that silently returned would make a task file referring to the snapshot cache look
     * like it worked - the worst possible reading of a not-yet-built feature - and P9 would then
     * inherit a green suite that proves nothing. `NotImplementedError` is an `Error`, not an
     * `Exception`, so the engine may reasonably let it escape `run` instead of recording it in
     * `TaskOutcome`; both are accepted here, and a silent success is not.
     */
    @Test
    fun aCacheCopyStepIsNotSilentlyANoOp() {
        TaskHarness(root).use { harness ->
            harness.datasource("report_oracle")

            val definition = Etl.task(
                "wip-cache",
                Etl.phase(
                    "extract",
                    Etl.cacheCopy(
                        name = "copy-cache",
                        cache = "wip_cache",
                        sql = "select lot_id, qty from wip where site = 'F12'",
                        output = "wip_cache",
                    ),
                ),
            )

            val attempt = runCatching { harness.run(definition) }
            val failure = attempt.exceptionOrNull() ?: attempt.getOrNull()?.failure

            assertThat(failure)
                .describedAs("a cacheCopy step must fail loudly until P9 builds it, not succeed quietly")
                .isInstanceOf(NotImplementedError::class.java)
            attempt.getOrNull()?.let { assertThat(it.outcome).isEqualTo(Outcome.FAILED) }
        }
    }
}
