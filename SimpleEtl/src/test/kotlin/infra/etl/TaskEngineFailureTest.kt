package infra.etl

import infra.etl.task.Outcome
import infra.etl.task.TriggerSource
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * P5, done-when item 6: **a failure in phase 2 leaves phase 1's external writes committed, and
 * the test asserts this rather than pretending otherwise** - plus the scratch laziness of spec
 * 2.4 shape A, driven through the engine.
 *
 * ### Asserting the absence of rollback
 *
 * Spec 5.4 is explicit: there is no rollback across chunks, steps or phases, and the framework
 * does not attempt cross-phase atomicity because it cannot - the targets may live in different
 * instances. So the assertion below is that the phase 1 table is **still there** after the run
 * failed. That is not a weak test standing in for a stronger one; a test that wished for
 * rollback would be asserting a property the framework has never claimed and cannot deliver, and
 * it would send whoever read it looking for a bug that is a design decision. The mitigation
 * spec 5.4 does offer - `idempotent: true`, or a work table and a swap - is the author's, not
 * the engine's.
 *
 * The external datasource here is a second DuckDB file rather than an Oracle container. A `sql`
 * step's statements are author SQL with side effects (spec 3.4), which is exactly how spec 5.4's
 * own worked example publishes, and `CREATE TABLE AS SELECT` keeps the fixture clear of INSERT
 * into DuckDB. What is being asserted is the absence of a rollback the framework never performs,
 * and that is not a property of the target vendor.
 *
 * ### Asserting laziness where it can actually fail
 *
 * `ScratchDb.close()` empties the scratch directory on every path, so "no scratch file exists
 * after the run" is true whether the engine opened scratch speculatively or never touched it.
 * The observation therefore happens *inside* the run, through a `sql` step that counts files
 * under the scratch root with DuckDB's `glob`, and the two directions are asserted against each
 * other: a task that never names `scratch` must see zero, and a task that does must see more.
 * Neither reading depends on an operating system behaviour.
 */
class TaskEngineFailureTest {

    @TempDir
    lateinit var root: Path

    @Test
    fun aFailureInPhaseTwoLeavesPhaseOneExternalWritesCommitted() {
        TaskHarness(root).use { harness ->
            val report = harness.datasource("report_oracle")

            val definition = Etl.task(
                "wip-two-phases",
                Etl.phase(
                    "publish",
                    Etl.sql(
                        "load-work-table",
                        "report_oracle",
                        "create or replace table wip_summary_work as select cast(i as bigint) as lot_id from range(0, 3) t(i)",
                    ),
                ),
                Etl.phase(
                    "swap",
                    Etl.sql("swap", "report_oracle", "select * from a_table_that_does_not_exist"),
                ),
            )

            val outcome = harness.run(definition)

            assertThat(outcome.outcome).isEqualTo(Outcome.FAILED)
            assertThat(outcome.failure).isNotNull()

            assertThat(report.tableExists("wip_summary_work"))
                .describedAs("spec 5.4: an external write committed in phase 1 stays committed")
                .isTrue()
            assertThat(report.longAt("select count(*) from wip_summary_work"))
                .describedAs("every row phase 1 wrote is still there")
                .isEqualTo(3L)
        }
    }

    /**
     * The other half of a failure test: the engine is usable afterwards. A run that failed must
     * leave no scratch file, no half-open instance and no state that poisons the next run - which
     * is the difference between a failed task and a failed service.
     */
    @Test
    fun theEngineIsUsableAfterAFailedRun() {
        TaskHarness(root).use { harness ->
            val mes = harness.datasource("oracle_mes")
            mes.createSourceTable("wip", rows = 4, marker = "w")
            harness.datasource("report_oracle")

            val failing = Etl.task(
                "wip-failing",
                Etl.phase(
                    "extract",
                    Etl.pipe("load-wip", "oracle_mes", "select lot_id, lot_code, qty from wip", table = "wip_stg"),
                ),
                Etl.phase("swap", Etl.sql("swap", "report_oracle", "select * from a_table_that_does_not_exist")),
            )
            val failed = harness.run(failing)
            assertThat(failed.outcome).isEqualTo(Outcome.FAILED)
            assertThat(harness.scratchFiles())
                .describedAs("spec 7.2: the scratch file is deleted on failure as well as on success")
                .isEmpty()

            val probe = harness.probeFile("after-failure")
            val good = Etl.task(
                "wip-recovered",
                Etl.phase(
                    "extract",
                    Etl.pipe("load-wip", "oracle_mes", "select lot_id, lot_code, qty from wip", table = "wip_stg"),
                    Etl.probeScratch("copy-out", probe, "wip_stg"),
                ),
            )
            val recovered = harness.runExpectingSuccess(good, TriggerSource.API)

            assertThat(recovered.runId)
                .describedAs("a second run is a different run")
                .isNotEqualTo(failed.runId)
            harness.readProbe(probe) { probeDb ->
                assertThat(Etl.longAt(probeDb, "select count(*) from wip_stg")).isEqualTo(4L)
            }
            assertThat(harness.scratchFiles()).isEmpty()
        }
    }

    /**
     * Spec 2.4 shape A: a task that never references `scratch` pays nothing. P4 proved `ScratchDb`
     * is lazy when nobody calls `connection()`; this proves the engine is the nobody - that it
     * does not open the instance speculatively at run start.
     */
    @Test
    fun aTaskThatNeverMentionsScratchOpensNoScratchDatabase() {
        TaskHarness(root).use { harness ->
            val report = harness.datasource("report_oracle")

            val definition = Etl.task(
                "wip-no-scratch",
                Etl.phase(
                    "publish",
                    Etl.export("read-site", "report_oracle", "siteCode" to "select 'F12'"),
                    Etl.sql("publish", "report_oracle", "create or replace table published as select :siteCode as site"),
                    Etl.globCount("count-scratch", "report_oracle", harness.scratchRoot, "scratch_seen"),
                ),
            )

            harness.runExpectingSuccess(definition)

            assertThat(report.longAt("select files from scratch_seen"))
                .describedAs("files under the scratch root while the task was running")
                .isZero()
            assertThat(harness.scratchFiles()).isEmpty()
        }
    }

    /**
     * The reading that makes the one above discriminate. Same harness, same probe, one pipe into
     * scratch: the count must now be non-zero, or the probe is measuring nothing and the shape-A
     * assertion is vacuous.
     */
    @Test
    fun aTaskThatUsesScratchDoesOpenOneWhileItRuns() {
        TaskHarness(root).use { harness ->
            val mes = harness.datasource("oracle_mes")
            mes.createSourceTable("wip", rows = 4, marker = "w")
            val report = harness.datasource("report_oracle")

            val definition = Etl.task(
                "wip-with-scratch",
                Etl.phase(
                    "extract",
                    Etl.pipe("load-wip", "oracle_mes", "select lot_id, lot_code, qty from wip", table = "wip_stg"),
                    Etl.globCount("count-scratch", "report_oracle", harness.scratchRoot, "scratch_seen"),
                ),
            )

            harness.runExpectingSuccess(definition)

            assertThat(report.longAt("select files from scratch_seen"))
                .describedAs("a task that uses scratch has a scratch file while it runs")
                .isPositive()
            assertThat(harness.scratchFiles())
                .describedAs("and none once the run has ended (spec 7.2)")
                .isEmpty()
        }
    }
}
