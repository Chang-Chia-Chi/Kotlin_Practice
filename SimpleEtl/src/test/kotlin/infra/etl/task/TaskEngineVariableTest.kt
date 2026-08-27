package infra.etl.task

import infra.etl.Etl
import infra.etl.TaskHarness
import infra.etl.task.Outcome
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir

/**
 * P5, done-when item 4: **variables resolve in step order; a variable used before its export is
 * an error; an export returning two rows is an error.** Spec 6.
 *
 * Variable values are not on `TaskOutcome`, so every assertion here reads what a *later step*
 * did with the value: a `sql` step on an external datasource writes the binding into a table the
 * test can read once the run has returned. That keeps the test on the public API and, more
 * usefully, asserts the thing that matters - not that the scope holds a value, but that the value
 * reached the SQL of a step in a later phase.
 *
 * The two error cases assert `FAILED` rather than a message, because spec 6 fixes the rule and
 * not the wording. Both are cases an implementation could plausibly get wrong by *succeeding* -
 * binding an unset name as null, or silently taking the first of two rows - so the outcome
 * assertion is the discriminating one and the message check only confirms the diagnostic names
 * something the author can act on.
 */
class TaskEngineVariableTest {

    @TempDir
    lateinit var root: Path

    /**
     * All three sources of spec 6.1 at once - built-in, literal, exported - crossing a phase
     * boundary, which is spec 6.2's "task scope, evaluated in step order".
     *
     * `runId` is cross-checked against `TaskOutcome.runId`: the built-in a task binds and the
     * identifier the caller is handed must be the same string, or log correlation is a fiction.
     */
    @Test
    fun variablesResolveInStepOrderAcrossPhases() {
        TaskHarness(root).use { harness ->
            val report = harness.datasource("report_oracle")
            report.exec("create table watermark as select cast('2026-08-01' as varchar) as processed_ts")

            val definition = Etl.task(
                "wip-summary",
                Etl.phase(
                    "extract",
                    Etl.export("read-watermark", "report_oracle", "lastTs" to "select max(processed_ts) from watermark"),
                ),
                Etl.phase(
                    "publish",
                    Etl.sql(
                        "publish",
                        "report_oracle",
                        "create or replace table published as " +
                            "select :lastTs as last_ts, :siteCode as site, :taskName as task, :runId as run_id",
                    ),
                ),
                vars = listOf(Etl.literal("siteCode", "F12")),
            )

            val outcome = harness.runExpectingSuccess(definition)

            assertAll(
                {
                    assertEquals(listOf("2026-08-01"), report.strings("select last_ts from published")) {
                        "exported in phase 1, bound in phase 2"
                    }
                },
                {
                    assertEquals(listOf("F12"), report.strings("select site from published")) {
                        "literal task variable (spec 6.1)"
                    }
                },
                {
                    assertEquals(listOf("wip-summary"), report.strings("select task from published")) {
                        "built-in taskName"
                    }
                },
                {
                    assertEquals(listOf(outcome.runId), report.strings("select run_id from published")) {
                        "the built-in runId must be the runId the caller was handed"
                    }
                },
            )
        }
    }

    /**
     * Spec 6.2: evaluation is in step order, so a name exported later is not available earlier.
     *
     * The wrong behaviour here is not a crash, it is a *success*: an engine that bound an unknown
     * name as null would write a null row and report SUCCEEDED, and the task would silently
     * process nothing every night. So the test asserts the failure and then asserts the table was
     * never written.
     */
    @Test
    fun aVariableUsedBeforeItsExportIsAnError() {
        TaskHarness(root).use { harness ->
            val report = harness.datasource("report_oracle")
            report.exec("create table watermark as select cast('2026-08-01' as varchar) as processed_ts")

            val definition = Etl.task(
                "wip-out-of-order",
                Etl.phase(
                    "publish",
                    Etl.sql("publish", "report_oracle", "create or replace table published as select :lastTs as last_ts"),
                ),
                Etl.phase(
                    "extract",
                    Etl.export("read-watermark", "report_oracle", "lastTs" to "select max(processed_ts) from watermark"),
                ),
            )

            val outcome = harness.run(definition)

            assertAll(
                { assertEquals(Outcome.FAILED, outcome.outcome) },
                { assertNotNull(outcome.failure) { "a FAILED run carries no failure" } },
                {
                    assertTrue("lastTs" in outcome.failure.toString()) {
                        "the diagnostic names the variable the author must export first; " +
                            "failure was: ${outcome.failure}"
                    }
                },
                {
                    assertFalse(report.tableExists("published")) {
                        "a step whose variables do not resolve must not half-run, but 'published' exists"
                    }
                },
            )
        }
    }

    /**
     * Spec 6.3: an `export` query returns exactly one row and one column; more than one row is an
     * error.
     *
     * The failure mode this guards is again a success - taking the first row and carrying on -
     * which would make the task's output depend on a row order the query never specified.
     */
    @Test
    fun anExportReturningTwoRowsIsAnError() {
        TaskHarness(root).use { harness ->
            val report = harness.datasource("report_oracle")
            report.exec("create table watermark as select cast(i as varchar) as processed_ts from range(0, 2) t(i)")

            val definition = Etl.task(
                "wip-two-rows",
                Etl.phase(
                    "extract",
                    Etl.export("read-watermark", "report_oracle", "lastTs" to "select processed_ts from watermark"),
                ),
                Etl.phase(
                    "publish",
                    Etl.sql("publish", "report_oracle", "create or replace table published as select :lastTs as last_ts"),
                ),
            )

            val outcome = harness.run(definition)

            assertAll(
                { assertEquals(Outcome.FAILED, outcome.outcome) },
                { assertNotNull(outcome.failure) { "a FAILED run carries no failure" } },
                {
                    val failure = outcome.failure.toString()
                    assertTrue(listOf("read-watermark", "lastTs").any { it in failure }) {
                        "the diagnostic names the export step or its variable; failure was: $failure"
                    }
                },
                {
                    assertFalse(report.tableExists("published")) {
                        "the run stopped at the bad export, so the later phase never ran, but 'published' exists"
                    }
                },
            )
        }
    }

    /**
     * The other half of spec 6.3, and the reason the two-row case cannot simply be "any row count
     * other than one is an error": **zero rows yields null.** A watermark table that is empty on
     * the first ever run is the case this exists for.
     *
     * Measured on duckdb_jdbc 1.1.3: a null bound through `setObject` reaches a
     * `CREATE TABLE AS SELECT ?` as a real SQL NULL, so the readback below distinguishes null
     * from the empty string.
     */
    @Test
    fun anExportReturningNoRowsYieldsNull() {
        TaskHarness(root).use { harness ->
            val report = harness.datasource("report_oracle")
            report.exec("create table watermark as select cast(i as varchar) as processed_ts from range(0, 0) t(i)")

            val definition = Etl.task(
                "wip-no-rows",
                Etl.phase(
                    "extract",
                    // Not an aggregate: `select max(x) from <empty>` returns one row holding
                    // null, which is a different case from the zero rows spec 6.3 names.
                    Etl.export("read-watermark", "report_oracle", "lastTs" to "select processed_ts from watermark"),
                ),
                Etl.phase(
                    "publish",
                    Etl.sql("publish", "report_oracle", "create or replace table published as select :lastTs as last_ts"),
                ),
            )

            harness.runExpectingSuccess(definition)

            assertEquals(1L, report.longAt("select count(*) from published where last_ts is null")) {
                "zero rows yields null, not an error and not an empty string (spec 6.3)"
            }
        }
    }

    /** Spec 6.2: a variable may not be redefined once set. */
    @Test
    fun aVariableMayNotBeRedefinedOnceSet() {
        TaskHarness(root).use { harness ->
            harness.datasource("report_oracle")

            val definition = Etl.task(
                "wip-redefined",
                Etl.phase(
                    "extract",
                    Etl.export("read-site", "report_oracle", "siteCode" to "select 'F12'"),
                    Etl.export("read-site-again", "report_oracle", "siteCode" to "select 'F13'"),
                ),
            )

            val outcome = harness.run(definition)

            assertAll(
                { assertEquals(Outcome.FAILED, outcome.outcome) },
                {
                    val failure = outcome.failure.toString()
                    assertTrue(listOf("siteCode", "read-site-again").any { it in failure }) {
                        "the diagnostic names neither the variable nor the step; failure was: $failure"
                    }
                },
            )
        }
    }

    /**
     * Spec 1.3 and 6.1: a literal `vars` entry may not be null.
     *
     * A null carries no type, and the framework never guesses one. An exported null is different -
     * it takes the canonical type of the column its export query selected - but a literal written
     * in a task file has no column to take a type from, so there is nothing to bind it as.
     */
    @Test
    fun aLiteralVarWithANullValueIsRejected() {
        TaskHarness(root).use { harness ->
            val report = harness.datasource("report_oracle")

            val attempt = runCatching {
                harness.run(
                    Etl.task(
                        "wip-null-literal",
                        Etl.phase(
                            "publish",
                            Etl.sql("publish", "report_oracle", "create or replace table published as select :siteCode as site"),
                        ),
                        vars = listOf(Etl.literal("siteCode", null)),
                    ),
                )
            }

            assertNotNull(attempt.exceptionOrNull() ?: attempt.getOrNull()?.failure) {
                "a null literal var has no type to bind as (spec 1.3)"
            }
            // The assertion that makes this test mean what it says. "Something threw" is also
            // satisfied by a driver refusing an untyped null at bind time, which is a different
            // defect with a different fix and would leave the rule unimplemented behind a green
            // test. A rule rejects the definition before the step reaches the database.
            assertAll(
                {
                    assertEquals(0, report.attempts.get()) {
                        "a null literal is rejected as a definition, not discovered at the driver"
                    }
                },
                {
                    assertFalse(report.tableExists("published")) {
                        "'published' exists, so the rejected step reached the database"
                    }
                },
            )
        }
    }

    /**
     * The remedy for a task variable a `target.sql` statement needs, now that spec 6.3 binds Row
     * keys only there: project the variable into the source select list, where
     * `JdbcSource.parameters` binds it and it arrives at the target as an ordinary, lower-cased
     * Row key.
     *
     * Worth pinning as an end-to-end idiom rather than left to a doc comment - it is the only
     * route a task variable now has into a target, and it has to survive AUTO DDL generation from
     * source metadata, which is where a bound parameter in a select list could plausibly go wrong.
     */
    @Test
    fun aTaskVariableReachesATargetByProjectionIntoTheSourceSelectList() {
        TaskHarness(root).use { harness ->
            val mes = harness.datasource("oracle_mes")
            mes.createSourceTable("wip", rows = 3, marker = "w")
            val probe = harness.probeFile("projected")

            val definition = Etl.task(
                "wip-projected",
                Etl.phase(
                    "extract",
                    Etl.pipe(
                        name = "load-wip",
                        sourceDatasource = "oracle_mes",
                        sql = "select lot_id, lot_code, :siteCode as site_code from wip",
                        table = "wip_stg",
                    ),
                    Etl.probeScratch("copy-out", probe, "wip_stg"),
                ),
                vars = listOf(Etl.literal("siteCode", "F12")),
            )

            harness.runExpectingSuccess(definition)

            harness.readProbe(probe) { probeDb ->
                assertEquals(
                    listOf("F12", "F12", "F12"),
                    Etl.strings(probeDb, "select site_code from wip_stg order by lot_id"),
                ) { "the projected task variable landed as an ordinary column" }
            }
        }
    }

    /**
     * The consequence of a typed null that will surprise a task author, pinned so that nobody
     * "fixes" it later: **`ts > :lastTs` with a null watermark matches nothing.**
     *
     * That is SQL's three-valued logic acting on the author's own predicate, not the framework
     * guessing a value, and the framework must not paper over it - a null that quietly behaved
     * like "match everything" would make the first run of a watermarked task read the whole table
     * by accident rather than by intent. The idiom for a first run that must read everything is
     * the explicit guard, and both halves are asserted here so the pair documents itself.
     */
    @Test
    fun aNullWatermarkMatchesNothingUnlessTheAuthorGuardsForIt() {
        TaskHarness(root).use { harness ->
            val mes = harness.datasource("oracle_mes")
            mes.createSourceTable("wip", rows = 4, marker = "w")
            val probe = harness.probeFile("watermark")

            val definition = Etl.task(
                "wip-watermark",
                Etl.phase(
                    "extract",
                    // Zero rows, so `lastTs` is defined and null (spec 6.3), typed from the
                    // export query's own column rather than bound as an untyped null.
                    Etl.export("read-watermark", "oracle_mes", "lastTs" to "select lot_code from wip where 1 = 0"),
                    Etl.pipe(
                        name = "load-strict",
                        sourceDatasource = "oracle_mes",
                        sql = "select lot_id, lot_code from wip where lot_code > :lastTs",
                        table = "wip_strict",
                    ),
                    Etl.pipe(
                        name = "load-guarded",
                        sourceDatasource = "oracle_mes",
                        sql = "select lot_id, lot_code from wip where :lastTs is null or lot_code > :lastTs",
                        table = "wip_guarded",
                    ),
                    Etl.probeScratch("copy-out", probe, "wip_strict", "wip_guarded"),
                ),
            )

            harness.runExpectingSuccess(definition)

            harness.readProbe(probe) { probeDb ->
                assertAll(
                    {
                        assertEquals(0L, Etl.longAt(probeDb, "select count(*) from wip_strict")) {
                            "a comparison against a null watermark matches nothing"
                        }
                    },
                    {
                        assertEquals(4L, Etl.longAt(probeDb, "select count(*) from wip_guarded")) {
                            "the explicit guard is how a first run reads everything"
                        }
                    },
                )
            }
        }
    }

    /**
     * Spec 6.3: variables bind into a `pipe` step's source SQL too, not only into statements.
     * That is the binding the framework exists for - `where upd_ts > :lastTs` is the whole point
     * of an `export` step - and it is a different code path from a `sql` step's statements.
     */
    @Test
    fun anExportedVariableBindsIntoALaterPipeSourceQuery() {
        TaskHarness(root).use { harness ->
            val mes = harness.datasource("oracle_mes")
            mes.createSourceTable("wip", rows = 6, marker = "w")
            val probe = harness.probeFile("bound-source")

            val definition = Etl.task(
                "wip-bound-source",
                Etl.phase(
                    "extract",
                    Etl.export("read-watermark", "oracle_mes", "lastLot" to "select cast(3 as bigint)"),
                    Etl.pipe(
                        name = "load-wip",
                        sourceDatasource = "oracle_mes",
                        sql = "select lot_id, lot_code, qty from wip where lot_id >= :lastLot",
                        table = "wip_stg",
                    ),
                    Etl.probeScratch("copy-out", probe, "wip_stg"),
                ),
            )

            harness.runExpectingSuccess(definition)

            harness.readProbe(probe) { probeDb ->
                assertEquals(
                    listOf("w-3", "w-4", "w-5"),
                    Etl.strings(probeDb, "select lot_code from wip_stg order by lot_id"),
                ) { "the exported watermark filtered the source query" }
            }
        }
    }
}
