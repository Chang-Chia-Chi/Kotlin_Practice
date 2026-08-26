package infra.simpleetl

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.nio.file.Path
import java.sql.SQLDataException
import java.sql.SQLException
import java.sql.SQLIntegrityConstraintViolationException
import java.sql.SQLRecoverableException
import java.sql.SQLTimeoutException
import java.sql.SQLTransientException
import java.util.function.Supplier

/**
 * P5, done-when items 2 and 3: **retry follows spec 5.3** - transient classification, exponential
 * backoff, scratch defaults and the attempt-suffix cleanup from P4 - and **a non-transient
 * failure fails immediately with no retry.**
 *
 * ### Nothing here sleeps
 *
 * Spec 5.3's backoff doubles from 2s and caps at 30s, so the cap case alone would cost 90
 * seconds of real waiting and would still only be asserting a stopwatch. The engine takes an
 * injected sleeper and [TaskHarness.delaysMillis] records what it *asked for*, which is both
 * faster and a stronger assertion: elapsed wall time cannot tell 30s-capped from 32s-doubled,
 * and a requested-delay list can.
 *
 * ### Classification is asserted in both directions
 *
 * A suite that only proved transient failures retry would pass against an engine that retried
 * everything, which is the failure spec 5.3 names outright: retrying a deterministic failure
 * three times turns a ten minute failure into a thirty minute one. So the non-transient half
 * counts attempts and demands exactly one.
 *
 * The count comes from [DuckFile.attempts], a real statement counter on a real datasource,
 * rather than from anything the engine reports about itself.
 *
 * ### What the classifier actually sees
 *
 * Measured on JDBI 3.45.4 and duckdb_jdbc 1.1.3 (P5 scratchpad probe): a `SQLTransientException`
 * raised by the driver arrives as `org.jdbi.v3.core.result.ResultSetException` with the
 * SQLException as its cause, and one raised while connecting arrives inside a
 * `ConnectionException`. Every JDBC failure reaches Layer 2 wrapped, so spec 5.3's set can only
 * be recognised through the cause chain. These tests inject at the driver, which is where a real
 * transient failure comes from, so they assert that behaviour rather than a convenient one.
 */
class TaskEngineRetryTest {

    @TempDir
    lateinit var root: Path

    companion object {

        /** Spec 5.3's transient set, exactly. */
        @JvmStatic
        fun transientFailures(): List<Arguments> = listOf(
            Arguments.of("SQLTransientException", Supplier<Throwable> { SQLTransientException("probe: transient") }),
            Arguments.of("SQLRecoverableException", Supplier<Throwable> { SQLRecoverableException("probe: recoverable") }),
            Arguments.of("SQLTimeoutException", Supplier<Throwable> { SQLTimeoutException("probe: timeout") }),
            Arguments.of(
                "SQLState class 08",
                Supplier<Throwable> { SQLException("probe: connection lost", "08006") },
            ),
        )

        /** Everything else, including a failure that is not a `SQLException` at all. */
        @JvmStatic
        fun nonTransientFailures(): List<Arguments> = listOf(
            Arguments.of("SQLDataException", Supplier<Throwable> { SQLDataException("probe: bad value", "22003") }),
            Arguments.of(
                "SQLIntegrityConstraintViolationException",
                Supplier<Throwable> { SQLIntegrityConstraintViolationException("probe: duplicate key", "23000") },
            ),
            Arguments.of(
                "SQLState class 42",
                Supplier<Throwable> { SQLException("probe: no such column", "42703") },
            ),
            Arguments.of(
                "not a SQLException at all",
                Supplier<Throwable> { IllegalStateException("probe: a framework error") },
            ),
        )
    }

    /**
     * One `sql` step on one datasource, with two retries allowed. One statement per step, so
     * [DuckFile.attempts] is the attempt count. `CREATE OR REPLACE` because a retry re-runs the
     * statement and must not fail for a second reason.
     */
    private fun oneStatementTask(retries: Int, statement: String = "create or replace table touched as select 1 as ok") =
        Etl.task(
            "wip-retry",
            Etl.phase("extract", Etl.sql("touch", "report_oracle", statement, retries = retries)),
        )

    @ParameterizedTest(name = "{0}")
    @MethodSource("transientFailures")
    fun aTransientFailureIsRetriedAndTheStepEventuallySucceeds(label: String, build: Supplier<Throwable>) {
        TaskHarness(root).use { harness ->
            val report = harness.datasource("report_oracle")
            report.failFirst(count = 2, afterRows = 0) { build.get() }

            val outcome = harness.run(oneStatementTask(retries = 2))

            assertThat(outcome.outcome).describedAs("%s is transient (spec 5.3)", label).isEqualTo(Outcome.SUCCEEDED)
            assertThat(report.attempts.get()).describedAs("attempts").isEqualTo(3)
            assertThat(harness.delaysMillis).describedAs("requested backoff").containsExactly(2_000L, 4_000L)
            assertThat(report.tableExists("touched"))
                .describedAs("the third attempt did the work, not just the bookkeeping")
                .isTrue()
        }
    }

    /**
     * The direction that carries the discriminating power. `attempts == 1` is the assertion an
     * engine that retries everything cannot pass.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("nonTransientFailures")
    fun aNonTransientFailureFailsImmediatelyWithNoRetry(label: String, build: Supplier<Throwable>) {
        TaskHarness(root).use { harness ->
            val report = harness.datasource("report_oracle")
            report.failFirst(count = 9, afterRows = 0) { build.get() }

            val outcome = harness.run(oneStatementTask(retries = 2))

            assertThat(outcome.outcome).describedAs("%s is not transient (spec 5.3)", label).isEqualTo(Outcome.FAILED)
            assertThat(outcome.failure).describedAs("the failure the task carries").isNotNull()
            assertThat(report.attempts.get())
                .describedAs("attempts for %s - retrying this would turn a 10 minute failure into a 30 minute one", label)
                .isEqualTo(1)
            assertThat(harness.delaysMillis).describedAs("requested backoff").isEmpty()
        }
    }

    /**
     * A real driver error rather than an injected one, and the case an implementation is most
     * likely to get wrong: measured on duckdb_jdbc 1.1.3, a syntax error arrives as a plain
     * `java.sql.SQLException` with a **null** SQLState, wrapped by JDBI in
     * `UnableToCreateStatementException`. A classifier that read `sqlState.startsWith("08")`
     * without a null guard would throw from inside its own error handling.
     */
    @Test
    fun aRealSyntaxErrorIsNotTransient() {
        TaskHarness(root).use { harness ->
            val report = harness.datasource("report_oracle")

            val outcome = harness.run(oneStatementTask(retries = 2, statement = "this is not sql"))

            assertThat(outcome.outcome).isEqualTo(Outcome.FAILED)
            assertThat(report.attempts.get()).describedAs("attempts").isEqualTo(1)
            assertThat(harness.delaysMillis).isEmpty()

            // The assertions that make this test falsifiable. A classifier written as
            // `sqlState!!.startsWith("08")` raises a NullPointerException on this very case, and
            // that NPE escapes `run`'s `catch (e: Exception)` as a perfectly ordinary non-null
            // failure with one attempt and no delays - so every assertion above stays green
            // against the implementation this test exists to reject. Measured by the P5 reviewer.
            // What the task carries has to be the driver's failure, not one the retry loop raised
            // while deciding what to do about it.
            val chain = generateSequence(outcome.failure) { if (it.cause === it) null else it.cause }.take(16).toList()
            assertThat(chain)
                .describedAs("the carried failure chain")
                .isNotEmpty
                .noneMatch { it is NullPointerException }
                .anyMatch { it is SQLException }
            assertThat(chain.joinToString(" ") { it.message.orEmpty() })
                .describedAs("the diagnostic names the statement the author has to fix")
                .contains("this is not sql")
        }
    }

    /**
     * Spec 5.3: exponential from 2s, doubling, capped at 30s. Six retries is the first count that
     * reaches the cap twice, which is what separates "capped" from "still doubling".
     */
    @Test
    fun backoffDoublesFromTwoSecondsAndCapsAtThirty() {
        TaskHarness(root).use { harness ->
            val report = harness.datasource("report_oracle")
            report.failAlways(afterRows = 0) { SQLTransientException("probe: always transient") }

            val outcome = harness.run(oneStatementTask(retries = 6))

            assertThat(outcome.outcome).isEqualTo(Outcome.FAILED)
            assertThat(report.attempts.get()).describedAs("one attempt plus six retries").isEqualTo(7)
            assertThat(harness.delaysMillis)
                .describedAs("spec 5.3 backoff")
                .containsExactly(2_000L, 4_000L, 8_000L, 16_000L, 30_000L, 30_000L)
        }
    }

    /**
     * Spec 5.3's defaults: 3 for a scratch target. Observed by not stating `retries` at all and
     * failing twice - an engine defaulting to 0, or to 1, cannot reach a third attempt.
     *
     * The failure is raised part way through the source result set, which is also what makes the
     * attempt-suffix test below possible; see [DuckFile].
     */
    @Test
    fun aScratchTargetDefaultsToThreeRetries() {
        TaskHarness(root).use { harness ->
            val mes = harness.datasource("oracle_mes")
            mes.createSourceTable("wip", rows = 6, marker = "w")
            mes.failFirst(count = 2, afterRows = 2) { SQLTransientException("probe: transient") }

            val definition = Etl.task(
                "wip-scratch-default",
                Etl.phase(
                    "extract",
                    Etl.pipe("load-wip", "oracle_mes", "select lot_id, lot_code, qty from wip", table = "wip_stg"),
                ),
            )

            val outcome = harness.run(definition)

            assertThat(outcome.outcome).isEqualTo(Outcome.SUCCEEDED)
            assertThat(mes.attempts.get()).describedAs("attempts on an unstated scratch retries").isEqualTo(3)
            assertThat(harness.delaysMillis).containsExactly(2_000L, 4_000L)
        }
    }

    /**
     * The other half of the same default: 0 for any other target. A `sql` step on an external
     * datasource with `retries` unstated must not retry even a transient failure, because the
     * framework cannot make a partially written external target safe on its own (spec 5.3).
     */
    @Test
    fun aNonScratchTargetDefaultsToNoRetries() {
        TaskHarness(root).use { harness ->
            val report = harness.datasource("report_oracle")
            report.failFirst(count = 1, afterRows = 0) { SQLTransientException("probe: transient") }

            val definition = Etl.task(
                "wip-external-default",
                Etl.phase(
                    "publish",
                    Etl.sql("publish", "report_oracle", "create or replace table published as select 1 as ok"),
                ),
            )

            val outcome = harness.run(definition)

            assertThat(outcome.outcome).isEqualTo(Outcome.FAILED)
            assertThat(report.attempts.get())
                .describedAs("attempts on an unstated non-scratch retries - the transient set does not override the default")
                .isEqualTo(1)
            assertThat(harness.delaysMillis).isEmpty()
        }
    }

    /**
     * Spec 5.5 driven by the engine, which is the half P4 could not prove. `DatasetNamerTest`
     * showed the names are right; this shows the engine chooses the attempt, publishes only after
     * the attempt succeeded, and leaves the failed attempt alone.
     *
     * Three things are asserted and one deliberately is not:
     *
     * - the stable name resolves to the second attempt's data,
     * - `wip_stg__a1` still exists, because nothing is ever dropped on DuckDB 1.1.3,
     * - the stable view points at `__a2` and not at `__a1`, which is what "unreferenced" means,
     * - **nothing about how many rows `wip_stg__a1` holds.** A failed attempt retains between
     *   zero and one chunk of rows depending on where the failure landed (spec 12, three measured
     *   shapes), so an assertion either way would be pinning down something the framework does
     *   not control.
     */
    @Test
    fun aRetriedScratchDatasetPublishesTheAttemptThatSucceeded() {
        TaskHarness(root).use { harness ->
            val mes = harness.datasource("oracle_mes")
            mes.createSourceTable("wip", rows = 6, marker = "w")
            mes.failFirst(count = 1, afterRows = 2) { SQLTransientException("probe: transient at row 3") }
            val probe = harness.probeFile("attempts")

            val definition = Etl.task(
                "wip-attempts",
                Etl.phase(
                    "extract",
                    Etl.pipe(
                        name = "load-wip",
                        sourceDatasource = "oracle_mes",
                        sql = "select lot_id, lot_code, qty from wip",
                        table = "wip_stg",
                        retries = 3,
                    ),
                ),
                Etl.phase("check", Etl.probeScratch("copy-out", probe, "wip_stg")),
            )

            harness.runExpectingSuccess(definition)

            assertThat(mes.attempts.get()).describedAs("attempts").isEqualTo(2)
            assertThat(harness.delaysMillis).containsExactly(2_000L)

            harness.readProbe(probe) { probeDb ->
                assertThat(Etl.strings(probeDb, "select lot_code from wip_stg order by lot_id"))
                    .describedAs("the stable name resolves to the attempt that succeeded")
                    .containsExactly("w-0", "w-1", "w-2", "w-3", "w-4", "w-5")

                assertThat(Etl.strings(probeDb, "select table_name from probe_tables"))
                    .describedAs("the failed attempt is left in place, never dropped (spec 5.5)")
                    .contains("wip_stg__a1", "wip_stg__a2")

                val stable = Etl.strings(probeDb, "select sql from probe_views where view_name = 'wip_stg'").single()
                assertThat(stable).describedAs("the stable view").contains("wip_stg__a2")
                assertThat(stable).describedAs("the first attempt is unreferenced").doesNotContain("wip_stg__a1")
            }
        }
    }
}
