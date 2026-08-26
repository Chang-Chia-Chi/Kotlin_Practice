package infra.simpleetl

import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.nio.file.Path

/**
 * The engine's own guards: the combinations it must refuse before it touches a database.
 *
 * Every one of these is a `require` in production that no test could fire until now, which is the
 * shape that shipped a real defect in P3 and again in P5 - a guard nothing exercises is
 * indistinguishable from a guard that does not work. Two of them, the scratch statement target and
 * the retried REQUIRED pipe, are reachable *only* from here: their happy paths need a non-DuckDB
 * target, but their rejections happen before any connection prepares a statement.
 *
 * That "before any statement" property is asserted rather than assumed, with [DuckFile.attempts].
 * A guard that fired late would still fail the run, so the outcome alone cannot tell an early
 * rejection from a late one - and for the REQUIRED case the difference is the whole point, because
 * a late guard means rows were already appended.
 */
class TaskEngineGuardTest {

    @TempDir
    lateinit var root: Path

    /**
     * The failure a rejected task carried, whether it arrived as a `TaskOutcome.failure`, as a
     * throw from `run`, or from the definition refusing to be constructed at all. Which of the
     * three a guard uses is the engineer's choice; that it refuses is the contract.
     */
    private fun rejection(block: () -> TaskOutcome): Throwable =
        runCatching(block).let { it.exceptionOrNull() ?: it.getOrNull()?.failure }
            ?: error("expected the task to be rejected, but it succeeded")

    /**
     * Validation rule 11 and spec 4.4: `target.sql` is not available on a DuckDB datasource,
     * because DuckDB writes go through the appender, which takes a table and not a statement.
     */
    @Test
    fun aStatementTargetOnScratchIsRejected() {
        TaskHarness(root).use { harness ->
            val mes = harness.datasource("oracle_mes")
            mes.createSourceTable("wip", rows = 3, marker = "w")

            val failure = rejection {
                harness.run(
                    Etl.task(
                        "wip-statement-target",
                        Etl.phase(
                            "publish",
                            Etl.pipeToStatement(
                                name = "load-wip",
                                sourceDatasource = "oracle_mes",
                                sql = "select lot_id, lot_code from wip",
                                targetDatasource = Etl.SCRATCH,
                                targetSql = "merge into wip_summary using (select :lot_id as lot_id) s on (1 = 1)",
                            ),
                        ),
                    ),
                )
            }

            assertThat(failure).hasMessageContaining("load-wip")
            assertThat(mes.attempts.get())
                .describedAs("rejected before the source query ran, so no row was ever read")
                .isZero()
        }
    }

    /**
     * A scratch target with `createTable: REQUIRED` cannot be retried.
     *
     * `retries` defaults to 3 for any scratch target, but only AUTO gets the attempt-suffixed name
     * and the stable view of spec 5.5. A REQUIRED target writes into one fixed table, so a
     * transient failure part way through leaves whatever the failed attempt already flushed -
     * between zero and one chunk, spec 12 - and the retry appends the whole source on top of it.
     * Silent duplication, behind a run that reports SUCCEEDED.
     *
     * The rejection has to name the way out, or an author who hits it at 03:00 has a refusal and
     * no remedy.
     */
    @Test
    fun aRetriedScratchRequiredPipeIsRejected() {
        TaskHarness(root).use { harness ->
            val mes = harness.datasource("oracle_mes")
            mes.createSourceTable("wip", rows = 3, marker = "w")

            val failure = rejection {
                harness.run(
                    Etl.task(
                        "wip-required-retry",
                        Etl.phase(
                            "extract",
                            Etl.pipe(
                                name = "load-wip",
                                sourceDatasource = "oracle_mes",
                                sql = "select lot_id, lot_code from wip",
                                table = "wip_stg",
                                createTable = CreateTable.REQUIRED,
                                retries = 3,
                            ),
                        ),
                    ),
                )
            }

            assertThat(failure).hasMessageContaining("load-wip")
            assertThat(failure.message)
                .describedAs("the diagnostic points at the remedy, not only at the refusal")
                .contains("AUTO")
            assertThat(mes.attempts.get())
                .describedAs("rejected before any row was appended - a late guard would be too late")
                .isZero()
        }
    }

    /**
     * Spec 9.1: `transform.addColumns` declares the columns the transform *adds*, so declaring one
     * the source query already produces is an author error. Left unchecked it hands the target
     * writer the same column twice.
     *
     * Declared in upper case against a lower-case source column, because Row keys are normalised
     * to lower case on read (spec 4.5) and a clash check comparing raw names would miss this.
     */
    @Test
    fun addColumnsDeclaringAColumnTheSourceAlreadyProducesIsRejected() {
        TaskHarness(root).use { harness ->
            val mes = harness.datasource("oracle_mes")
            mes.createSourceTable("wip", rows = 3, marker = "w")

            val failure = rejection {
                harness.run(
                    Etl.task(
                        "wip-clash",
                        Etl.phase(
                            "extract",
                            Etl.pipe(
                                name = "load-wip",
                                sourceDatasource = "oracle_mes",
                                sql = "select lot_id, lot_code from wip",
                                table = "wip_stg",
                                transform = RowTransform { row -> row },
                                addColumns = listOf(ColumnMeta("LOT_CODE", CanonicalType.STRING, nullable = true)),
                            ),
                        ),
                    ),
                )
            }

            assertThat(failure).hasMessageContaining("load-wip")
            assertThat(failure.message)
                .describedAs("the diagnostic names the clashing column, lower cased as Row keys are")
                .contains("lot_code")
        }
    }

    /** Spec 6.2: one step may not export the same name twice. */
    @Test
    fun twoExportVarsWithTheSameNameInOneStepAreRejected() {
        TaskHarness(root).use { harness ->
            harness.datasource("report_oracle")

            val failure = rejection {
                harness.run(
                    Etl.task(
                        "wip-duplicate-export",
                        Etl.phase(
                            "extract",
                            Etl.export(
                                "read-site",
                                "report_oracle",
                                "siteCode" to "select 'F12'",
                                "siteCode" to "select 'F13'",
                            ),
                        ),
                    ),
                )
            }

            assertThat(failure).hasMessageContaining("siteCode")
        }
    }

    /**
     * Spec 6.1's `attempt` is a built-in whose value changes per attempt, so it is bound from the
     * retry loop rather than from the scope. An export defining it would be shadowed silently.
     */
    @Test
    fun theAttemptBuiltInCannotBeExported() {
        TaskHarness(root).use { harness ->
            harness.datasource("report_oracle")

            val failure = rejection {
                harness.run(
                    Etl.task(
                        "wip-attempt-export",
                        Etl.phase("extract", Etl.export("read-attempt", "report_oracle", "attempt" to "select 1")),
                    ),
                )
            }

            assertThat(failure).hasMessageContaining("attempt")
        }
    }

    /** Spec 7.1: `scratch` is reserved and cannot also name a configured datasource. */
    @Test
    fun scratchCannotAlsoBeAConfiguredDatasource() {
        TaskHarness(root).use { harness ->
            val impostor = harness.datasource("report_oracle")
            harness.register(Etl.SCRATCH, Jdbi.create(impostor))

            val failure = rejection {
                harness.run(
                    Etl.task(
                        "wip-reserved",
                        Etl.phase("publish", Etl.sql("publish", "report_oracle", "select 1")),
                    ),
                )
            }

            assertThat(failure).hasMessageContaining(Etl.SCRATCH)
        }
    }

    companion object {

        /**
         * Guards that reject a step without reaching a database. Each fragment is a word only the
         * intended diagnostic contains, so a step that failed for an unrelated reason cannot pass.
         */
        @JvmStatic
        fun rejectedSteps(): List<Arguments> = listOf(
            Arguments.of("unknown datasource", "nope", Etl.sql("publish", "nope", "select 1")),
            Arguments.of(
                "negative retries",
                "negative",
                Etl.sql("publish", "report_oracle", "select 1", retries = -1),
            ),
            Arguments.of(
                "positional parameter",
                "positional",
                Etl.sql("publish", "report_oracle", "create or replace table published as select ? as site"),
            ),
        )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rejectedSteps")
    fun aDefinitionLevelGuardRejectsTheStep(label: String, fragment: String, step: Step) {
        TaskHarness(root).use { harness ->
            val report = harness.datasource("report_oracle")

            val failure = rejection { harness.run(Etl.task("wip-guard", Etl.phase("publish", step))) }

            assertThat(failure).describedAs(label).hasMessageContaining(fragment)
            assertThat(report.tableExists("published"))
                .describedAs("a rejected step must not half-run")
                .isFalse()
        }
    }
}
