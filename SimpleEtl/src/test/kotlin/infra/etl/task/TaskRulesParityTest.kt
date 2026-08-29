package infra.etl.task

import infra.etl.Etl
import infra.etl.TaskFiles
import infra.etl.TaskHarness
import infra.etl.duckdb.CreateTable
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

/**
 * E10's whole claim, asserted: the task-shaped rules of spec 10 have **one** implementation,
 * and a task file and a definition built in code reach it and come back with the same sentence.
 *
 * Each case below is one rule, broken twice - once in YAML for `TaskFileLoader`, once in Kotlin for
 * `TaskEngine` - and the two diagnostics are compared **verbatim**, not by a shared fragment. That
 * is the discriminating form. Before E10 every one of these rules existed as two `require`s worded
 * independently, and both of those wordings named the rule, named the step and offered a remedy; a
 * test asserting "both mention rule 12" would have passed against the drift review findings M10 and
 * M11 were raised about. Only equality fails when one copy is edited and the other is not.
 *
 * The engine's message is the loader's with `step '<name>': ` in front, and that difference is the
 * design: `ValidationError` carries a structured `step` field a report groups by, and an exception
 * carries prose. So the rule itself never names the step, and each caller stamps it on its own way.
 *
 * **Both sides run the real thing.** The loader side goes through `TaskFileLoader.load` over a file
 * on disk; the engine side goes through `TaskEngine.run`, which is what proves the engine actually
 * calls the module rather than merely being able to. Every rule here rejects before a connection is
 * opened, so no case needs a database, a cache or a configured datasource to reach its verdict.
 */
class TaskRulesParityTest {

    @TempDir
    lateinit var root: Path

    /**
     * One rule, broken on both paths.
     *
     * @param step the step name, which both files use, so the two diagnostics can be compared after
     *   the engine's prefix is accounted for.
     * @param vars the task literals both sides declare. Rule 7 resolves `:name` against the built-ins
     *   plus these, so a case testing the *amended* rule 7 has to define the name it binds - or the
     *   plain rule fires first and the case proves nothing about the amendment.
     */
    class Case(
        val label: String,
        val step: String,
        val yaml: String,
        val built: Step,
        val vars: List<LiteralVar> = emptyList(),
    ) {
        override fun toString(): String = label
    }

    companion object {

        @JvmStatic
        fun cases(): List<Arguments> = listOf(
            // Rule 7: a ':name' no built-in, literal var or earlier export has defined.
            Case(
                label = "rule 7 - an undefined bind name",
                step = "build-summary",
                yaml = """
                    name: parity
                    phases:
                      - name: only
                        steps:
                          - name: build-summary
                            type: materialize
                            datasource: scratch
                            output: summary
                            sql: "select 1 as x where 1 = :nosuchvar"
                """.trimIndent(),
                built = Etl.materialize(
                    name = "build-summary",
                    output = "summary",
                    sql = "select 1 as x where 1 = :nosuchvar",
                ),
            ),
            // Rule 7 as spec 10 amends it (review finding H3): a non-scratch materialize binds
            // nothing at all, because Oracle rejects a bind variable in DDL with ORA-01027.
            Case(
                label = "rule 7 amended - a non-scratch materialize that binds",
                step = "build-summary",
                yaml = """
                    name: parity
                    vars:
                      - name: siteCode
                        value: "F12"
                    phases:
                      - name: only
                        steps:
                          - name: build-summary
                            type: materialize
                            datasource: report_oracle
                            output: summary
                            sql: "select lot_id from wip where site = :siteCode"
                """.trimIndent(),
                built = Etl.materialize(
                    name = "build-summary",
                    datasource = "report_oracle",
                    output = "summary",
                    sql = "select lot_id from wip where site = :siteCode",
                ),
                vars = listOf(Etl.literal("siteCode", "F12")),
            ),
            // Rule 8: one step exporting the same name twice.
            Case(
                label = "rule 8 - a variable defined twice",
                step = "read-site",
                yaml = """
                    name: parity
                    phases:
                      - name: only
                        steps:
                          - name: read-site
                            type: export
                            datasource: report_oracle
                            vars:
                              - name: siteCode
                                sql: "select 'F12'"
                              - name: siteCode
                                sql: "select 'F13'"
                """.trimIndent(),
                built = Etl.export(
                    "read-site", "report_oracle",
                    "siteCode" to "select 'F12'",
                    "siteCode" to "select 'F13'",
                ),
            ),
            // Rule 11: target.sql on the DuckDB working file.
            Case(
                label = "rule 11 - a statement target on scratch",
                step = "load-wip",
                yaml = """
                    name: parity
                    phases:
                      - name: only
                        steps:
                          - name: load-wip
                            type: pipe
                            source:
                              datasource: oracle_mes
                              sql: "select lot_id from wip"
                            target:
                              datasource: scratch
                              sql: "merge into wip_summary using (select 1) s on (1 = 1)"
                """.trimIndent(),
                built = Etl.pipeToStatement(
                    name = "load-wip",
                    sourceDatasource = "oracle_mes",
                    sql = "select lot_id from wip",
                    targetDatasource = SCRATCH,
                    targetSql = "merge into wip_summary using (select 1) s on (1 = 1)",
                ),
            ),
            // Rule 12 as spec 10 amends it (review finding H2): a retried sql step off scratch.
            Case(
                label = "rule 12 - a retried sql step off scratch",
                step = "bookkeeping",
                yaml = """
                    name: parity
                    phases:
                      - name: only
                        steps:
                          - name: bookkeeping
                            type: sql
                            datasource: report_oracle
                            statements:
                              - "update etl_watermark set processed_ts = sysdate"
                            retries: 2
                """.trimIndent(),
                built = Etl.sql(
                    "bookkeeping", "report_oracle",
                    "update etl_watermark set processed_ts = sysdate",
                    retries = 2,
                ),
            ),
            // Rule 13's scratch-only half: spec 5.6 puts the parquet file in the scratch directory.
            Case(
                label = "rule 13 - PARQUET off scratch",
                step = "build-summary",
                yaml = """
                    name: parity
                    phases:
                      - name: only
                        steps:
                          - name: build-summary
                            type: materialize
                            datasource: report_oracle
                            output: summary
                            format: PARQUET
                            sql: "select lot_id from wip"
                """.trimIndent(),
                built = Etl.materialize(
                    name = "build-summary",
                    datasource = "report_oracle",
                    output = "summary",
                    sql = "select lot_id from wip",
                    format = MaterializeFormat.PARQUET,
                ),
            ),
            // Rule 18: only createTable AUTO gets spec 5.5's attempt-suffixed name, so a retried
            // REQUIRED scratch target would append onto the failed attempt's flushed rows.
            Case(
                label = "rule 18 - a retried REQUIRED scratch target",
                step = "load-required",
                yaml = """
                    name: parity
                    phases:
                      - name: only
                        steps:
                          - name: load-required
                            type: pipe
                            source:
                              datasource: oracle_mes
                              sql: "select lot_id from wip"
                            target:
                              datasource: scratch
                              table: wip_req
                              createTable: REQUIRED
                            retries: 2
                """.trimIndent(),
                built = Etl.pipe(
                    name = "load-required",
                    sourceDatasource = "oracle_mes",
                    sql = "select lot_id from wip",
                    table = "wip_req",
                    createTable = CreateTable.REQUIRED,
                    retries = 2,
                ),
            ),
            // Rule 19: CopyOutSpec.sql is a plain string with no binding channel.
            Case(
                label = "rule 19 - a cacheCopy that binds",
                step = "copy-wip",
                yaml = """
                    name: parity
                    vars:
                      - name: siteCode
                        value: "F12"
                    phases:
                      - name: only
                        steps:
                          - name: copy-wip
                            type: cacheCopy
                            cache: wip_cache
                            output: wip_copy
                            sql: "select lot_id from wip where site = :siteCode"
                """.trimIndent(),
                built = Etl.cacheCopy(
                    "copy-wip", "wip_cache",
                    "select lot_id from wip where site = :siteCode",
                    "wip_copy",
                ),
                vars = listOf(Etl.literal("siteCode", "F12")),
            ),
            // Rule 6's positional half. The DuckDB syntax check is the loader's alone and is not
            // reached here: this text never parses as a bound statement to begin with.
            Case(
                label = "rule 6 - a positional parameter",
                step = "bookkeeping",
                yaml = """
                    name: parity
                    phases:
                      - name: only
                        steps:
                          - name: bookkeeping
                            type: sql
                            datasource: report_oracle
                            statements:
                              - "update etl_watermark set processed_ts = ?"
                """.trimIndent(),
                built = Etl.sql(
                    "bookkeeping", "report_oracle",
                    "update etl_watermark set processed_ts = ?",
                ),
            ),
            // Not one of the seven, but the same module owns it now: it is what makes `retries`
            // being nullable safe, and it was worded twice before E10 as well.
            Case(
                label = "spec 5.3 - negative retries",
                step = "bookkeeping",
                yaml = """
                    name: parity
                    phases:
                      - name: only
                        steps:
                          - name: bookkeeping
                            type: sql
                            datasource: report_oracle
                            statements:
                              - "select 1 from dual"
                            retries: -1
                """.trimIndent(),
                built = Etl.sql("bookkeeping", "report_oracle", "select 1 from dual", retries = -1),
            ),
        ).map { Arguments.of(it) }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    fun oneRuleReachesBothPathsWithTheSameWording(case: Case) {
        val directory = root.resolve("file").also { it.toFile().mkdirs() }
        val fromFile = TaskFiles.loadOneWithCaches(directory, case.yaml, TaskFiles.CACHES)
        val forStep = fromFile.errors.filter { it.step == case.step }

        val fromEngine = TaskHarness(root.resolve("engine")).use { harness ->
            harness.datasource("oracle_mes")
            harness.datasource("report_oracle")
            harness.run(Etl.task("parity", Etl.phase("only", case.built), vars = case.vars)).failure
        }

        assertAll(
            {
                assertTrue(forStep.isNotEmpty()) {
                    "the loader accepted the file; errors were ${fromFile.errors.map { it.message }}"
                }
            },
            {
                assertTrue(fromEngine is IllegalArgumentException) {
                    "the engine rejected the definition with $fromEngine, not an argument error"
                }
            },
            {
                assertEquals(
                    forStep.map { "step '${case.step}': ${it.message}" },
                    listOf(fromEngine?.message),
                ) {
                    "one rule, one sentence: the loader and the engine must not word it differently"
                }
            },
        )
    }

    /**
     * Spec 5.3's defaults, on the path that had no loader in front of it (spec 2.1): 3 for a step
     * writing into scratch, 0 for anywhere else, resolved from a stated `null`.
     *
     * No call site here names `defaultRetries`, which is the point. Until E10 `retries` was a
     * non-null `Int` with a constructor default, so "not stated" was unrepresentable and every
     * construction site - the loader's five branches, the model's own constructors, every test
     * builder - re-derived the value from the datasource itself (review findings M10, M11).
     */
    @Test
    fun aDefinitionBuiltInCodeThatOmitsRetriesTakesTheDatasourceDefault() {
        val rules = TaskRules()
        val scratchPipe = Etl.pipe("load", "oracle_mes", "select 1", table = "wip_stg")
        val externalSql = Etl.sql("publish", "report_oracle", "select 1")
        val scratchSql = Etl.sql("index", SCRATCH, "select 1")
        val cacheCopy = Etl.cacheCopy("copy", "wip_cache", "select 1", "wip_copy")

        assertAll(
            { assertEquals(3, rules.retries(scratchPipe)) { "a scratch target retries by default" } },
            { assertEquals(3, rules.retries(scratchSql)) { "so does a scratch sql step" } },
            { assertEquals(0, rules.retries(externalSql)) { "an external datasource does not" } },
            {
                assertEquals(0, rules.retries(cacheCopy)) {
                    "a cacheCopy resolves to 0 on both paths since E10, which is what retired rule 20's " +
                        "model-versus-YAML asymmetry"
                }
            },
            {
                val stated = listOf(scratchPipe, scratchSql, externalSql, cacheCopy).map { it.retries }
                assertEquals(listOf<Int?>(null, null, null, null), stated) {
                    "the model carries what the author stated, not what it resolves to"
                }
            },
        )
    }
}
