package infra.etl

import infra.etl.pipe.RowTransform
import infra.etl.task.LoadResult
import infra.etl.task.TaskDefinition
import infra.etl.task.TaskFileLoader
import infra.etl.task.ValidationError
import java.nio.file.Path
import kotlin.io.path.writeText
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.assertAll

/**
 * P6 test support: task files on disk, the one-rule-away editing helpers, and the two
 * reconciliation seams.
 *
 * **This file is the phase's reconciliation seam.** The engineer built `TaskFileLoader` in
 * parallel with these tests and neither side saw the other, so every assumption about a shape
 * spec 11.2 does not actually pin is funnelled through this file and marked `INTEGRATE:`. No
 * test class names a production constructor or unwraps a load result, so a shape that came out
 * differently is a small edit here rather than a rewrite of four test classes.
 *
 * ### The two unsettled shapes
 *
 * 1. **What `load` returns.** Spec 11.2 declares
 *    `Result<List<TaskDefinition>, ValidationReport>`, which cannot exist: `kotlin.Result` takes
 *    one type parameter. [load] below is the only function that assumes a replacement.
 * 2. **How the loader learns what exists.** Rules 3, 4 and 5 check names against things that
 *    live outside the task file - configured Jdbi beans, `RowTransform` CDI beans, and the
 *    `TaskHookRegistry`. Spec 11.2 shows `TaskFileLoader` with no constructor at all, so
 *    [newLoader] is the only function that assumes one.
 *
 * ### How a broken file is built
 *
 * Every broken file is one of [VALID], [VALID_REQUIRED] or [minimal] with a single edit applied
 * by [edit] or [dropLine], both of which fail loudly when the anchor does not appear exactly
 * once. That matters more than it looks: a `replace` whose anchor has drifted silently yields
 * the *valid* text, and a test that then asserts rejection fails for a reason that has nothing
 * to do with the loader. The pairing with the canaries in `TaskFileLoaderValidTest` is what
 * makes the eighteen rule tests mean anything - without a valid sibling that loads, every one
 * of them would pass against a loader that rejects every file it is handed.
 *
 * Nothing here creates a temporary table (spec 7.2, and P4's `NoTempTableTest` scans this file
 * like any other), and no fixture DELETEs, TRUNCATEs or DROPs anything: the only SQL in these
 * files is text that a loader parses and never executes.
 */
object TaskFiles {

    // -----------------------------------------------------------------------------------
    // What the surrounding application is assumed to provide (validation rules 3, 4, 5).
    // `scratch` is deliberately absent: spec 7.1 reserves it, and it is not a Jdbi bean.
    // -----------------------------------------------------------------------------------

    val DATASOURCES: Set<String> = setOf("oracle_mes", "report_oracle", "other_oracle")

    val WIP_ENRICHER: RowTransform = RowTransform { it }

    val TRANSFORMS: Map<String, RowTransform> = mapOf("wipEnricher" to WIP_ENRICHER)

    val HOOKS: Set<String> = setOf("notify-downstream", "page-oncall")

    /**
     * What one call to `TaskFileLoader.load` produced, in a shape the tests can assert on.
     *
     * [tasks] is null exactly when the load was rejected, which is the distinction the
     * done-when item "one bad file out of ten prevents startup" turns on.
     */
    data class Loaded(val tasks: List<TaskDefinition>?, val errors: List<ValidationError>) {

        /** The tasks, or a failure that prints the report rather than an opaque NPE. */
        fun tasksOrFail(): List<TaskDefinition> {
            assertNotNull(tasks) { "expected a clean load, got ${errors.joinToString("\n")}" }
            return tasks!!
        }

        fun single(): TaskDefinition = tasksOrFail().single()
    }

    // INTEGRATE: the only place naming TaskFileLoader's constructor. If the engineer resolved
    // datasources / transforms / hooks through some other channel - a resolver interface, CDI,
    // a builder - change this one expression.
    private fun newLoader(): TaskFileLoader = TaskFileLoader(DATASOURCES, TRANSFORMS, HOOKS)

    /** INTEGRATE: the other place naming the constructor, for the one test that varies it. */
    fun loaderWithDatasources(datasources: Set<String>): TaskFileLoader = TaskFileLoader(datasources)

    /**
     * INTEGRATE: the only place that assumes what `load` returns.
     *
     * The tests were written against `kotlin.Result<List<TaskDefinition>>` with the report on a
     * failure; the engineer chose the sealed pair `LoadResult.Loaded` / `LoadResult.Invalid`
     * instead. Reconciling the two was this one function, exactly as intended - no test class
     * was touched.
     */
    fun load(directory: Path): Loaded =
        when (val result = newLoader().load(directory)) {
            is LoadResult.Loaded -> Loaded(result.tasks, emptyList())
            is LoadResult.Invalid -> Loaded(null, result.report.errors)
        }

    // -----------------------------------------------------------------------------------
    // Files on disk. INTEGRATE: the file extension the loader scans for is an assumption too.
    // -----------------------------------------------------------------------------------

    fun dirOf(root: Path, vararg files: Pair<String, String>): Path {
        files.forEach { (name, text) -> root.resolve(name).writeText(text) }
        return root
    }

    fun loadOne(root: Path, yaml: String): Loaded = load(dirOf(root, "task.yaml" to yaml))

    // -----------------------------------------------------------------------------------
    // One-rule-away editing.
    // -----------------------------------------------------------------------------------

    /** Replaces [from] with [to], insisting that [from] occurs exactly once. */
    fun edit(base: String, from: String, to: String): String {
        val hits = base.split(from).size - 1
        check(hits == 1) { "edit anchor occurs $hits times, not once:\n<$from>" }
        return base.replace(from, to)
    }

    /**
     * [VALID] with its one `transform.addColumns` entry restated. That entry is the only column
     * type a task file *states* rather than discovers, which after the rule 15 ruling is exactly
     * the half of the rule startup can reach (spec 10 rule 15, 4.6).
     *
     * `nullable` is always written out. It defaults to true, and a test that relied on the
     * default could not tell "BOOLEAN is rejected because it is nullable" from "BOOLEAN is
     * rejected on sight" - which is the difference the acceptance cases exist to pin.
     */
    fun withAddColumn(type: String, nullable: Boolean, precision: Int? = null): String = edit(
        VALID,
        "            - name: row_hash\n              type: VARCHAR\n",
        "            - name: row_hash\n              type: $type\n              nullable: $nullable\n" +
            if (precision == null) "" else "              precision: $precision\n",
    )

    /** Drops the single line containing [needle], insisting there is exactly one. */
    fun dropLine(base: String, needle: String): String {
        val lines = base.lines()
        val hits = lines.count { it.contains(needle) }
        check(hits == 1) { "line anchor occurs $hits times, not once: <$needle>" }
        return lines.filterNot { it.contains(needle) }.joinToString("\n")
    }

    // -----------------------------------------------------------------------------------
    // Assertions.
    // -----------------------------------------------------------------------------------

    /**
     * The shared shape of an eighteen-rule test: the load was rejected, every error names the
     * file it came from, and at least one error carries [step] and a message naming [mentions].
     *
     * [step] is asserted on the structured field rather than searched for in the message text.
     * `ValidationError` carries a `step` field precisely so that a caller can group by it, and
     * an assertion satisfied by the step name merely appearing somewhere in prose would also be
     * satisfied by a loader that never populates the field. Task-level rules pass null, which
     * is the whole reason the field is nullable.
     *
     * `line` is checked only for plausibility. It is nullable by contract because not every
     * rule can cheaply produce one, so asserting a number - or even non-null - would be testing
     * a guess about the implementation rather than the contract.
     */
    fun assertRejects(loaded: Loaded, file: String, step: String?, vararg mentions: String) {
        assertAll(
            {
                assertNull(loaded.tasks) {
                    "this file is one rule away from a sibling that loads, so it must be rejected"
                }
            },
            {
                assertTrue(loaded.errors.isNotEmpty()) {
                    "a rejection with no errors is not a report; errors were ${loaded.errors}"
                }
            },
        )

        assertAll(
            loaded.errors.map { error ->
                {
                    assertTrue(error.file.contains(file)) {
                        "every error must identify the file it came from; file was '${error.file}'"
                    }
                    val line = error.line
                    assertTrue(line == null || line > 0) { "line $line is not a YAML line number" }
                }
            },
        )

        val matching = loaded.errors.filter { it.step == step }
        assertTrue(matching.isNotEmpty()) {
            "no error carries step=$step; the report carried steps ${loaded.errors.map { it.step }} " +
                "and messages ${loaded.errors.map { it.message }}"
        }
        assertTrue(matching.any { error -> mentions.all { error.message.contains(it, ignoreCase = true) } }) {
            "no error with step=$step mentioned all of ${mentions.toList()} (ignoring case); " +
                "messages were ${matching.map { it.message }}"
        }
    }

    // -----------------------------------------------------------------------------------
    // The baselines. Each one is asserted to load in TaskFileLoaderValidTest.
    // -----------------------------------------------------------------------------------

    /**
     * The main baseline: every field of spec 3.1 to 3.5 that P5's model can carry, arranged so
     * that most of the eighteen rules of spec 10 are one edit away from firing.
     *
     * `description` is deliberately absent even though spec 3.1 lists it: `TaskDefinition` has
     * no field for it, so accepting it is a judgement call of its own and it gets its own
     * isolated test. Putting it here would make every other test in the phase depend on that
     * call going one particular way.
     */
    val VALID: String = """
        name: wip-summary
        enabled: true
        schedule:
          cron: "0 */10 * * * ?"
        logging: true
        chunkSize: 5000
        scratch:
          memoryLimitMb: 4096
        onSuccess: notify-downstream
        onFailure: page-oncall
        vars:
          - name: siteCode
            value: "F12"
        phases:
          - name: extract
            steps:
              - name: read-watermark
                type: export
                datasource: oracle_mes
                vars:
                  - name: lastTs
                    sql: "select max(processed_ts) from etl_watermark where task_name = :taskName"
              - name: load-wip
                type: pipe
                chunkSize: 20000
                source:
                  datasource: oracle_mes
                  sql: "select lot_id, qty, upd_ts from wip where (:lastTs is null or upd_ts > :lastTs) and site = :siteCode"
                transform:
                  bean: wipEnricher
                  addColumns:
                    - name: row_hash
                      type: VARCHAR
                target:
                  datasource: scratch
                  table: wip_stg
                  createTable: AUTO
                retries: 3
          - name: build
            steps:
              - name: build-summary
                type: materialize
                datasource: scratch
                output: summary
                format: TABLE
                sql: "select lot_id, sum(qty) as qty from wip_stg group by 1"
                retries: 3
              - name: index-staging
                type: sql
                datasource: scratch
                statements:
                  - "create index idx_wip_lot on wip_stg (lot_id)"
                retries: 3
          - name: publish
            steps:
              - name: publish-summary
                type: pipe
                source:
                  datasource: scratch
                  sql: "select lot_id, qty from summary"
                target:
                  datasource: report_oracle
                  sql: "merge into wip_summary t using (select :lot_id as lot_id, :qty as qty from dual) s on (t.lot_id = s.lot_id) when matched then update set t.qty = s.qty when not matched then insert (lot_id, qty) values (s.lot_id, s.qty)"
                  idempotent: true
                retries: 2
    """.trimIndent()

    /**
     * The second baseline, for rules 15 and 18: an author-owned scratch table created by a
     * `sql` step and filled under `createTable: REQUIRED`.
     *
     * `retries: 0` is stated rather than omitted because it has to be. Spec 5.3 defaults
     * `retries` to 3 for any scratch target, REQUIRED included, so an omitted value is itself
     * the rule 18 violation - which is what
     * `rule18OmittedRetriesOnARequiredScratchTargetIsStillRejected` exists to pin.
     */
    val VALID_REQUIRED: String = """
        name: staged-required
        phases:
          - name: stage
            steps:
              - name: create-target
                type: sql
                datasource: scratch
                statements:
                  - "create table wip_req (id BIGINT, note VARCHAR)"
                retries: 3
              - name: load-required
                type: pipe
                source:
                  datasource: oracle_mes
                  sql: "select lot_id as id, note from wip"
                target:
                  datasource: scratch
                  table: wip_req
                  createTable: REQUIRED
                retries: 0
    """.trimIndent()

    /** The smallest file that violates nothing. Used to fill a directory. */
    fun minimal(name: String): String = """
        name: $name
        phases:
          - name: only
            steps:
              - name: touch
                type: sql
                datasource: scratch
                statements:
                  - "select 1"
    """.trimIndent()

    /**
     * Rule 7's ordering clause, as a pair of files differing only in the order of two steps.
     *
     * Built by concatenation rather than by editing text: "the export moved after its use" is a
     * structural change, and an anchored replace expressing it would be two edits whose
     * combined effect a reader has to reconstruct.
     */
    fun orderedVars(exportFirst: Boolean): String {
        val header = listOf(
            "name: ordered-vars",
            "phases:",
            "  - name: extract",
            "    steps:",
        ).joinToString("\n", postfix = "\n")
        val exportStep = listOf(
            "      - name: read-watermark",
            "        type: export",
            "        datasource: oracle_mes",
            "        vars:",
            "          - name: lastTs",
            "            sql: \"select max(processed_ts) from etl_watermark\"",
        ).joinToString("\n", postfix = "\n")
        val useStep = listOf(
            "      - name: use-watermark",
            "        type: materialize",
            "        datasource: scratch",
            "        output: recent",
            "        sql: \"select 1 as a where 1 = 1 or :lastTs is null\"",
        ).joinToString("\n", postfix = "\n")
        return header + if (exportFirst) exportStep + useStep else useStep + exportStep
    }
}
