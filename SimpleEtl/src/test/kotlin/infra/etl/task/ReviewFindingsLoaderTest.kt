package infra.etl.task

import infra.etl.TaskFiles
import infra.etl.TaskFiles.VALID
import infra.etl.TaskFiles.VALID_CACHE_COPY
import infra.etl.TaskFiles.assertRejects
import infra.etl.TaskFiles.edit
import infra.etl.TaskFiles.loadOne
import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * The review findings that spec 10's rules either missed or over-reached on. Every file here is
 * one edit away from a sibling that loads, which is P6's pairing rule: standing alone, "this file
 * was rejected" is satisfied by a loader that rejects everything.
 *
 * - **M7**: `phases` carries no `# optional` annotation in spec 3.1, but the DTO defaulted it to
 *   the empty list and no rule rejected one. A task with no step scheduled, ran, and reported
 *   SUCCEEDED every ten minutes while its table quietly stopped updating.
 * - **M9**: `chunkSize` and `retries` are checked at boot precisely so a bad value is not a
 *   failure five minutes into a run. `scratch.memoryLimitMb` was not, and `ScratchDb`'s own
 *   `require` then fired at the first line of every run, forever.
 * - **M8**: rule 15 is about what the DuckDB 1.1.3 appender can express, and it was applied to
 *   every `transform.addColumns` entry whatever the target. A nullable DOUBLE on a REQUIRED Oracle
 *   target - which `JdbcWriters.javaType` binds without complaint - was inexpressible: undeclared
 *   it was dropped silently (see `ReviewFindingsEngineTest`), declared it failed startup.
 * - **N1**: a `cacheCopy`'s SQL is spliced into `CREATE TABLE <output> AS <sql>`, so it must be a
 *   SELECT. `json_serialize_sql` answers `not implemented` for a parsed non-SELECT and rule 6
 *   discarded that answer, so a `copy (...) to ...` loaded clean and failed on every firing.
 * - **N2**: JDBI's lexer reads a colon followed by digits as a parameter name, so DuckDB's own
 *   `[1:3]` slice arrived at rule 19 as the binding `:3` - and at rule 6 as the rewritten
 *   `[1?]`, a syntax error the author never wrote.
 */
class ReviewFindingsLoaderTest {

    @TempDir
    lateinit var root: Path

    private fun dir(name: String): Path = Files.createDirectories(root.resolve(name))

    // --- M7: a task that would do nothing ----------------------------------------------------

    @Test
    fun aTaskWithNoPhasesIsRejectedRatherThanSucceedingEveryTenMinutesHavingDoneNothing() {
        val yaml = """
            name: does-nothing
            schedule:
              cron: "0 */10 * * * ?"
        """.trimIndent()

        assertRejects(loadOne(dir("no-phases"), yaml), file = "task.yaml", step = null, "phases")
    }

    @Test
    fun aPhaseWithNoStepsIsRejected() {
        val yaml = """
            name: empty-phase
            phases:
              - name: extract
                steps: []
        """.trimIndent()

        assertRejects(loadOne(dir("no-steps"), yaml), file = "task.yaml", step = null, "extract")
    }

    // --- M9: a scratch memory limit that fails at run start ------------------------------------

    @Test
    fun aNonPositiveScratchMemoryLimitIsRejectedAtBootRatherThanAtTheFirstLineOfEveryRun() {
        val rejected = loadOne(dir("zero-limit"), edit(VALID, "memoryLimitMb: 4096", "memoryLimitMb: 0"))

        assertRejects(rejected, file = "task.yaml", step = null, "memoryLimitMb")
    }

    // --- M8: rule 15 belongs to DuckDB targets ------------------------------------------------

    /**
     * The same file twice, differing only in the target datasource. The scratch half is what keeps
     * the rule alive: a fix that simply deleted rule 15 would pass the Oracle half alone.
     */
    @Test
    fun aNullableDoubleAddedColumnLoadsForAnOracleTargetAndIsStillRejectedForAScratchOne() {
        val oracle = addedDouble(datasource = "report_oracle", table = "wip_summary")
        val scratch = addedDouble(datasource = "scratch", table = "wip_stg")

        val loaded = loadOne(dir("oracle-target"), oracle)

        assertEquals(
            "ratio",
            (loaded.single().phases.single().steps.single() as PipeStep).addColumns.single().name,
        ) { "errors were ${loaded.errors.map { it.message }}" }
        assertRejects(loadOne(dir("scratch-target"), scratch), file = "task.yaml", step = "load-report", "DOUBLE")
    }

    private fun addedDouble(datasource: String, table: String): String = """
        name: added-double
        phases:
          - name: publish
            steps:
              - name: load-report
                type: pipe
                source:
                  datasource: oracle_mes
                  sql: "select lot_id, qty from wip"
                transform:
                  bean: wipEnricher
                  addColumns:
                    - name: ratio
                      type: DOUBLE
                      nullable: true
                target:
                  datasource: $datasource
                  table: $table
                  createTable: REQUIRED
                retries: 0
    """.trimIndent()

    // --- N1 and N2: the cacheCopy SQL ---------------------------------------------------------

    private val cacheSelect = "sql: \"select lot_id, qty from wip where site = 'F12'\""

    private fun cacheStepOf(loaded: TaskFiles.Loaded): CacheCopyStep =
        loaded.single().phases.first().steps.single() as CacheCopyStep

    @Test
    fun aCacheCopyStatementThatParsesButIsNotASelectIsRejected() {
        val copyTo = edit(VALID_CACHE_COPY, cacheSelect, "sql: \"copy (select lot_id from wip) to 'wip.parquet'\"")

        assertRejects(loadOne(dir("not-select"), copyTo), file = "task.yaml", step = "copy-wip", "SELECT")
    }

    /**
     * An array slice and a struct literal, both ordinary DuckDB, both carrying a colon that JDBI
     * lexes as a parameter name. They reach the cache verbatim, so rejecting them would refuse SQL
     * the cache runs perfectly well - while `:siteCode` in the same position must still be
     * rejected, which `CacheCopyLoaderTest` pins.
     */
    @Test
    fun aDuckDbSliceAndStructLiteralAreNotVariablesAndReachTheCacheVerbatim() {
        val sliced = "select site_code[1:3] as prefix, {'k':1} as tag from wip"
        val loaded = loadOne(dir("sliced"), edit(VALID_CACHE_COPY, cacheSelect, "sql: \"$sliced\""))

        assertEquals(sliced, cacheStepOf(loaded).sql) { "errors were ${loaded.errors.map { it.message }}" }
    }
}
