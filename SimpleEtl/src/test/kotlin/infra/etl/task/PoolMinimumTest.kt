package infra.etl.task

import infra.etl.Etl
import infra.etl.duckdb.CreateTable
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll

/**
 * Review finding H4, the half the framework can answer: which tasks run a pipe whose source and
 * target are the *same* datasource, and therefore hold two of that pool's connections at once.
 *
 * Two runs that each hold one such connection and wait for a second are in a circular wait, and
 * no acquisition order can break it because both come from one pool. Undersized, both runs hang
 * with `busy = true` and every later firing of either task is skipped as AlreadyRunning. Spec 7.1
 * now states the minimum; this is the arithmetic behind the number logged at every startup and
 * reload.
 *
 * The configured pool size is deliberately not read. `Jdbi` exposes neither its `ConnectionFactory`
 * nor its `DataSource` - verified against jdbi3-core 3.45.4 - so the only way to it is reflection
 * into a third party's private fields.
 */
class PoolMinimumTest {

    private fun sameDatasourcePipe(name: String, datasource: String) = PipeStep(
        name = name,
        source = PipeSource(datasource, "select lot_id from wip"),
        target = TableTarget(datasource, "wip_tgt", CreateTable.REQUIRED),
    )

    private fun crossDatasourcePipe(name: String) = PipeStep(
        name = name,
        source = PipeSource("oracle_mes", "select lot_id from wip"),
        target = TableTarget("report_oracle", "wip_tgt", CreateTable.REQUIRED),
    )

    private fun task(name: String, vararg steps: Step) =
        Etl.task(name = name, phases = arrayOf(Etl.phase("only", *steps)))

    @Test
    fun `only a pipe whose source and target are one datasource counts`() {
        val tasks = listOf(
            task("same", sameDatasourcePipe("load", "oracle_mes")),
            task("crossing", crossDatasourcePipe("load")),
            task("into-scratch", Etl.pipe("load", "oracle_mes", "select lot_id from wip", "wip_stg")),
        )

        assertEquals(mapOf("oracle_mes" to listOf("same")), sameDatasourcePipeUsers(tasks))
    }

    /**
     * Scratch is excluded even though its source and target names match, because it is not a pool
     * at all: a scratch read takes a `ScratchDb.duplicate()` and a scratch write the single write
     * connection (spec 7.2). Counting it would report a requirement against a datasource no
     * operator can size.
     */
    @Test
    fun `a scratch-to-scratch pipe is not a pool user`() {
        val tasks = listOf(task("in-scratch", sameDatasourcePipe("shuffle", SCRATCH)))

        assertEquals(emptyMap<String, List<String>>(), sameDatasourcePipeUsers(tasks))
    }

    /**
     * The multiplier is tasks, not steps. `TaskRunner` admits one run per task at a time (spec
     * 8.4), so a task's two same-datasource pipes cannot overlap and must not double the
     * requirement - while two *tasks* genuinely can, and do.
     */
    @Test
    fun `a task counts once per datasource however many such steps it has`() {
        val busy = task(
            "busy",
            sameDatasourcePipe("first", "oracle_mes"),
            sameDatasourcePipe("second", "oracle_mes"),
            sameDatasourcePipe("third", "report_oracle"),
        )
        val other = task("other", sameDatasourcePipe("only", "oracle_mes"))

        val users = sameDatasourcePipeUsers(listOf(busy, other))

        assertAll(
            { assertEquals(listOf("busy", "other"), users["oracle_mes"]) { "two tasks, so a pool of 4" } },
            { assertEquals(listOf("busy"), users["report_oracle"]) { "one task, so a pool of 2" } },
        )
    }

    /** A pipe to a statement target takes its second connection the same way a table target does. */
    @Test
    fun `a statement target counts too`() {
        val tasks = listOf(
            task(
                "merging",
                Etl.pipeToStatement(
                    name = "publish",
                    sourceDatasource = "report_oracle",
                    sql = "select lot_id from wip",
                    targetDatasource = "report_oracle",
                    targetSql = "merge into wip_summary t using (select :lot_id as lot_id from dual) s " +
                        "on (t.lot_id = s.lot_id) when matched then update set t.lot_id = s.lot_id",
                ),
            ),
        )

        assertEquals(mapOf("report_oracle" to listOf("merging")), sameDatasourcePipeUsers(tasks))
    }
}
