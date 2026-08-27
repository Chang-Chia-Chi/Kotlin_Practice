package infra.etl

import infra.etl.duckdb.CreateTable
import infra.etl.duckdb.DuckDbTableWriter
import infra.etl.pipe.JdbcSource
import infra.etl.pipe.PipeResult
import infra.etl.pipe.RowPipe
import infra.etl.pipe.RowTransform
import java.sql.Connection
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * Done-when item 3: the commit happens once per chunk, verified by counting rows visible from a
 * second connection *while the pipe is still running*.
 *
 * For a DuckDB target the appender's `flush()` is that commit (spec 4.6): measured on the pinned
 * 1.1.3, appended rows are invisible even to the appending connection until flush, and visible
 * to a `duplicate()` connection immediately after it. `autoCommit` is true by default, so there
 * is no `commit()` to observe and nothing else to look at.
 *
 * The observer is a `duplicate()` of the target connection, which shares the instance (spec 7.2),
 * and it is read from inside a [RowTransform] - which runs on the pipe's own thread, once per
 * row, at a point the test controls. One thread throughout: no sleeps, no second thread, and no
 * DuckDB connection used from two threads at once.
 */
class RowPipeCommitTest {

    private val sourceDb = Pipe.openDuck()
    private val targetDb = Pipe.openDuck()
    private val observer: Connection = targetDb.duplicate()
    private val jdbi: Jdbi = Jdbi.create { sourceDb.duplicate() }

    @AfterEach
    fun closeConnections() {
        observer.close()
        sourceDb.close()
        targetDb.close()
    }

    /**
     * 175 rows at a chunk size of 50. The transform records, for every row it sees, how many rows
     * the observer can already count in the target, so the whole visibility timeline is a list
     * rather than one sampled reading.
     *
     * The expected timeline is exact: while row `k` is being transformed, `floor((k - 1) / 50)`
     * chunks have been written and flushed, so the observer sees that many times 50. Rows 1 to 50
     * see nothing, rows 51 to 100 see 50, and so on. Two failures this catches that a single
     * mid-run reading would not:
     *
     * - a pipe that flushes only at `close()` leaves the timeline all zeros;
     * - a pipe that flushes per row, or per some chunk size of its own, moves every boundary.
     */
    @Test
    fun `each chunk becomes visible to a second connection at its chunk boundary`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 175)
        val visibleAtRow = ArrayList<Long>()

        val result = RowPipe(
            source = JdbcSource(jdbi, "select lot_id, lot_code, qty, site from wip_src order by lot_id"),
            target = DuckDbTableWriter(targetDb, "wip_stg", CreateTable.AUTO, Pipe.STEP),
            step = Pipe.STEP,
            chunkSize = 50,
            transform = RowTransform { row ->
                visibleAtRow.add(Pipe.rowCount(observer, "wip_stg"))
                row
            },
        ).run()

        assertThat(result).isEqualTo(PipeResult(175L, 175L))
        assertThat(visibleAtRow).hasSize(175)
        assertThat(visibleAtRow)
            .describedAs("rows visible to the second connection while row k was in the transform")
            .isEqualTo(List(175) { k -> (k / 50) * 50L })
        assertThat(Pipe.rowCount(observer, "wip_stg"))
            .describedAs("the final chunk is visible once the run returns")
            .isEqualTo(175L)
    }

    /**
     * The same observation on the chunk-size axis: a chunk size of 175 makes the whole run one
     * chunk, and then nothing at all may be visible mid-run. This is the control for the test
     * above - it is what proves the timeline there tracks the chunk boundary rather than merely
     * tracking progress through the source.
     */
    @Test
    fun `a single chunk becomes visible only when that chunk is written`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 175)
        val visibleAtRow = ArrayList<Long>()

        RowPipe(
            source = JdbcSource(jdbi, "select lot_id, lot_code, qty, site from wip_src order by lot_id"),
            target = DuckDbTableWriter(targetDb, "wip_stg", CreateTable.AUTO, Pipe.STEP),
            step = Pipe.STEP,
            chunkSize = 175,
            transform = RowTransform { row ->
                visibleAtRow.add(Pipe.rowCount(observer, "wip_stg"))
                row
            },
        ).run()

        assertThat(visibleAtRow).hasSize(175)
        assertThat(visibleAtRow).describedAs("one chunk, so nothing is visible until it is written")
            .containsOnly(0L)
        assertThat(Pipe.rowCount(observer, "wip_stg")).isEqualTo(175L)
    }
}
