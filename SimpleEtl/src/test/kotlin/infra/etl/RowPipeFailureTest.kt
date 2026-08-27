package infra.etl

import infra.etl.duckdb.CreateTable
import infra.etl.duckdb.DuckDbTableWriter
import infra.etl.pipe.JdbcSource
import infra.etl.pipe.PipeResult
import infra.etl.pipe.RowPipe
import infra.etl.pipe.RowTransform
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * Done-when item 5: the source stream and every connection are closed when the target throws
 * mid-chunk. A `RowPipe` failure propagates (plan P3, not in scope: retry), so every test here
 * asserts the exception reached the caller *and* that nothing was left open behind it.
 *
 * Leaks are counted by [RecordingConnections], which proxies the source connection, its
 * statements and its result sets. It fails when nothing was opened as well as when something was
 * left open, so a pipe that died before querying cannot pass by doing nothing. The happy-path
 * counterpart lives in [RowPipeTest]; these are the tests that give the counter teeth, since a
 * writer that closes only on the happy path balances there and not here.
 */
class RowPipeFailureTest {

    private val sourceDb = Pipe.openDuck()
    private val targetDb = Pipe.openDuck()
    private val recording = RecordingConnections { sourceDb.duplicate() }
    private val jdbi: Jdbi = Jdbi.create(recording)

    @AfterEach
    fun closeConnections() {
        sourceDb.close()
        targetDb.close()
    }

    private fun source() = JdbcSource(jdbi, "select lot_id, lot_code, qty, site from wip_src order by lot_id")

    /**
     * The headline case. The target takes chunk 1 whole, then takes half of chunk 2 and throws -
     * so the failure lands with rows of the failing chunk already flushed, which is the shape
     * spec 12 measured as retaining every completed row.
     *
     * Three things are asserted, and the resource assertions are the point: an exception test
     * that only catches the exception would pass against a pipe that leaked the source result
     * set on every failure.
     */
    @Test
    fun `the source stream and its connection close when the target throws mid chunk`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 175)
        val probe = ProbeWriter(
            delegate = DuckDbTableWriter(targetDb, "wip_stg", CreateTable.AUTO, Pipe.STEP),
            failOnChunk = 2,
        )

        val thrown = catchThrowable {
            RowPipe(source(), probe, Pipe.STEP, chunkSize = 50).run()
        }

        assertThat(thrown).isInstanceOf(TargetFailure::class.java)
            .hasMessageContaining(Pipe.STEP)
        recording.assertStreamed("target throws mid-chunk")
        assertThat(probe.closes).describedAs("the target is closed on the failure path too").isEqualTo(1)
        assertThat(probe.chunkSizes).describedAs("the pipe stopped at the failing chunk").containsExactly(50, 50)
        // The 50 rows of chunk 1 and the 25 the failing chunk flushed before it threw. Spec 12:
        // a flushed row survives, and a chunk left part-appended does not.
        assertThat(Pipe.rowCount(targetDb, "wip_stg")).isEqualTo(75L)
    }

    /**
     * Usability after the failure, which is what "returns to a usable state" means for a pipe
     * whose failure propagates: the same `JdbcSource` runs again, from the top, into a fresh
     * target, and reads the whole source. A pipe that left the source handle open or the source
     * result set half-consumed would not manage it, and the leak counter would show it.
     */
    @Test
    fun `a failed pipe leaves the source usable for the next pipe`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 175)
        val failing = ProbeWriter(failOnChunk = 2)
        catchThrowable { RowPipe(source(), failing, Pipe.STEP, chunkSize = 50).run() }

        val result = RowPipe(
            source(),
            DuckDbTableWriter(targetDb, "wip_retry", CreateTable.AUTO, Pipe.STEP),
            Pipe.STEP,
            chunkSize = 50,
        ).run()

        assertThat(result).isEqualTo(PipeResult(175L, 175L))
        assertThat(Pipe.rowCount(targetDb, "wip_retry")).isEqualTo(175L)
        assertThat(recording.connectionsOpened.get()).describedAs("one connection per run, two runs").isEqualTo(2)
        recording.assertStreamed("failed run then a good one")
    }

    /**
     * Two failures racing to be the one the caller sees. The write failure is the one that lost
     * data, so it must be the exception that propagates, and the close failure must arrive with
     * it rather than replacing it - which is what `use` gives, through `addSuppressed`, and what
     * nothing in this suite asserted until here. A pipe that closed its target in a bare
     * `finally` would report the close failure and hide the reason the step failed.
     */
    @Test
    fun `a throwing close does not swallow the failure that lost the data`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 175)
        val probe = ProbeWriter(failOnChunk = 2, failOnClose = true)

        val thrown = catchThrowable { RowPipe(source(), probe, Pipe.STEP, chunkSize = 50).run() }

        assertThat(thrown).isInstanceOf(TargetFailure::class.java)
            .describedAs("the caller sees the write failure, not the close failure")
            .hasMessageContaining("chunk 2")
        assertThat(thrown.suppressed).hasSize(1)
        assertThat(thrown.suppressed[0]).isInstanceOf(TargetFailure::class.java)
            .hasMessageContaining("close")
        assertThat(probe.closes).isEqualTo(1)
        recording.assertStreamed("target throws from write and from close")
    }

    /**
     * A target that throws at `open` - which is where spec 4.6 rejects a BLOB column, a nullable
     * DOUBLE, or an undeclarable DECIMAL width, so this is the common failure in practice. The
     * source query has already run by then, because `open` takes the source column list, so
     * there is a live statement and result set to leak.
     */
    @Test
    fun `the source closes when the target throws at open`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 10)
        val probe = ProbeWriter(failOnOpen = true)

        val thrown = catchThrowable { RowPipe(source(), probe, Pipe.STEP, chunkSize = 4).run() }

        assertThat(thrown).isInstanceOf(TargetFailure::class.java)
        assertThat(probe.chunkSizes).describedAs("no chunk is written after a failed open").isEmpty()
        recording.assertStreamed("target throws at open")
    }

    /**
     * A transform is caller code running inside the pipe (spec 9.1), so it is the third place a
     * failure can start. Nothing catches it - retry is P5 - but the resources still close.
     */
    @Test
    fun `the source closes when the transform throws`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 175)
        val probe = ProbeWriter(DuckDbTableWriter(targetDb, "wip_stg", CreateTable.AUTO, Pipe.STEP))

        val thrown = catchThrowable {
            RowPipe(
                source(),
                probe,
                Pipe.STEP,
                chunkSize = 50,
                transform = RowTransform { row ->
                    if (row.long("lot_id") == 60L) throw IllegalStateException("transform gave up on lot 60")
                    row
                },
            ).run()
        }

        assertThat(thrown).isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("lot 60")
        recording.assertStreamed("transform throws")
        assertThat(probe.closes).isEqualTo(1)
    }
}
