package infra.etl.pipe

import infra.etl.Pipe
import infra.etl.ProbeWriter
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll

/**
 * Review finding M3: **which rows a chunk counts.**
 *
 * The execution contract and [RowPipe]'s own KDoc both fix the loop as accumulate up to
 * `chunkSize` rows, apply the transform, write. `pump` applied the transform first, so a chunk
 * filled with `chunkSize` *surviving* rows instead - and a selective transform then stretched one
 * commit across far more source rows than the author asked for. At the documented ratio in the
 * class KDoc of `RowPipe`'s caller, a transform keeping one row in a thousand turns a chunk size
 * of 5000 into a single commit spanning five million source rows: a transient failure four
 * million rows in commits nothing, and the retry re-reads the whole span.
 *
 * No test pinned either order, which is why the deviation survived four phases. This one pins it
 * from the source side, where the difference is visible.
 */
class RowPipeChunkOrderTest {

    private val sourceDb = Pipe.openDuck()
    private val jdbi: Jdbi = Jdbi.create { sourceDb.duplicate() }

    @AfterEach
    fun closeConnection() = sourceDb.close()

    /** Keeps the rows whose `lot_id` satisfies [keep], and drops the rest. */
    private fun keeping(keep: (Long) -> Boolean) = RowTransform { row ->
        row.takeIf { keep(it.long("lot_id")!!) }
    }

    private fun pipe(rows: Int, chunkSize: Int, transform: RowTransform): Pair<ProbeWriter, PipeResult> {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = rows)
        val writer = ProbeWriter()
        val result = RowPipe(
            source = JdbcSource(jdbi = jdbi, sql = "select * from wip_src order by lot_id"),
            target = writer,
            step = Pipe.STEP,
            chunkSize = chunkSize,
            transform = transform,
        ).run()
        return writer to result
    }

    /**
     * Ten source rows at a chunk size of four, with a transform dropping every second row.
     *
     * Counting source rows gives chunks of `[0..3]`, `[4..7]`, `[8..9]`, which survive as 2, 2 and
     * 1 rows. Counting survivors gives one write of 4 - reached only at source row 6 - and a
     * remainder of 1. The two orders are told apart by nothing else: both read ten rows and write
     * five, which is what the control below asserts.
     */
    @Test
    fun `a chunk boundary falls every chunkSize source rows and not every chunkSize written rows`() {
        val (writer, result) = pipe(rows = 10, chunkSize = 4, transform = keeping { it % 2 == 0L })

        assertAll(
            {
                assertEquals(listOf(2, 2, 1), writer.chunkSizes) {
                    "a chunk holds the survivors of chunkSize source rows (spec 5.2); [4, 1] is the " +
                        "transform-first order, which stretches one commit across the whole source"
                }
            },
            { assertEquals(PipeResult(10, 5), result) { "the order must not change what was read or written" } },
        )
    }

    /**
     * The control that stops the assertion above being satisfied by a pipe that writes eagerly:
     * with no transform at all, every chunk is full and the boundaries are the same under either
     * order.
     */
    @Test
    fun `without a transform the chunking is unchanged`() {
        val (writer, result) = pipe(rows = 10, chunkSize = 4, transform = keeping { true })

        assertAll(
            { assertEquals(listOf(4, 4, 2), writer.chunkSizes) },
            { assertEquals(PipeResult(10, 10), result) },
        )
    }

    /**
     * A chunk whose rows the transform drops entirely is not written as an empty chunk. Under the
     * old order that could not arise - the buffer simply stayed short - and it must not arise now
     * either: `DuckDbTableWriter.write` flushes the appender on every call, so an empty write would
     * add a commit boundary for a chunk that has nothing to commit.
     */
    @Test
    fun `a chunk the transform empties is not written at all`() {
        val (writer, result) = pipe(rows = 8, chunkSize = 4, transform = keeping { it >= 4L })

        assertAll(
            { assertEquals(listOf(4), writer.chunkSizes) { "the first four source rows were all dropped" } },
            { assertEquals(PipeResult(8, 4), result) },
        )
    }
}
