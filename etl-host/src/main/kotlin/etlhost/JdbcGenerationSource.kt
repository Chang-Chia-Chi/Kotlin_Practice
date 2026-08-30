package etlhost

import infra.etl.duckdb.CreateTable
import infra.etl.duckdb.DuckDbTableWriter
import infra.etl.pipe.JdbcSource
import infra.etl.pipe.RowPipe
import infra.snapshotcache.api.BuildContext
import infra.snapshotcache.api.GenerationSource
import org.jboss.logging.Logger
import org.jdbi.v3.core.Jdbi

/**
 * The one seam that knows about the source system (snapshotcache spec 5.2), built out of SimpleEtl
 * Layer 1 rather than out of new code.
 *
 * This is spec 2.1's claim cashed: Layer 1 - `JdbcSource`, `RowPipe`, `DuckDbTableWriter` - ships
 * to the snapshot cache without Layer 2, and a host that has both gets its `GenerationSource` for
 * six lines. The typed mapping, the chunking, the appender and the borrow-don't-close contract on
 * `BuildContext.target` are all already tested in `RowPipeTest`; nothing is re-implemented here.
 *
 * The connection is the candidate generation file's and belongs to the cache. It is never closed
 * here, and `RowPipe` does not close it either.
 *
 * @param table the table inside the generation, which this host names after the group.
 * @param chunkSize `jdbc.fetchSize` (spec 7.2), which the framework leaves to this class because it
 *   is the only thing that opens a source connection.
 */
class JdbcGenerationSource(
    private val jdbi: Jdbi,
    private val sql: String,
    private val table: String,
    private val chunkSize: Int,
) : GenerationSource {

    override fun refresh(ctx: BuildContext) {
        val step = "refresh-$table"
        val result = RowPipe(
            source = JdbcSource(jdbi, sql),
            target = DuckDbTableWriter(ctx.target, table, CreateTable.AUTO, step),
            step = step,
            chunkSize = chunkSize,
        ).run()
        log.infov(
            "group {0} generation {1}: {2} row(s) read into {3}, dataAsOf {4}",
            ctx.group, ctx.generation, result.rowsRead, table, ctx.dataAsOf,
        )
    }

    private companion object {
        private val log: Logger = Logger.getLogger(JdbcGenerationSource::class.java)
    }
}
