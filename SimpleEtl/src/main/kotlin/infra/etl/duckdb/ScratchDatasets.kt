package infra.etl.duckdb

import java.nio.file.Path

/**
 * The write-then-publish protocol for one scratch dataset, as a module rather than as a
 * rule each caller remembers.
 *
 * [DatasetNamer] names things and points a view at them; it deliberately does not decide *when* to
 * point it, because publishing is what makes an attempt the live one. That left the sequence -
 * name this attempt, write into that name, publish only if the write returned - to four call sites
 * in `TaskEngine`, each of which spelled it out by hand and each of which grew a comment restating
 * the same rule. This class is that sequence, written once.
 *
 * ```
 * attempt 1 throws     ->  table wip_stg__a1 left unreferenced, no view
 * attempt 2 returns    ->  table wip_stg__a2, and wip_stg now resolves to it
 * ```
 *
 * **A failed attempt does not publish, and there is no `catch` here that makes that true.** The
 * publish is the statement after the block, so anything the block throws leaves it unreached and
 * propagates to the step's retry loop. Catching and rethrowing would be the same behaviour with
 * one more place to get it wrong.
 *
 * **Two entry points rather than one plus a `format` argument**, because the format enum is
 * `MaterializeFormat` and it lives in `infra.etl.task`. This is an adapter in `infra.etl.duckdb`,
 * and an adapter never names the layer above it - the ArchUnit rule in `ArchitectureTest` is what
 * holds that, and a `format` parameter here would be the one thing that broke it.
 *
 * Nothing is dropped, deleted or truncated to make room: DuckDB 1.1.3 reclaims nothing, and
 * [ScratchDb.close] emptying the run directory is the only reclamation point.
 */
internal class ScratchDatasets(private val scratch: ScratchDb, private val namer: DatasetNamer) {

    /**
     * Writes one attempt of [dataset] as a table and publishes the stable view over it.
     *
     * @param write given the attempt-suffixed physical name to write into. Its value is returned
     *   unchanged, because two of the callers need one - a `pipe` carries out the rows it moved and
     *   a `cacheCopy` the generation it read.
     */
    fun <T> attemptTable(dataset: String, attempt: Int, write: (String) -> T): T {
        val written = write(namer.physical(dataset, attempt))
        namer.publishTable(scratch.connection(), dataset, attempt)
        return written
    }

    /**
     * Writes one attempt of [dataset] as a parquet file and publishes the stable view over it.
     *
     * Downstream the result is indistinguishable from [attemptTable]: a view over `read_parquet`
     * and a view over the equivalent table report the same column names and driver types on
     * duckdb_jdbc 1.1.3, which is what lets a `materialize` change `format` without any other step
     * changing.
     *
     * @param write given the absolute path of this attempt's file. A retry overwrites its own
     *   attempt's file rather than adding a table to the database.
     */
    fun <T> attemptParquet(dataset: String, attempt: Int, write: (Path) -> T): T {
        val written = write(namer.parquetPath(dataset, attempt))
        namer.publishParquet(scratch.connection(), dataset, attempt)
        return written
    }
}
