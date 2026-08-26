package infra.simpleetl.spike

import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.sql.SQLException
import kotlin.io.path.fileSize

/**
 * P0 / S2 - scratch growth and process RSS.
 *
 * One "run" models one task run under spec 7.2: one DuckDB file, five sequential
 * one-million-row writes, one of which fails twice before succeeding. Failures follow the
 * attempt-suffix scheme of spec 5.5: each attempt writes ds__aN, a successful attempt
 * repoints the stable view, and a failed attempt's table is left in place. Nothing is
 * dropped, deleted, or truncated. At run end the instance closes and the file is deleted.
 *
 * The run repeats in one JVM so RSS can be compared against the baseline.
 *
 * -Dspike.s2.runs=N  -Dspike.s2.rows=N  -Dspike.s2.datasets=N  -Dspike.s2.memMb=N
 */
class S2ScratchGrowthSpike {

    @Test
    fun measure() {
        val runs = intProp("spike.s2.runs", 10)
        val rows = intProp("spike.s2.rows", 1_000_000)
        val datasets = intProp("spike.s2.datasets", 5)
        val memMb = intProp("spike.s2.memMb", 4096)
        val entropy = entropyProp()
        val root = Files.createTempDirectory("s2-spike")

        println("S2 runs=$runs rowsPerWrite=$rows datasets=$datasets memory_limit=${memMb}MB entropy=$entropy")
        println("failing dataset = ds3, attempts a1 and a2 fail at 60% of rows, a3 succeeds")

        val baseline = settledMem()
        println("baseline rss=${mb(baseline.rss)}MB heap=${mb(baseline.heap)}MB nonHeap=${mb(baseline.nonHeap)}MB")

        val rowsPerRun = rows.toLong() * datasets + (rows * 0.6).toLong() * 2   // 2 failed attempts at 60%
        val lines = mutableListOf<String>()
        try {
            for (run in 1..runs) {
                val started = System.nanoTime()
                val sizes = oneRun(root, run, rows, datasets, memMb, entropy)
                val wall = (System.nanoTime() - started) / 1_000_000
                val m = settledMem()
                lines += "| $run | ${mb(m.rss)}MB | ${sign(m.rss - baseline.rss)}MB | " +
                    "${mb(sizes.first)}MB db / ${mb(sizes.second)}MB dir | ${mb(m.heap)}MB | ${mb(m.nonHeap)}MB | ${wall}ms |"
                println("  run $run done: ${lines.last()}")
            }
        } finally {
            root.toFile().deleteRecursively()
        }

        println()
        println("rows appended per run: $rowsPerRun")
        println("| Run | RSS after | Delta vs baseline | File size at end of run | Heap | NonHeap | Wall |")
        lines.forEach(::println)
        val end = settledMem()
        println("baseline rss=${mb(baseline.rss)}MB  final rss=${mb(end.rss)}MB  peak rss=${mb(end.peakRss)}MB")
    }

    /** Returns db file bytes and whole scratch dir bytes, both measured at the end of the run. */
    private fun oneRun(root: Path, run: Int, rows: Int, datasets: Int, memMb: Int, entropy: Entropy): Pair<Long, Long> {
        val dir = Files.createDirectory(root.resolve("run$run"))
        val db = dir.resolve("scratch.duckdb")
        try {
            var dbSize: Long
            var dirSize: Long
            openScratch(db, memMb, dir).use { conn ->
                for (d in 1..datasets) {
                    val dataset = "ds$d"
                    val failing = d == 3
                    var attempt = 1
                    while (true) {
                        val physical = "${dataset}__a$attempt"
                        conn.exec("create table $physical $ROW_DDL")
                        val failAt = if (failing && attempt <= 2) (rows * 0.6).toInt() else -1
                        try {
                            appendRows(conn, physical, rows, flushEvery = 5000, failAtRow = failAt, entropy = entropy)
                        } catch (injected: SQLException) {
                            // spec 5.5: leave the failed attempt in place, no DROP / DELETE / TRUNCATE
                            attempt++
                            continue
                        }
                        conn.exec("create or replace view $dataset as select * from $physical")
                        break
                    }
                    check(conn.scalar("select count(*) from $dataset") == rows.toLong())
                }
                if (run == 1) {
                    // evidence that spec 5.5 held: failed attempts survive, the view points at the last one
                    println("  run 1 catalog: " + conn.scalar(
                        "select string_agg(table_name || '=' || estimated_size, ' ' order by table_name) " +
                            "from duckdb_tables()"))
                    println("  run 1 views:   " + conn.scalar(
                        "select string_agg(view_name, ' ' order by view_name) from duckdb_views() " +
                            "where internal = false"))
                }
                conn.exec("CHECKPOINT")
                dbSize = db.fileSize()
                dirSize = dirBytes(dir)
            }
            // instance closed; the file is the only thing left, this is the size the volume must hold
            return maxOf(dbSize, db.fileSize()) to maxOf(dirSize, dirBytes(dir))
        } finally {
            dir.toFile().deleteRecursively()   // spec 7.2: close the instance and delete the file
        }
    }

    /** GC first so heap noise does not mask whether native memory came back. */
    private fun settledMem(): Mem {
        System.gc()
        System.gc()
        return mem()
    }

    private fun sign(bytes: Long) = (if (bytes >= 0) "+" else "-") + mb(kotlin.math.abs(bytes))
}
