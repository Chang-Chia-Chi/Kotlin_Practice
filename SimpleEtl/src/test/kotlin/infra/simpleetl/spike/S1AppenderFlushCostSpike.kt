package infra.simpleetl.spike

import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.deleteRecursively

/**
 * P0 / S1 - appender flush cost.
 * One million rows with flush() every 5000 rows versus flush only at close().
 * Variants alternate over several iterations so JIT warmup does not favour whichever ran first.
 *
 * -Dspike.s1.rows=N   -Dspike.s1.iters=N   -Dspike.s1.memMb=N
 */
class S1AppenderFlushCostSpike {

    @OptIn(kotlin.io.path.ExperimentalPathApi::class)
    @Test
    fun measure() {
        val rows = intProp("spike.s1.rows", 1_000_000)
        val iters = intProp("spike.s1.iters", 3)
        val memMb = intProp("spike.s1.memMb", 4096)
        val root = Files.createTempDirectory("s1-spike")

        println("S1 rows=$rows iters=$iters memory_limit=${memMb}MB")
        try {
            runVariant(root, "warmup", 50_000, 5000, memMb)   // JIT and native library warmup

            val results = LinkedHashMap<String, MutableList<Pair<Long, Long>>>()
            repeat(iters) { i ->
                for ((label, flushEvery) in listOf("flush per 5000" to 5000, "flush at close only" to 0)) {
                    val r = runVariant(root, "i${i}_${flushEvery}", rows, flushEvery, memMb)
                    results.getOrPut(label) { mutableListOf() }.add(r)
                    println("  iter $i $label wall=${r.first}ms peakRss=${mb(r.second)}MB")
                }
            }

            println()
            println("| Variant | Wall time (median of $iters) | Peak RSS (max) | All wall times |")
            results.forEach { (label, rs) ->
                val walls = rs.map { it.first }.sorted()
                val median = walls[walls.size / 2]
                val peak = rs.maxOf { it.second }
                println("| $label | ${median}ms | ${mb(peak)}MB | ${rs.map { it.first }.joinToString("/")} |")
            }
            val m = mem()
            println("JVM at end: heap=${mb(m.heap)}MB nonHeap=${mb(m.nonHeap)}MB rss=${mb(m.rss)}MB peakRss=${mb(m.peakRss)}MB")
        } finally {
            root.deleteRecursively()
        }
    }

    /** Returns wall millis and peak RSS observed during the append. */
    private fun runVariant(root: Path, tag: String, rows: Int, flushEvery: Int, memMb: Int): Pair<Long, Long> {
        val dir = Files.createDirectory(root.resolve(tag))
        val db = dir.resolve("scratch.duckdb")
        return try {
            openScratch(db, memMb, dir).use { conn ->
                conn.exec("create table t $ROW_DDL")
                val started = System.nanoTime()
                val (_, peak) = sampledPeakRss(500) { appendRows(conn, "t", rows, flushEvery) }
                val wall = (System.nanoTime() - started) / 1_000_000
                check(conn.scalar("select count(*) from t") == rows.toLong())
                wall to peak
            }
        } finally {
            dir.toFile().deleteRecursively()
        }
    }
}
