package infra.etl.spike

import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.fileSize

/**
 * P0 / S4b - spill factor.
 *
 * Builds one dataset, then runs queries that cannot fit in memory_limit and samples the
 * temp_directory while they run. Spill space is released the instant a query ends, so only
 * a sampled peak is meaningful; an end-of-query reading is always zero.
 *
 * Also tests the claim that "without it a large join fails outright instead of
 * spilling" by running the same query with temp_directory left unset.
 *
 * -Dspike.s4b.rows=N  -Dspike.s4b.limitLow=MB  -Dspike.s4b.limitHigh=MB
 */
@Tag("spike")
class S4bSpillFactorSpike {

    private val queries = listOf(
        "hash join" to
            "select count(*) from t a join t b on a.code = b.code and a.id >= b.id",
        "sort / window" to
            "select sum(rn % 1000) from (select row_number() over (order by code) as rn from t)",
        "hash aggregate" to
            "select count(*) from (select code, count(*) as n from t group by code)",
    )

    @Test
    fun measure() {
        val rows = intProp("spike.s4b.rows", 10_000_000)
        val limits = listOf(intProp("spike.s4b.limitLow", 256), intProp("spike.s4b.limitHigh", 512))
        val root = Files.createTempDirectory("s4b-spike")
        val db = root.resolve("big.duckdb")
        val spill = Files.createDirectory(root.resolve("spill"))

        try {
            val buildStart = System.nanoTime()
            openScratch(db, 4096, spill).use { conn ->
                conn.exec("create table t $ROW_DDL")
                appendRows(conn, "t", rows, flushEvery = 5000, entropy = Entropy.HIGH)
                conn.exec("CHECKPOINT")
            }
            val input = db.fileSize()
            println("S4b rows=$rows inputOnDisk=${mb(input)}MB built in ${(System.nanoTime() - buildStart) / 1_000_000}ms")
            println("limits=${limits}MB  spill sampled every 50ms")

            val out = mutableListOf<String>()
            for (limit in limits) {
                for ((label, sql) in queries) {
                    out += run(label, sql, db, limit, spill, input)
                    println("  " + out.last())
                }
            }

            println()
            println("=== temp_directory NOT set (spec 7.2 claim) ===")
            val unset = mutableListOf<String>()
            for ((label, sql) in queries) {
                unset += runUnset(label, sql, db, limits.first(), root, input)
                println("  " + unset.last())
            }

            println()
            println("| memory_limit | Query | Outcome | Peak spill | Spill factor vs input | Left after close | Wall |")
            out.forEach(::println)
            println()
            println("| memory_limit | Query | temp_directory unset - outcome | Peak outside the db file | Files created | Wall |")
            unset.forEach(::println)
        } finally {
            root.toFile().deleteRecursively()
        }
    }

    private fun run(label: String, sql: String, db: Path, limit: Int, spill: Path, input: Long): String {
        spill.toFile().listFiles()?.forEach { it.deleteRecursively() }   // start from a clean spill dir
        val started = System.nanoTime()
        var outcome: String
        var peak = 0L
        openScratch(db, limit, spill).use { conn ->
            val (result, p) = sampledPeak(50, { dirBytes(spill) }) {
                runCatching { conn.scalar(sql) }
            }
            peak = p
            outcome = result.fold({ "ok (=$it)" }, { "FAILED ${it.javaClass.simpleName}: ${oneLine(it.message)}" })
        }
        val wall = (System.nanoTime() - started) / 1_000_000
        val leftover = dirBytes(spill)   // after close: does a failed query orphan its spill files?
        val factor = if (peak == 0L) "no spill" else "%.2fx".format(peak.toDouble() / input)
        return "| ${limit}MB | $label | $outcome | ${mb(peak)}MB | $factor | ${mb(leftover)}MB | ${wall}ms |"
    }

    /**
     * Same query with temp_directory unset. The db is copied into an empty directory so
     * anything that appears there is DuckDB's own doing, and names are captured by the
     * sampler because DuckDB removes its temp directory before close() returns.
     */
    private fun runUnset(label: String, sql: String, src: Path, limit: Int, root: Path, input: Long): String {
        val dir = Files.createDirectory(root.resolve("unset-${label.filter { it.isLetter() }}"))
        val db = dir.resolve("big.duckdb")
        Files.copy(src, db)
        val seen = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
        val started = System.nanoTime()
        var outcome: String
        var peak = 0L
        try {
            openScratch(db, limit, null).use { conn ->
                val (result, p) = sampledPeak(50, {
                    dir.toFile().list()?.forEach { if (it != "big.duckdb") seen.add(it) }
                    dirBytes(dir)
                }) { runCatching { conn.scalar(sql) } }
                peak = p
                outcome = result.fold({ "ok (=$it)" }, { "FAILED ${it.javaClass.simpleName}: ${oneLine(it.message)}" })
            }
        } finally {
            dir.toFile().deleteRecursively()
        }
        val wall = (System.nanoTime() - started) / 1_000_000
        val created = if (seen.isEmpty()) "none" else seen.sorted().joinToString("+")
        return "| ${limit}MB | $label | $outcome | ${mb(peak - input)}MB | $created | ${wall}ms |"
    }

    private fun oneLine(s: String?) = (s ?: "").replace(Regex("\\s+"), " ").take(140)
}
