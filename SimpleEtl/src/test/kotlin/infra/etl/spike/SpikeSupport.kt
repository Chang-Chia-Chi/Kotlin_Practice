package infra.etl.spike

import org.duckdb.DuckDBAppender
import org.duckdb.DuckDBConnection
import java.io.File
import java.lang.management.ManagementFactory
import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.sql.SQLException
import java.time.LocalDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.fileSize

/**
 * P0 spike support. Throwaway measurement code, not a framework deliverable.
 * Superseded by P1+ and deleted with the spike classes.
 */

fun intProp(name: String, default: Int): Int = System.getProperty(name)?.trim()?.toInt() ?: default

fun mb(bytes: Long): String = "%.1f".format(bytes / 1024.0 / 1024.0)

data class Mem(val rss: Long, val peakRss: Long, val heap: Long, val nonHeap: Long)

private val windows = System.getProperty("os.name").startsWith("Windows")

/**
 * OS-level resident memory for this process, plus JVM heap / non-heap for comparison.
 * Windows: PowerShell Get-Process WorkingSet64 / PeakWorkingSet64 on our own pid.
 * Linux:   /proc/self/status VmRSS / VmHWM.
 */
fun mem(): Mem {
    val mx = ManagementFactory.getMemoryMXBean()
    val (rss, peak) = if (windows) windowsRss() else linuxRss()
    return Mem(rss, peak, mx.heapMemoryUsage.used, mx.nonHeapMemoryUsage.used)
}

private fun windowsRss(): Pair<Long, Long> {
    val pid = ProcessHandle.current().pid()
    val script = "\$p = Get-Process -Id $pid; '{0},{1}' -f \$p.WorkingSet64, \$p.PeakWorkingSet64"
    val proc = ProcessBuilder("powershell", "-NoProfile", "-NonInteractive", "-Command", script)
        .redirectErrorStream(true)
        .start()
    val out = proc.inputStream.bufferedReader().readText().trim()
    proc.waitFor()
    val parts = out.lines().last().split(",")
    return parts[0].trim().toLong() to parts[1].trim().toLong()
}

private fun linuxRss(): Pair<Long, Long> {
    val status = File("/proc/self/status").readLines()
    fun kb(key: String) = status.first { it.startsWith(key) }.filter { it.isDigit() }.toLong() * 1024
    return kb("VmRSS:") to kb("VmHWM:")
}

/**
 * Runs [block] while sampling RSS on a daemon thread. Returns the block's value and the
 * highest RSS seen. Uses a latch rather than sleep so the sampler stops immediately.
 */
fun <T> sampledPeakRss(intervalMs: Long = 1000, block: () -> T): Pair<T, Long> =
    sampledPeak(intervalMs, { mem().rss }, block)

/**
 * Runs [block] while a daemon thread polls [probe], returning the block's value and the
 * highest probe reading. Spill space is reclaimed the moment a query ends, so an
 * end-of-query directory reading is always zero; only sampling sees the peak.
 */
fun <T> sampledPeak(intervalMs: Long, probe: () -> Long, block: () -> T): Pair<T, Long> {
    val peak = AtomicLong(0)
    val stop = CountDownLatch(1)
    val sampler = Thread {
        do peak.updateAndGet { maxOf(it, probe()) }
        while (!stop.await(intervalMs, TimeUnit.MILLISECONDS))
    }
    sampler.isDaemon = true
    sampler.start()
    try {
        val value = block()
        return value to peak.get()
    } finally {
        stop.countDown()
        sampler.join()
    }
}

/**
 * Opens a file-mode DuckDB with the settings a scratch database applies at open.
 * A null [tempDir] deliberately leaves temp_directory unset, which is what S4b tests.
 */
fun openScratch(dbFile: Path, memoryLimitMb: Int, tempDir: Path?): DuckDBConnection {
    val conn = DriverManager.getConnection("jdbc:duckdb:${dbFile.toAbsolutePath()}") as DuckDBConnection
    conn.createStatement().use { s ->
        s.execute("SET memory_limit='${memoryLimitMb}MB'")
        if (tempDir != null) {
            s.execute("SET temp_directory='${tempDir.toAbsolutePath().toString().replace('\\', '/')}'")
        }
    }
    return conn
}

const val ROW_DDL = "(id BIGINT, code VARCHAR, qty DECIMAL(18,3), ts TIMESTAMP)"

/**
 * Row entropy. DuckDB compresses hard, so the two ends bracket any real dataset:
 * LOW is sequential ids and 1000 repeated codes, which RLE and dictionary away to almost
 * nothing; HIGH is random values in every column, which barely compress at all.
 */
enum class Entropy { LOW, HIGH }

fun entropyProp(default: Entropy = Entropy.HIGH): Entropy =
    System.getProperty("spike.entropy")?.uppercase()?.let(Entropy::valueOf) ?: default

/**
 * Appends [rows] representative rows through the appender.
 * [flushEvery] <= 0 means flush only at close. [failAtRow] >= 0 injects a failure.
 */
fun appendRows(
    conn: DuckDBConnection,
    table: String,
    rows: Int,
    flushEvery: Int,
    failAtRow: Int = -1,
    entropy: Entropy = Entropy.LOW,
) {
    val base = LocalDateTime.of(2024, 1, 1, 0, 0)
    val rnd = java.util.Random(42)                      // fixed seed: reproducible
    val hex = "0123456789abcdef".toCharArray()
    conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, table).use { a ->
        for (i in 0 until rows) {
            if (i == failAtRow) throw SQLException("injected transient failure at row $i")
            a.beginRow()
            if (entropy == Entropy.LOW) {
                a.append(i.toLong())
                a.append("code-${i % 1000}")
                a.appendBigDecimal(BigDecimal.valueOf((i % 100_000).toLong(), 3))
                a.appendLocalDateTime(base.plusSeconds(i.toLong()))
            } else {
                a.append(rnd.nextLong())
                a.append(CharArray(16) { hex[rnd.nextInt(16)] }.concatToString())
                a.appendBigDecimal(BigDecimal.valueOf(rnd.nextLong() % 1_000_000_000_000L, 3))
                a.appendLocalDateTime(base.plusSeconds(rnd.nextInt(31_536_000).toLong()))
            }
            a.endRow()
            if (flushEvery > 0 && (i + 1) % flushEvery == 0) a.flush()
        }
    }
}

fun DuckDBConnection.exec(sql: String) = createStatement().use { it.execute(sql) }

fun DuckDBConnection.scalar(sql: String): Any? =
    createStatement().use { s -> s.executeQuery(sql).use { r -> if (r.next()) r.getObject(1) else null } }

/** Total bytes of everything in [dir] - db file, WAL, and any temp spill. */
fun dirBytes(dir: Path): Long {
    if (!Files.isDirectory(dir)) return 0
    // spill files appear and vanish under the sampler, so a racing walk must not throw
    return try {
        Files.walk(dir).use { s ->
            s.filter { Files.isRegularFile(it) }.mapToLong { runCatching { it.fileSize() }.getOrDefault(0) }.sum()
        }
    } catch (racing: java.io.IOException) {
        0
    }
}

fun appendString(a: DuckDBAppender, v: String?) = a.append(v)

// --- S4a: variable-width rows with a realistic type mix -----------------------------

enum class ColKind(val sqlType: String) {
    VARCHAR("VARCHAR"), BIGINT("BIGINT"), DOUBLE("DOUBLE"),
    TIMESTAMP("TIMESTAMP"), DECIMAL("DECIMAL(18,3)"),
}

/**
 * [n] columns cycling text / numeric / temporal, alternating the two members of the
 * numeric and temporal groups. At n divisible by 3 this is exactly one third each.
 */
fun mixedColumns(n: Int): List<ColKind> = (0 until n).map { i ->
    when (i % 3) {
        0 -> ColKind.VARCHAR
        1 -> if (i / 3 % 2 == 0) ColKind.BIGINT else ColKind.DOUBLE
        else -> if (i / 3 % 2 == 0) ColKind.TIMESTAMP else ColKind.DECIMAL
    }
}

fun mixedDdl(cols: List<ColKind>): String =
    cols.mapIndexed { i, c -> "c$i ${c.sqlType}" }.joinToString(", ", "(", ")")

fun appendMixedRows(
    conn: DuckDBConnection,
    table: String,
    cols: List<ColKind>,
    rows: Int,
    entropy: Entropy,
) {
    val base = LocalDateTime.of(2024, 1, 1, 0, 0)
    val rnd = java.util.Random(42)
    val hex = "0123456789abcdef".toCharArray()
    val high = entropy == Entropy.HIGH
    conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, table).use { a ->
        for (i in 0 until rows) {
            a.beginRow()
            for (c in cols) when (c) {
                ColKind.VARCHAR ->
                    a.append(if (high) CharArray(16) { hex[rnd.nextInt(16)] }.concatToString() else "code-${i % 1000}")
                ColKind.BIGINT -> a.append(if (high) rnd.nextLong() else i.toLong())
                ColKind.DOUBLE -> a.append(if (high) rnd.nextDouble() * 1e9 else (i % 1000).toDouble())
                ColKind.TIMESTAMP ->
                    a.appendLocalDateTime(base.plusSeconds(if (high) rnd.nextInt(31_536_000).toLong() else i.toLong()))
                ColKind.DECIMAL ->
                    a.appendBigDecimal(BigDecimal.valueOf(if (high) rnd.nextLong() % 1_000_000_000_000L else (i % 100_000).toLong(), 3))
            }
            a.endRow()
            if ((i + 1) % 5000 == 0) a.flush()
        }
    }
}
