package infra.etl.spike

import org.duckdb.DuckDBAppender
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.nio.file.Files
import java.time.LocalDateTime

/**
 * P0 / S3 - implicit cast on append.
 *
 * Answers whether DuckDB 1.1.3's object-typed appender methods (the only null-accepting
 * ones) can target a column type they are not named for, and whether the value
 * round-trips exactly. This decides whether validation rule 15 can be relaxed.
 *
 * Each case gets its own database file so an appender failure cannot poison the next case.
 */
@Tag("spike")
class S3ImplicitCastSpike {

    private class Case(val label: String, val colType: String, val append: (DuckDBAppender) -> Unit)

    @Test
    fun measure() {
        val dt = LocalDateTime.of(2024, 1, 15, 13, 45, 30, 123_000_000)
        val cases = listOf(
            // the rule 15 question: object-typed methods into non VARCHAR/DECIMAL/TIMESTAMP columns
            Case("appendBigDecimal(42)", "BIGINT") { it.appendBigDecimal(BigDecimal("42")) },
            Case("appendBigDecimal(42.7)", "BIGINT") { it.appendBigDecimal(BigDecimal("42.7")) },
            Case("appendBigDecimal(2^63, beyond Long)", "BIGINT") { it.appendBigDecimal(BigDecimal("9223372036854775808")) },
            Case("appendBigDecimal(null)", "BIGINT") { it.appendBigDecimal(null) },
            Case("appendBigDecimal(1.5)", "DOUBLE") { it.appendBigDecimal(BigDecimal("1.5")) },
            Case("appendBigDecimal(null)", "DOUBLE") { it.appendBigDecimal(null) },
            Case("appendLocalDateTime(2024-01-15T00:00)", "DATE") { it.appendLocalDateTime(LocalDateTime.of(2024, 1, 15, 0, 0)) },
            Case("appendLocalDateTime(2024-01-15T13:45:30.123)", "DATE") { it.appendLocalDateTime(dt) },
            Case("appendLocalDateTime(null)", "DATE") { it.appendLocalDateTime(null) },
            // the String overload as an alternative null carrier
            Case("append(String '42')", "BIGINT") { appendString(it, "42") },
            Case("append(String null)", "BIGINT") { appendString(it, null) },
            Case("append(String null)", "DOUBLE") { appendString(it, null) },
            Case("append(String null)", "DATE") { appendString(it, null) },
            Case("append(String 'true')", "BOOLEAN") { appendString(it, "true") },
            Case("append(String null)", "BOOLEAN") { appendString(it, null) },
            // controls: the three types the appender already allows
            Case("append(String null)", "VARCHAR") { appendString(it, null) },
            Case("appendBigDecimal(null)", "DECIMAL(18,3)") { it.appendBigDecimal(null) },
            Case("appendBigDecimal(42.750)", "DECIMAL(18,3)") { it.appendBigDecimal(BigDecimal("42.750")) },
            Case("appendLocalDateTime(null)", "TIMESTAMP") { it.appendLocalDateTime(null) },
            Case("appendLocalDateTime(2024-01-15T13:45:30.123)", "TIMESTAMP") { it.appendLocalDateTime(dt) },
        )

        val root = Files.createTempDirectory("s3-spike")
        println("| Case | Column type | Read back | DuckDB behaviour |")
        try {
            cases.forEachIndexed { i, c -> println(run(root, i, c)) }
        } finally {
            root.toFile().deleteRecursively()
        }
    }

    private fun run(root: java.nio.file.Path, index: Int, case: Case): String {
        val dir = Files.createDirectory(root.resolve("c$index"))
        var behaviour: String
        var readBack = "-"
        try {
            openScratch(dir.resolve("t.duckdb"), 512, dir).use { conn ->
                conn.exec("create table t (v ${case.colType})")
                behaviour = try {
                    conn.createAppender("main", "t").use { a ->
                        a.beginRow()
                        case.append(a)
                        a.endRow()
                        a.flush()
                    }
                    "stored without error"
                } catch (e: Exception) {
                    return "| ${case.label} | ${case.colType} | - | THREW ${e.javaClass.simpleName}: ${oneLine(e.message)} |"
                }
                val v = conn.scalar("select v from t")
                readBack = if (v == null) "NULL" else "${v.javaClass.simpleName}(${v})"
            }
        } finally {
            dir.toFile().deleteRecursively()
        }
        return "| ${case.label} | ${case.colType} | $readBack | $behaviour |"
    }

    private fun oneLine(s: String?) = (s ?: "").replace(Regex("\\s+"), " ").take(120)
}
