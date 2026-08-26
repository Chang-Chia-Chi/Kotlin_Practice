package infra.simpleetl.spike

import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.fileSize

/**
 * P0 / S4a - wide-row density.
 *
 * S2 measured bytes-per-value on a 4-column row only. This measures whether that constant
 * holds as column count rises. Total values are held roughly constant across the three
 * widths, so the comparison is per stored value and not per row.
 *
 * -Dspike.s4a.values=N
 */
class S4aWideRowDensitySpike {

    @Test
    fun measure() {
        val totalValues = intProp("spike.s4a.values", 12_000_000)
        val widths = listOf(4, 15, 30)
        val root = Files.createTempDirectory("s4a-spike")

        println("S4a targetValuesPerCase=$totalValues widths=$widths")
        val rowsOut = mutableListOf<String>()
        try {
            for (entropy in listOf(Entropy.LOW, Entropy.HIGH)) {
                for (width in widths) {
                    val cols = mixedColumns(width)
                    val rows = totalValues / width
                    val (bytes, empty) = build(root, "e${entropy}_w$width", cols, rows, entropy)
                    val values = rows.toLong() * width
                    val perValue = (bytes - empty).toDouble() / values
                    val mixed = cols.groupingBy { it }.eachCount().entries
                        .sortedBy { it.key.name }.joinToString("+") { "${it.value}${it.key.name.take(2)}" }
                    rowsOut += "| $width | $entropy | $rows | $values | ${mb(bytes - empty)}MB | " +
                        "%.2f".format(perValue) + " | $mixed |"
                    println("  " + rowsOut.last())
                }
            }
        } finally {
            root.toFile().deleteRecursively()
        }

        println()
        println("| Columns | Entropy | Rows | Values | File bytes (empty subtracted) | Bytes/value | Type mix |")
        rowsOut.forEach(::println)
    }

    /** Returns the checkpointed file size and the empty-file baseline that was subtracted. */
    private fun build(root: Path, tag: String, cols: List<ColKind>, rows: Int, entropy: Entropy): Pair<Long, Long> {
        val dir = Files.createDirectory(root.resolve(tag))
        val db = dir.resolve("scratch.duckdb")
        try {
            openScratch(db, 4096, dir).use { conn ->
                conn.exec("CHECKPOINT")
                val empty = db.fileSize()
                conn.exec("create table t ${mixedDdl(cols)}")
                appendMixedRows(conn, "t", cols, rows, entropy)
                conn.exec("CHECKPOINT")
                check(conn.scalar("select count(*) from t") == rows.toLong())
                return db.fileSize() to empty
            }
        } finally {
            dir.toFile().deleteRecursively()
        }
    }
}
