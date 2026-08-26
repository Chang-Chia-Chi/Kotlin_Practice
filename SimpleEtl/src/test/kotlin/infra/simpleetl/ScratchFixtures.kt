package infra.simpleetl

import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.ResultSet
import kotlin.io.path.extension
import kotlin.io.path.isRegularFile
import kotlin.io.path.readText

/**
 * P4 test support. Written for this phase rather than shared with P1's `Duck`, P2's `Scratch`
 * or P3's `Pipe`, all of which belong to phases that may not be edited.
 *
 * Datasets are built with `CREATE TABLE AS SELECT ... FROM range(n)`: no INSERT into DuckDB
 * (non-negotiable rule 1), no appender inside a fixture. Nothing here DELETEs, TRUNCATEs or
 * DROPs a DuckDB dataset (spec 5.5), and nothing creates a DuckDB temporary table (spec 7.2) -
 * which matters more here than in earlier phases, because one of this phase's own checks bans
 * exactly that and a fixture that used one would make the check a lie.
 *
 * [Scratchpad] holds only helpers. It briefly carried forwarders onto `ScratchDb` and
 * `DatasetNamer` as a reconciliation seam while the two halves of this phase were written blind;
 * once the shapes matched, the forwarders were pure renames and are gone. Tests construct both
 * types directly.
 */
object Scratchpad {

    /**
     * 512 MB, chosen so a readback can be told apart from DuckDB's default, which is roughly
     * 80% of machine RAM and therefore several GB on any machine this suite runs on.
     */
    const val MEMORY_LIMIT_MB = 512

    // ---------------------------------------------------------------------------------------
    // Filesystem
    // ---------------------------------------------------------------------------------------

    /**
     * Where the spill goes. Named but not created: the fixture must leave the filesystem exactly
     * as it found it, or the lazy-creation item would be tested against a directory tree this
     * file had already touched.
     */
    fun spillDir(root: Path): Path = root.resolve("spill")

    /**
     * Every regular file under [root], relative and slash-normalised. Directories are excluded
     * deliberately: the done-when item is about the scratch *file*, and whether an empty
     * directory is left behind is the caller's to decide, so asserting on directories would
     * pin down something P4 never specified.
     */
    fun regularFiles(root: Path): List<String> =
        Files.walk(root).use { walk ->
            walk.filter { it.isRegularFile() }
                .map { root.relativize(it).toString().replace(File.separatorChar, '/') }
                .sorted()
                .toList()
        }

    /** Forward slashes and doubled quotes, so a path can be embedded in DuckDB SQL. */
    fun sqlPath(path: Path): String =
        path.toAbsolutePath().toString().replace(File.separatorChar, '/').replace("'", "''")

    // ---------------------------------------------------------------------------------------
    // DuckDB
    // ---------------------------------------------------------------------------------------

    fun exec(connection: Connection, vararg sql: String) =
        connection.createStatement().use { statement -> sql.forEach { statement.execute(it) } }

    /**
     * One attempt's dataset. The `lot_code` marker column carries [marker], so "the view
     * resolves to attempt 2" is answered by the data and not by a row count two attempts could
     * share.
     *
     * The column set is deliberately wider than the naming tests need: a TIMESTAMP and a column
     * that is null on every other row, because the parquet parity item is a claim about the
     * whole read seam and a three-column all-populated projection is where a format difference
     * would be least likely to show.
     */
    fun createAttemptTable(connection: Connection, table: String, marker: String, rows: Int) =
        exec(connection, "create table $table as ${attemptSelect(marker, rows)}")

    /** The projection [createAttemptTable] lands, reused verbatim by the parquet half of spec 5.6. */
    fun attemptSelect(marker: String, rows: Int): String =
        """
        select cast(i as bigint)                            as lot_id,
               cast('$marker-' || i as varchar)              as lot_code,
               cast(i * 1.5 as decimal(18,3))               as qty,
               case when i % 2 = 0 then null
                    else cast('$marker-note-' || i as varchar) end as note,
               cast('2026-01-02 03:04:05' as timestamp)
                   + to_hours(cast(i as bigint))            as upd_ts
        from range(0, $rows) t(i)
        """.trimIndent()

    fun currentSetting(connection: Connection, name: String): String =
        scalar(connection, "select current_setting('$name')") { it.getString(1) }

    /**
     * DuckDB 1.1.3 echoes `memory_limit` back in binary display units - "488.2 MiB" for a
     * requested 512MB, measured on the pinned driver - so a readback cannot be string-compared
     * against what was set. Parsed to bytes instead, and asserted as a range, which is also
     * indifferent to whether the implementation writes MB or MiB into the SET.
     */
    fun settingBytes(setting: String): Double {
        val match = Regex("""^([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)$""").find(setting.trim())
            ?: error("memory_limit readback is not a number and a unit: '$setting'")
        val unit = when (match.groupValues[2].uppercase()) {
            "B", "BYTE", "BYTES" -> 1.0
            "KIB" -> 1024.0
            "MIB" -> 1024.0 * 1024.0
            "GIB" -> 1024.0 * 1024.0 * 1024.0
            "TIB" -> 1024.0 * 1024.0 * 1024.0 * 1024.0
            else -> error("unknown memory_limit unit in '$setting'")
        }
        return match.groupValues[1].toDouble() * unit
    }

    /** Separator-normalised, because DuckDB echoes `temp_directory` back exactly as it was set. */
    fun normalisePath(text: String): String = text.replace(File.separatorChar, '/')

    /**
     * `CREATE VIEW <name> AS SELECT ...`, as DuckDB 1.1.3 stores it.
     *
     * Read from `duckdb_views()` with `internal = false` rather than from
     * `information_schema.views`: the latter also lists the 130-odd views DuckDB defines for its
     * own catalog, which would drown any assertion about the views a run created.
     */
    fun viewDefinition(connection: Connection, view: String): String =
        scalar(
            connection,
            "select sql from duckdb_views() where internal = false and view_name = '$view'",
        ) { it.getString(1) }

    fun viewDefinitions(connection: Connection): List<String> =
        column(connection, "select sql from duckdb_views() where internal = false") { it.getString(1) }

    fun tableNames(connection: Connection): List<String> =
        column(connection, "select table_name from duckdb_tables() order by table_name") { it.getString(1) }

    /** duckdb_tables() carries a `temporary` flag; a temp table created dynamically shows here. */
    fun temporaryTableNames(connection: Connection): List<String> =
        column(
            connection,
            "select table_name from duckdb_tables() where temporary order by table_name",
        ) { it.getString(1) }

    fun rowCount(connection: Connection, relation: String): Long =
        scalar(connection, "select count(*) from $relation") { it.getLong(1) }

    /**
     * Column names, driver type names and every value rendered as text.
     *
     * Rendering through `getObject().toString()` rather than a typed accessor is deliberate: the
     * parquet parity item asks whether the *same* downstream SQL yields the *same* answer, so the
     * comparison must not be able to launder a type difference through a typed getter. Data-class
     * equality then makes one assertion cover names, types and values at once.
     */
    fun grid(connection: Connection, sql: String): Grid =
        connection.createStatement().use { statement ->
            statement.executeQuery(sql).use { rs ->
                val meta = rs.metaData
                val header = (1..meta.columnCount).map { meta.getColumnName(it) to meta.getColumnTypeName(it) }
                val rows = ArrayList<List<String?>>()
                while (rs.next()) {
                    rows.add((1..meta.columnCount).map { rs.getObject(it)?.toString() })
                }
                Grid(header, rows)
            }
        }

    data class Grid(val header: List<Pair<String, String>>, val rows: List<List<String?>>)

    private fun <T> scalar(connection: Connection, sql: String, read: (ResultSet) -> T): T =
        column(connection, sql, read).first()

    private fun <T> column(connection: Connection, sql: String, read: (ResultSet) -> T): List<T> =
        connection.createStatement().use { statement ->
            statement.executeQuery(sql).use { rs ->
                val out = ArrayList<T>()
                while (rs.next()) out.add(read(rs))
                out
            }
        }
}

/**
 * The "equivalent check" of P4's last done-when item. ArchUnit is not on the classpath and the
 * build rules forbid adding it, so the ban is enforced by scanning Kotlin source instead.
 *
 * The pattern is written so that this file does not match itself: the regex source spells the
 * gaps as `\s+` rather than as real whitespace, and the offending strings the check is proved
 * against are assembled at run time from separate words. That is why no directory needs to be
 * excluded from the scan - an exclusion would be the hole a ban like this usually leaks through.
 *
 * It matches a *statement*, not the words: the relation name must be followed by a column list or
 * by `as`. Prose that names the ban, and the diagnostic a production check raises when it catches
 * one, both mention the three words in order and neither is a statement, so a text scan that
 * stopped at the words would fail against source that is documenting the rule it enforces - and
 * the only ways out of that are an exclusion list or an unmentionable rule.
 */
object TempTableBan {

    val PATTERN = Regex(
        """create\s+(or\s+replace\s+)?((global|local)\s+)?temp(orary)?\s+table\s+("[^"]*"|[^\s;()]+)\s*([(]|as\b)""",
        RegexOption.IGNORE_CASE,
    )

    fun matches(text: String): Boolean = PATTERN.containsMatchIn(text)

    data class Offence(val file: String, val line: Int, val text: String)

    data class Scan(val filesScanned: Int, val offences: List<Offence>)

    /**
     * Every `.kt` file under [roots]. Each file is matched as one string rather than line by
     * line, because the pattern's gaps are `\s+` and a statement split across two source lines
     * would otherwise slip through; the line number is recovered from the match offset.
     */
    fun scan(roots: List<Path>): Scan {
        var files = 0
        val offences = ArrayList<Offence>()
        roots.forEach { root ->
            check(Files.isDirectory(root)) { "source root does not exist: ${root.toAbsolutePath()}" }
            Files.walk(root).use { walk ->
                walk.filter { it.isRegularFile() && it.extension == "kt" }.forEach { file ->
                    files++
                    val text = file.readText()
                    val name = root.relativize(file).toString().replace(File.separatorChar, '/')
                    PATTERN.findAll(text).forEach { match ->
                        val line = text.take(match.range.first).count { it == '\n' } + 1
                        offences.add(Offence(name, line, match.value.replace('\n', ' ')))
                    }
                }
            }
        }
        return Scan(files, offences)
    }
}
