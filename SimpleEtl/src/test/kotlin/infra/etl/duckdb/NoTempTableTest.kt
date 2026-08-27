package infra.etl.duckdb

import infra.etl.Scratchpad
import infra.etl.TempTableBan
import infra.etl.duckdb.DatasetNamer
import infra.etl.duckdb.ScratchDb
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir

/**
 * P4, done-when item 6: the ban on DuckDB temporary tables, spec 7.2.
 *
 * ArchUnit is not on the classpath and the phase rules forbid adding it, so this is the
 * "equivalent check": a scan of Kotlin source for the statement, plus a catalog check on a real
 * scratch database. The two catch different things. The scan reaches code no test exercises;
 * the catalog check reaches SQL assembled at run time, which no text scan can see.
 *
 * The scan is only worth having if it can fail, so [theCheckRejectsARealTemporaryTable] proves
 * the detector against strings it must reject and strings it must not. Asserting that today's
 * tree is clean, with no evidence the detector would notice if it were not, is the
 * leak-accounting-fixture problem in a different costume.
 *
 * Nothing in this file writes the banned statement literally. The strings under test are
 * assembled from separate words at run time, which is what lets the scan cover every source
 * file in the module with no exclusions - and an exclusion list is the usual hole in a ban
 * like this one.
 */
class NoTempTableTest {

    @TempDir
    lateinit var root: Path

    private val roots = listOf(Path.of("src", "main", "kotlin"), Path.of("src", "test", "kotlin"))

    private fun sql(vararg words: String) = words.joinToString(" ")

    @Test
    fun theCheckRejectsARealTemporaryTable() {
        val banned = listOf(
            sql("create", "temp", "table", "wip_stg__a1 (id bigint)"),
            sql("CREATE", "TEMPORARY", "TABLE", "wip_stg__a1 (id bigint)"),
            sql("Create", "Or", "Replace", "Temp", "Table", "t (id bigint)"),
            sql("create", "global", "temporary", "table", "t AS SELECT 1"),
            sql("create", "temp", "table", "\"quoted name\"(id bigint)"),
            listOf("create", "temp", "table", "t (id bigint)").joinToString("\n  "),
        )

        assertTrue(banned.all { TempTableBan.matches(it) }) {
            "the detector missed ${banned.filterNot { TempTableBan.matches(it) }}"
        }
    }

    /**
     * The other half of a check that can fail: one that fires on everything is no better than
     * one that fires on nothing.
     *
     * The last two entries are the reason the pattern matches a statement and not three words.
     * Production code that enforces this ban has to be able to name it - in a doc comment, and in
     * the diagnostic it raises when it catches one - and a scan that flagged those would leave
     * only two ways out, an exclusion list or a rule nobody is allowed to write down.
     */
    @Test
    fun theCheckAcceptsPermanentTablesAndProseThatMerelyMentionTheWord() {
        val allowed = listOf(
            sql("create", "table", "temp_stg (id bigint)"),
            sql("create", "or", "replace", "view", "temp_stg as select * from temp_stg__a1"),
            sql("create", "table", "t (temporary varchar)"),
            "nothing here creates a temporary table",
            "the standing ban on `" + sql("CREATE", "TEMP", "TABLE") + "` (spec 7.2)",
            sql("CREATE", "TEMP", "TABLE") + " is banned (spec 7.2): CHECKPOINT has no effect",
        )

        assertTrue(allowed.none { TempTableBan.matches(it) }) {
            "the detector fired on ${allowed.filter { TempTableBan.matches(it) }}"
        }
    }

    @Test
    fun noKotlinSourceInTheModuleCreatesATemporaryTable() {
        val scan = TempTableBan.scan(roots)

        assertAll(
            {
                assertTrue(scan.filesScanned > 10) {
                    "the scan found ${scan.filesScanned} Kotlin sources at $roots - it would pass vacuously"
                }
            },
            { assertTrue(scan.offences.isEmpty()) { "expected no offences, was ${scan.offences}" } },
        )
    }

    /**
     * The enforcement point, exercised rather than believed.
     *
     * [aScratchRunLeavesNoTemporaryRelationInTheCatalog] below asks a *fixture* query and gets
     * the right answer, which says nothing about whether production would notice - and the ban
     * is documented as enforced, so a reader trusts it. This test drives the production sweep
     * in `ScratchDb.close()` instead, and it is the only enforcement point that reaches SQL
     * assembled at run time, which the source scan cannot see by construction.
     *
     * This is the one place in the suite that creates the banned statement, and it creates it to
     * prove the ban bites. The statement is assembled from separate words so the source scan
     * still passes over this file, exactly as the detector proofs above are.
     *
     * Cleanup must come *before* the throw. Otherwise a run rejected for a temp table also leaks
     * its scratch file, and on DuckDB 1.1.3 that file is space nothing ever gets back (spec 5.5).
     */
    @Test
    fun closeRaisesWhenTheRunLeftATemporaryTableOnTheWriteConnection() {
        val scratch = ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(root))
        Scratchpad.exec(scratch.connection(), sql("create", "temp", "table", "leak_stg (id bigint)"))

        val thrown = assertThrows<IllegalStateException> { scratch.close() }
        assertTrue(thrown.message?.contains("leak_stg") == true) { "message was: ${thrown.message}" }

        assertAll(
            {
                val files = Scratchpad.regularFiles(root)
                assertTrue(files.isEmpty()) {
                    "the run was rejected, so it must not also leave its file behind; files were $files"
                }
            },
            {
                assertThrows<IllegalStateException>(
                    { "a closed scratch must refuse to reopen rather than create a second file" },
                ) { scratch.connection() }
            },
        )
    }

    /**
     * The same, on a duplicate. DuckDB's temporary catalog is per connection, so a sweep that
     * consulted only the write connection would report a clean run while the ban was being
     * broken on a reader - which is why `close()` asks every connection it issued.
     *
     * Do not extend this to the other half of `close()`'s contract, the "something under the
     * directory survived deletion" branch: that fires only where the OS refuses to delete an
     * open file. On Windows it is reachable, on Linux the delete succeeds and the test would
     * fail on CI.
     */
    @Test
    fun closeRaisesWhenTheTemporaryTableWasCreatedOnADuplicate() {
        val scratch = ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(root))
        Scratchpad.exec(scratch.duplicate(), sql("create", "temp", "table", "leak_dup (id bigint)"))

        val thrown = assertThrows<IllegalStateException> { scratch.close() }
        assertTrue(thrown.message?.contains("leak_dup") == true) { "message was: ${thrown.message}" }

        val files = Scratchpad.regularFiles(root)
        assertTrue(files.isEmpty()) { "expected no file left behind, was $files" }
    }

    /**
     * The run-time half. A scratch database exercised the way spec 5.5 says a task uses it -
     * attempt tables plus a stable view - must contain no temporary relation afterwards.
     *
     * This is the part that would catch a `CREATE ` + kind + ` TABLE` assembled from pieces,
     * which the source scan cannot see. It is asserted on the catalog rather than on the source
     * because DuckDB's `duckdb_tables()` carries a `temporary` flag, so the question has a
     * direct answer.
     */
    @Test
    fun aScratchRunLeavesNoTemporaryRelationInTheCatalog() {
        ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(root)).use { scratch ->
            val namer = DatasetNamer(root)
            val connection = scratch.connection()

            Scratchpad.createAttemptTable(connection, namer.physical("wip_stg", 1), "a1", rows = 3)
            Scratchpad.createAttemptTable(connection, namer.physical("wip_stg", 2), "a2", rows = 5)
            namer.publishTable(connection, "wip_stg", 2)

            assertAll(
                {
                    val tables = Scratchpad.tableNames(connection)
                    assertTrue(tables.isNotEmpty()) {
                        "the fixture created nothing, so the assertion below would be vacuous"
                    }
                },
                {
                    val temporary = Scratchpad.temporaryTableNames(connection)
                    assertTrue(temporary.isEmpty()) {
                        "the run left a temporary relation in the catalog: $temporary"
                    }
                },
            )
        }
    }
}
