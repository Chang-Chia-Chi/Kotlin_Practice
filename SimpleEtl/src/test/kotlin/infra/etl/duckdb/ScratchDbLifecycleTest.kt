package infra.etl.duckdb

import infra.etl.Scratchpad
import infra.etl.duckdb.ScratchDb
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * P4, done-when items 1 and 2: lazy file creation, and the two settings spec 7.2 requires at
 * open.
 *
 * Everything here runs against a real DuckDB 1.1.3 file-mode database. Nothing is asserted from
 * an internal flag: laziness is read off the filesystem and the settings are read back out of
 * the engine with `current_setting`, because a `SET` that was issued and a `SET` that took
 * effect are not the same claim.
 */
class ScratchDbLifecycleTest {

    @TempDir
    lateinit var root: Path

    /**
     * Done-when 1. The negative half - "a task shape that never references scratch leaves no
     * file on disk" - is only worth anything next to the positive half, or it would also pass
     * against a ScratchDb that never creates a file at all. So the same test opens a connection
     * afterwards and requires a file to appear.
     */
    @Test
    fun fileIsCreatedLazily_untouchedScratchLeavesNothingOnDisk_firstConnectionCreatesTheFile() {
        val spill = Scratchpad.spillDir(root)
        val db = ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, spill)

        assertThat(Scratchpad.regularFiles(root))
            .describedAs("shape A of spec 2.4 never references scratch, so nothing may be created")
            .isEmpty()

        db.use { scratch ->
            scratch.connection()
            assertThat(Scratchpad.regularFiles(root))
                .describedAs("the first connection() must create the file, or the negative half above is vacuous")
                .isNotEmpty()
        }
    }

    /**
     * The same shape as above, but the run also closes. A task that touches nothing must not
     * fail at run end, and must still leave no file behind.
     */
    @Test
    fun closeWithoutEverConnecting_isSilentAndLeavesNothingOnDisk() {
        ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(root)).use { }

        assertThat(Scratchpad.regularFiles(root)).isEmpty()
    }

    /**
     * Done-when 2, first half. DuckDB 1.1.3 echoes `memory_limit` back in binary display units
     * ("488.2 MiB" for a requested 512MB, measured on the pinned driver), so the readback is
     * compared as a byte range rather than as a string. The range is wide enough to accept
     * either a decimal-MB or a binary-MiB reading of the configured 512, and far too narrow to
     * accept DuckDB's default, which is about 80% of machine RAM.
     */
    @Test
    fun memoryLimitIsAppliedAtOpen_readBackFromCurrentSetting() {
        ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(root)).use { scratch ->
            val setting = Scratchpad.currentSetting(scratch.connection(), "memory_limit")

            assertThat(Scratchpad.settingBytes(setting))
                .describedAs("memory_limit read back as '%s'; DuckDB's default is a large fraction of machine RAM", setting)
                .isBetween(400.0 * 1024 * 1024, 600.0 * 1024 * 1024)
        }
    }

    /**
     * Done-when 2, second half. The configured directory is a sibling of the database file, so
     * this fails against an implementation that leaves `temp_directory` unset: DuckDB 1.1.3 then
     * reports `<dbfile>.tmp` instead. Spec 7.2 records that an unset value does not make a join
     * fail - it puts spill somewhere uncounted - which is why the assertion is on the value and
     * not on a query outcome.
     */
    @Test
    fun tempDirectoryIsAppliedAtOpen_readBackFromCurrentSetting() {
        val spill = Scratchpad.spillDir(root)

        ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, spill).use { scratch ->
            val setting = Scratchpad.currentSetting(scratch.connection(), "temp_directory")

            assertThat(Scratchpad.normalisePath(setting))
                .isEqualTo(Scratchpad.normalisePath(spill.toAbsolutePath().toString()))
        }
    }

    /**
     * Spec 7.2: additional connections come from `duplicate()`, which shares the instance, and
     * `memory_limit` is database level so it is not multiplied per connection. Both halves are
     * asserted - the duplicate sees a table the write connection created, and reports the same
     * two settings - because a `duplicate()` that quietly opened a second instance would satisfy
     * neither, and would double the memory budget without any test noticing.
     */
    @Test
    fun duplicateSharesTheInstanceAndItsSettings() {
        val spill = Scratchpad.spillDir(root)

        ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, spill).use { scratch ->
            val write = scratch.connection()
            Scratchpad.createAttemptTable(write, "wip_stg__a1", "a1", rows = 4)

            val read = scratch.duplicate()
            try {
                assertThat(read).isNotSameAs(write)
                assertThat(Scratchpad.rowCount(read, "wip_stg__a1")).isEqualTo(4L)
                assertThat(Scratchpad.currentSetting(read, "temp_directory"))
                    .isEqualTo(Scratchpad.currentSetting(write, "temp_directory"))
                assertThat(Scratchpad.currentSetting(read, "memory_limit"))
                    .isEqualTo(Scratchpad.currentSetting(write, "memory_limit"))
            } finally {
                read.close()
            }
        }
    }
}
