package infra.etl.duckdb

import infra.etl.Scratchpad
import infra.etl.duckdb.ScratchDb
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir

/**
 * P4, done-when items 1 and 2: lazy file creation, and the two settings a scratch database
 * requires at open - `memory_limit` and `temp_directory`.
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

        val untouched = Scratchpad.regularFiles(root)
        assertTrue(untouched.isEmpty()) {
            "shape A of spec 2.4 never references scratch, so nothing may be created; files were $untouched"
        }

        db.use { scratch ->
            scratch.connection()
            val connected = Scratchpad.regularFiles(root)
            assertTrue(connected.isNotEmpty()) {
                "the first connection() must create the file, or the negative half above is vacuous"
            }
        }
    }

    /**
     * The same shape as above, but the run also closes. A task that touches nothing must not
     * fail at run end, and must still leave no file behind.
     */
    @Test
    fun closeWithoutEverConnecting_isSilentAndLeavesNothingOnDisk() {
        ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(root)).use { }

        val files = Scratchpad.regularFiles(root)
        assertTrue(files.isEmpty()) { "expected nothing on disk, was $files" }
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

            val bytes = Scratchpad.settingBytes(setting)
            assertTrue(bytes in (400.0 * 1024 * 1024)..(600.0 * 1024 * 1024)) {
                "memory_limit read back as '$setting' ($bytes bytes); DuckDB's default is a large " +
                    "fraction of machine RAM"
            }
        }
    }

    /**
     * Done-when 2, second half. The configured directory is a sibling of the database file, so
     * this fails against an implementation that leaves `temp_directory` unset: DuckDB 1.1.3 then
     * reports `<dbfile>.tmp` instead. An unset value does not make a join fail - it puts spill
     * somewhere uncounted - which is why the assertion is on the value and not on a query
     * outcome.
     */
    @Test
    fun tempDirectoryIsAppliedAtOpen_readBackFromCurrentSetting() {
        val spill = Scratchpad.spillDir(root)

        ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, spill).use { scratch ->
            val setting = Scratchpad.currentSetting(scratch.connection(), "temp_directory")

            assertEquals(
                Scratchpad.normalisePath(spill.toAbsolutePath().toString()),
                Scratchpad.normalisePath(setting),
            )
        }
    }

    /**
     * Additional connections come from `duplicate()`, which shares the instance, and
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
                assertAll(
                    { assertNotSame(write, read) },
                    { assertEquals(4L, Scratchpad.rowCount(read, "wip_stg__a1")) },
                    {
                        assertEquals(
                            Scratchpad.currentSetting(write, "temp_directory"),
                            Scratchpad.currentSetting(read, "temp_directory"),
                        )
                    },
                    {
                        assertEquals(
                            Scratchpad.currentSetting(write, "memory_limit"),
                            Scratchpad.currentSetting(read, "memory_limit"),
                        )
                    },
                )
            } finally {
                read.close()
            }
        }
    }
}
