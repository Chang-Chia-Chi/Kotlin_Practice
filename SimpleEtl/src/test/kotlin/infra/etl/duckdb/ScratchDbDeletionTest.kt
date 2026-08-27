package infra.etl.duckdb

import infra.etl.Scratchpad
import infra.etl.duckdb.ScratchDb
import java.nio.file.Path
import java.sql.Connection
import java.sql.SQLException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * P4, done-when item 3: the file is deleted on success, on failure, and on an exception thrown
 * from inside the run block.
 *
 * Three tests rather than one, because the three paths are three different pieces of production
 * control flow: the normal end of a run, a step that failed and was handled, and a throw that
 * unwinds past the run block. An implementation that deletes only in the first case passes a
 * single happy-path test and loses a scratch file per failed run in production - and spec 7.2
 * names close-and-delete as the *only* reliable reclamation point in DuckDB 1.1.3, so a leaked
 * file is not tidiness, it is disk that never comes back.
 *
 * Each test asserts the file existed mid-run before asserting it is gone. Without that, all
 * three would pass against a ScratchDb that never created a file.
 */
class ScratchDbDeletionTest {

    @TempDir
    lateinit var root: Path

    @Test
    fun fileIsDeletedOnSuccess() {
        ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(root)).use { scratch ->
            Scratchpad.createAttemptTable(scratch.connection(), "wip_stg__a1", "a1", rows = 8)
            assertThat(Scratchpad.regularFiles(root)).isNotEmpty()
        }

        assertThat(Scratchpad.regularFiles(root))
            .describedAs("scratch artefacts left behind after a successful run")
            .isEmpty()
    }

    /**
     * A step failed, the framework handled it, and the run finished normally. The failure is a
     * real driver error - a query against a relation that was never created - rather than a
     * thrown marker, so the connection has genuinely seen an error before close.
     */
    @Test
    fun fileIsDeletedAfterAHandledStepFailure() {
        var failure: String? = null

        ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(root)).use { scratch ->
            val connection = scratch.connection()
            Scratchpad.createAttemptTable(connection, "wip_stg__a1", "a1", rows = 8)
            assertThat(Scratchpad.regularFiles(root)).isNotEmpty()

            val thrown = runCatching { Scratchpad.rowCount(connection, "no_such_dataset") }.exceptionOrNull()
            failure = (thrown as SQLException?)?.message
        }

        assertThat(failure).describedAs("the fixture's own failure injection did not fire").isNotNull()
        assertThat(Scratchpad.regularFiles(root))
            .describedAs("scratch artefacts left behind after a failed step")
            .isEmpty()
    }

    /**
     * The exception unwinds out of the run block. Both halves matter: the caller must still see
     * the original failure - a cleanup that swallowed it would turn a failed run into a silent
     * one - and the file must be gone anyway.
     */
    @Test
    fun fileIsDeletedWhenTheRunBlockThrows_andTheFailurePropagates() {
        val scratch = ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(root))

        assertThatThrownBy {
            scratch.use {
                Scratchpad.createAttemptTable(it.connection(), "wip_stg__a1", "a1", rows = 8)
                assertThat(Scratchpad.regularFiles(root)).isNotEmpty()
                throw IllegalStateException("step 'build-summary' blew up")
            }
        }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("build-summary")

        assertThat(Scratchpad.regularFiles(root))
            .describedAs("scratch artefacts left behind after an exception from inside the run block")
            .isEmpty()
    }

    /**
     * A duplicate the run forgot to close must not keep the file alive. Spec 7.2 hands out
     * duplicates for concurrent reads, and on Windows a file cannot be deleted while any handle
     * into it is open - so "close the write connection and delete" is not enough, and the failure
     * would be a scratch file per run surviving on the volume with nothing pointing at it.
     *
     * The duplicate is deliberately left open, which is what separates this from the duplicate
     * test in [ScratchDbLifecycleTest]: there it is closed in a `finally`, the way a careful
     * caller would.
     */
    @Test
    fun fileIsDeletedEvenWhenTheRunLeavesADuplicateOpen() {
        lateinit var leaked: Connection

        ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(root)).use { scratch ->
            Scratchpad.createAttemptTable(scratch.connection(), "wip_stg__a1", "a1", rows = 4)
            leaked = scratch.duplicate()
            assertThat(Scratchpad.rowCount(leaked, "wip_stg__a1")).isEqualTo(4L)
            assertThat(Scratchpad.regularFiles(root)).isNotEmpty()
        }

        assertThat(leaked.isClosed())
            .describedAs(
                "the duplicate the run forgot must be closed by close(). File absence cannot " +
                    "answer this on its own: every measurement behind this suite is from Windows, " +
                    "where an open handle blocks the delete, but CI is Linux, where the file " +
                    "unlinks happily while the handle is open - so on CI the assertion below " +
                    "passes against an implementation that never closes a duplicate.",
            )
            .isTrue()
        assertThat(Scratchpad.regularFiles(root))
            .describedAs("an unclosed duplicate kept the scratch file alive past the end of the run")
            .isEmpty()
    }

    /**
     * The directory handed to ScratchDb belongs to the caller. Deleting the file is the run's
     * job; deleting the caller's directory is not, and a per-run subdirectory left empty is
     * harmless either way - which is why the assertions above count regular files only.
     */
    @Test
    fun theCallerSuppliedDirectorySurvivesTheRun() {
        ScratchDb(root, Scratchpad.MEMORY_LIMIT_MB, Scratchpad.spillDir(root)).use { scratch ->
            Scratchpad.createAttemptTable(scratch.connection(), "wip_stg__a1", "a1", rows = 2)
        }

        assertThat(root).exists().isDirectory()
    }
}
