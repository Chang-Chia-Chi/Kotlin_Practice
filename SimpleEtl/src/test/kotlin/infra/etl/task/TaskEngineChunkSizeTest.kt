package infra.etl.task

import infra.etl.DuckFile
import infra.etl.Etl
import infra.etl.RecordingConnections
import infra.etl.TaskHarness
import java.nio.file.Path
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * P5, done-when item 5: **`chunkSize` resolves step, then task, then default**.
 *
 * The observable is what reaches the pipe. The resolved chunk size becomes the source statement's
 * `fetchSize`, so the number is visible at the JDBC boundary without asking the engine to report
 * on itself - and P3's own suite already asserts `RowPipe` pushes it there, so a reading here is a
 * reading of the resolved value and nothing else.
 *
 * The recorder is P3's [RecordingConnections], reused rather than reimplemented: its handover
 * note asks the next phase to extend it rather than write a third counter, and nothing in that
 * frozen file is modified here. It records the *requested* fetch size, which is the right
 * reading - measured in P3, duckdb_jdbc 1.1.3 accepts `setFetchSize` and goes on reporting its
 * own 2048, so what the statement reports afterwards would say nothing about what the framework
 * asked for.
 *
 * Each task has exactly one pipe step and one source datasource, so the recorded list is one
 * entry long and `containsExactly` is meaningful.
 */
class TaskEngineChunkSizeTest {

    @TempDir
    lateinit var root: Path

    private val sourceSql = "select lot_id, lot_code, qty from wip"

    /**
     * Registers the source datasource twice: as a plain [DuckFile] so the test can create the
     * source table, and as `oracle_mes` behind the recorder that the pipe actually reads through.
     */
    private fun recordedSource(harness: TaskHarness): RecordingConnections {
        val backing = harness.datasource("backing")
        backing.createSourceTable("wip", rows = 12, marker = "w")
        val recording = RecordingConnections { backing.connection().duplicate() }
        harness.register("oracle_mes", Jdbi.create(recording))
        return recording
    }

    private fun task(stepChunkSize: Int?, taskChunkSize: Int?) = Etl.task(
        "wip-chunks",
        Etl.phase(
            "extract",
            Etl.pipe(
                name = "load-wip",
                sourceDatasource = "oracle_mes",
                sql = sourceSql,
                table = "wip_stg",
                chunkSize = stepChunkSize,
            ),
        ),
        chunkSize = taskChunkSize,
    )

    @Test
    fun aStepChunkSizeOverridesTheTaskDefault() {
        TaskHarness(root).use { harness ->
            val recording = recordedSource(harness)

            harness.runExpectingSuccess(task(stepChunkSize = 101, taskChunkSize = 202))

            assertEquals(listOf(101), recording.fetchSizesRequested) {
                "the step's own chunkSize reaches the source statement"
            }
        }
    }

    @Test
    fun theTaskChunkSizeAppliesWhenTheStepStatesNone() {
        TaskHarness(root).use { harness ->
            val recording = recordedSource(harness)

            harness.runExpectingSuccess(task(stepChunkSize = null, taskChunkSize = 202))

            assertEquals(listOf(202), recording.fetchSizesRequested) {
                "the task-level default reaches a step that states none"
            }
        }
    }

    /**
     * Neither level states one, so `TaskDefinition`'s own default of 5000 applies. This is why the
     * fixture omits the argument rather than passing 5000: passing it would test that an explicit
     * 5000 arrives, which is the first case again.
     */
    @Test
    fun theFrameworkDefaultAppliesWhenNeitherLevelStatesOne() {
        TaskHarness(root).use { harness ->
            val recording = recordedSource(harness)

            harness.runExpectingSuccess(task(stepChunkSize = null, taskChunkSize = null))

            assertEquals(listOf(5000), recording.fetchSizesRequested)
        }
    }
}
