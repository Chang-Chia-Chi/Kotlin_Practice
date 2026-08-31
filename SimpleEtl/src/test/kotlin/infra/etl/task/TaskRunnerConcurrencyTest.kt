package infra.etl.task

import infra.etl.P7Tasks
import infra.etl.P7World
import infra.etl.Trig
import infra.etl.task.TriggerResult
import java.nio.file.Path
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir

/**
 * Confinement to one worker per task, rejection rather than queueing, and
 * different tasks running at the same time.
 *
 * **Nothing here reads a thread name.** The measurement was `DefaultDispatcher-worker-1
 * @wip-summary#1` with assertions on and `DefaultDispatcher-worker-2` with them off; surefire
 * enables `-ea` and production does not, so an assertion on the `@name` suffix would derive its
 * discriminating power from a JVM flag - which is P4's Windows-file-lock finding wearing a
 * different hat. Thread *identity* is compared with `isSameAs`, which does not depend on any
 * flag, and the coroutine name is asserted where the runner hands it over
 * (`TaskRunnerCoroutineNameTest`), not where a debug agent happens to print it.
 *
 * **Rejection is two assertions, and the second is the one that bites.** A
 * `limitedParallelism(1)` dispatcher serialises, so an implementation that accepted the second
 * trigger and let the dispatcher queue it would return *something* and run the task twice, late.
 * Every rejection test here therefore also proves the rejected trigger never ran afterwards, by
 * counting probe arrivals across the whole test rather than looking for the absence of an event
 * inside a window.
 *
 * Every rejection is paired with an acceptance, because a `TaskRunner` that rejected every
 * trigger it was ever handed would pass the rejection halves on their own.
 */
class TaskRunnerConcurrencyTest {

    @TempDir
    lateinit var root: Path

    private val world: P7World by lazy { P7World(root) }

    @AfterEach
    fun tearDown() = world.close()

    /**
     * Done-when: two runs of one task observe the same single worker thread, neither of which is
     * the triggering thread.
     */
    @Test
    fun twoRunsOfOneTaskShareOneWorkerThreadAndNeitherIsTheTriggeringThread() {
        val probe = world.probe("probe_ds")
        probe.parking = false
        val definition = P7Tasks.parking("wip-summary", "probe_ds")
        val admin = world.admin(definition)
        val triggering = Thread.currentThread()

        val first = Trig.acceptedRunId(admin.trigger("wip-summary", "ops"))
        val firstThread = probe.awaitEntry()
        Trig.awaitSucceeded(admin, "wip-summary", first)

        val second = Trig.awaitAccepted(admin, "wip-summary", "ops")
        val secondThread = probe.awaitEntry()
        Trig.awaitSucceeded(admin, "wip-summary", second)

        assertAll(
            {
                assertNotSame(triggering, firstThread) {
                    "the run must not execute inline on the triggering thread (spec 8.1)"
                }
            },
            { assertNotSame(triggering, secondThread) },
            {
                assertSame(firstThread, secondThread) {
                    "one task is confined to one worker (spec 8.3); they were $firstThread and $secondThread"
                }
            },
        )
    }

    /** Done-when: a second `TaskAdmin.trigger` during a run is rejected, not queued. */
    @Test
    fun aSecondApiTriggerDuringARunIsRejectedAndNeverRunsAfterwards() {
        val probe = world.probe("probe_ds")
        val definition = P7Tasks.parking("wip-summary", "probe_ds")
        val admin = world.admin(definition)

        val first = Trig.acceptedRunId(admin.trigger("wip-summary", "ops"))
        probe.awaitEntry()

        val rejected = admin.trigger("wip-summary", "ops")

        assertEquals(TriggerResult.AlreadyRunning, rejected)

        probe.release()
        Trig.awaitSucceeded(admin, "wip-summary", first)

        // The acceptance half, and simultaneously the proof that the rejected trigger was not
        // queued: a queued run would be parked at the probe right now and this would never accept.
        val third = Trig.awaitAccepted(admin, "wip-summary", "ops")
        probe.awaitEntry()
        assertEquals(2, probe.threads.size) {
            "the rejected trigger must never have reached the task body; probe threads were ${probe.threads}"
        }

        probe.release()
        Trig.awaitSucceeded(admin, "wip-summary", third)
    }

    /**
     * The same rejection from the other direction: the `CronScheduler` callback. Its `run: () ->
     * Unit` signature hands the result nowhere, so "skipped, not queued" is only
     * visible as the run that never happened.
     */
    @Test
    fun aSecondScheduledFiringDuringARunIsSkippedAndNeverRunsAfterwards() {
        val probe = world.probe("probe_ds")
        val definition = P7Tasks.parking("wip-summary", "probe_ds", cron = "0 */10 * * * ?")
        val admin = world.admin(definition)
        val rejectedSchedule = Trig.apply(world.scheduler, listOf(definition))
        assertNull(rejectedSchedule) { "the schedule was rejected: ${rejectedSchedule?.errors}" }

        world.cron.fire("wip-summary")
        probe.awaitEntry()

        world.cron.fire("wip-summary")
        assertEquals(TriggerResult.AlreadyRunning, admin.trigger("wip-summary", "ops")) {
            "a run started by the schedule blocks an API trigger too (spec 8.4)"
        }

        probe.release()

        val next = Trig.awaitAccepted(admin, "wip-summary", "ops")
        probe.awaitEntry()
        assertEquals(2, probe.threads.size) {
            "the skipped firing must never have accumulated into a backlog; probe threads were ${probe.threads}"
        }

        probe.release()
        Trig.awaitSucceeded(admin, "wip-summary", next)
    }

    /**
     * Different tasks may run concurrently, each with its own dispatcher and its own
     * scratch file. This is the first point in the project at which two `ScratchDb` files are
     * open at the same time, which is why both tasks materialise into scratch before they park.
     */
    @Test
    fun differentTasksRunConcurrentlyEachWithItsOwnScratchFile() {
        val probeA = world.probe("probe_a")
        val probeB = world.probe("probe_b")
        val taskA = P7Tasks.parking("task-a", "probe_a", touchScratch = true)
        val taskB = P7Tasks.parking("task-b", "probe_b", touchScratch = true)
        val admin = world.admin(taskA, taskB)

        val runA = Trig.acceptedRunId(admin.trigger("task-a", "ops"))
        val threadA = probeA.awaitEntry()
        val runB = Trig.acceptedRunId(admin.trigger("task-b", "ops"))
        val threadB = probeB.awaitEntry()

        assertNotSame(threadA, threadB) {
            "two tasks are two dispatchers, so they do not share a worker (spec 8.3); both were $threadA"
        }
        // By runId, not by count: this is the only phase-visible tie between the `Accepted(runId)`
        // a caller was handed and the run's own scratch directory, and it is the whole reason
        // P5's shipped `TaskEngine.run` was widened to take the runId. A count of two passes
        // against an engine that names the directory from a fresh UUID of its own.
        assertEquals(setOf(runA, runB), world.liveScratchFiles().toSet()) {
            "one DuckDB file per run, both live at once, each named for its runId (spec 7.2)"
        }

        probeA.release()
        probeB.release()
        Trig.awaitSucceeded(admin, "task-a", runA)
        Trig.awaitSucceeded(admin, "task-b", runB)

        assertTrue(world.liveScratchFiles().isEmpty()) {
            "each run deletes its own file at run end (spec 7.2); live files were ${world.liveScratchFiles()}"
        }
    }
}
