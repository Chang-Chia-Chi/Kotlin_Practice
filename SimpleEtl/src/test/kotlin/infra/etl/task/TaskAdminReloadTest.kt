package infra.etl.task

import infra.etl.P7Tasks
import infra.etl.P7World
import infra.etl.RecordingCron
import infra.etl.Trig
import infra.etl.task.TaskDefinition
import infra.etl.task.TriggerResult
import java.nio.file.Path
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir

/**
 * Reload is atomic, and a running task keeps the definition it started with.
 *
 * The reload oracle is [RecordingCron.registered] rather than `TaskAdmin.list()`, because the
 * schedule registry is the one observable the frozen public surface pins - `TaskStatus`'s shape is
 * the engineer's. It is also the stronger oracle for the atomicity item: it changes only if the new
 * definitions were committed, so "nothing changed" and "everything changed" are the same
 * assertion read two ways.
 *
 * The rejection ([reloadWithOneInvalidFileChangesNothingAndReturnsTheErrors]) is paired with an
 * acceptance ([reloadWithEveryFileValidReturnsNoReportAndAppliesTheNewSet]) built from the same
 * two files with a single edit between them - the invalid file names an unconfigured datasource
 * and is valid in every other respect, so it is one rule away from loading. Without the pair, a
 * `TaskAdmin` that rejected every directory it was handed would pass the rejection half.
 */
class TaskAdminReloadTest {

    @TempDir
    lateinit var root: Path

    private val world: P7World by lazy { P7World(root) }

    @AfterEach
    fun tearDown() = world.close()

    private fun startingSchedule(definition: TaskDefinition) {
        val rejected = Trig.apply(world.scheduler, listOf(definition))
        assertNull(rejected) { "the starting schedule was rejected: ${rejected?.errors}" }
    }

    @Test
    fun reloadWithOneInvalidFileChangesNothingAndReturnsTheErrors() {
        world.probe("probe_ds")
        val original = P7Tasks.parking("wip-summary", "probe_ds", cron = "0 */10 * * * ?")
        val admin = world.admin(original)
        startingSchedule(original)
        val before = world.cron.registered
        // Without this the "unchanged" assertion below would be satisfied by two empty maps.
        assertTrue(before.isNotEmpty()) { "nothing was registered to begin with, so 'unchanged' would be vacuous" }

        val directory = P7Tasks.directory(
            root.resolve("tasks-invalid"),
            "a-good.yaml" to P7Tasks.yaml("loaded-a", cron = "0 */5 * * * ?"),
            "b-bad.yaml" to P7Tasks.yaml("loaded-b", cron = "0 0 * * * ?", datasource = "not_configured"),
        )

        val report = admin.reload(directory)

        assertNotNull(report) { "the batch carrying an invalid file was accepted" }
        assertAll(
            {
                assertEquals(setOf("b-bad.yaml"), report!!.errors.map { it.file }.toSet()) {
                    "the report names the file that failed and no phantom errors for the other"
                }
            },
            {
                assertEquals(before, world.cron.registered) {
                    "a bad edit cannot take the scheduler down (spec 8.5)"
                }
            },
            {
                assertEquals(TriggerResult.Unknown, admin.trigger("loaded-a", "ops")) {
                    "the valid file in the rejected batch was not applied either"
                }
            },
        )
    }

    @Test
    fun reloadWithEveryFileValidReturnsNoReportAndAppliesTheNewSet() {
        world.probe("probe_ds")
        val original = P7Tasks.parking("wip-summary", "probe_ds", cron = "0 */10 * * * ?")
        val admin = world.admin(original)
        startingSchedule(original)

        val directory = P7Tasks.directory(
            root.resolve("tasks-valid"),
            "a-good.yaml" to P7Tasks.yaml("loaded-a", cron = "0 */5 * * * ?"),
            "b-good.yaml" to P7Tasks.yaml("loaded-b", cron = "0 0 * * * ?"),
        )

        val report = admin.reload(directory)

        assertAll(
            { assertNull(report) { "an entirely valid batch was rejected: ${report?.errors}" } },
            {
                assertEquals(
                    mapOf("loaded-a" to "0 */5 * * * ?", "loaded-b" to "0 0 * * * ?"),
                    world.cron.registered,
                )
            },
            { assertEquals(TriggerResult.Unknown, admin.trigger("no-longer-there", "ops")) },
        )
    }

    /**
     * The other half of "atomic". Every file here loads cleanly, so the loader passes it; the
     * *scheduler* is what refuses, because one cron is shaped legally but is unparseable - which
     * is exactly the host obligation: `CronScheduler.schedule` must throw on an unparseable cron,
     * or the atomic reload silently accepts a bad one.
     *
     * The third assertion is the one the loader-rejection test cannot make: the definition set
     * must not be swapped either. Dropping `reload`'s `if (rejected != null) return rejected`
     * still returns a report to the caller and still leaves the schedules alone, and a suite that
     * only checked those two would stay green while a task nobody can schedule became triggerable.
     */
    @Test
    fun aReloadTheSchedulerRejectsChangesNeitherTheScheduleNorTheDefinitions() {
        world.probe("probe_ds")
        val original = P7Tasks.parking("wip-summary", "probe_ds", cron = "0 */10 * * * ?")
        val admin = world.admin(original)
        startingSchedule(original)
        val before = world.cron.registered
        assertTrue(before.isNotEmpty()) { "nothing was registered to begin with, so 'unchanged' would be vacuous" }
        world.cron.rejectCron = { it == "0 0 30 * * ?" }

        val directory = P7Tasks.directory(
            root.resolve("tasks-cron-rejected"),
            "a-good.yaml" to P7Tasks.yaml("loaded-a", cron = "0 */5 * * * ?"),
            "b-unparseable.yaml" to P7Tasks.yaml("loaded-b", cron = "0 0 30 * * ?"),
        )

        val report = admin.reload(directory)

        assertNotNull(report) { "the batch carrying an unparseable cron was accepted" }
        assertAll(
            {
                val messages = report!!.errors.map { it.message }.joinToString("\n")
                assertTrue("loaded-b" in messages) { "no error named loaded-b; messages were: $messages" }
            },
            { assertEquals(before, world.cron.registered) },
            {
                assertEquals(TriggerResult.Unknown, admin.trigger("loaded-a", "ops")) {
                    "the definition set must not be swapped when the scheduler refused"
                }
            },
        )
    }

    /**
     * Suggestion from review: a reload that changes a task's *steps* but not its cron keeps the
     * registration, and the callback must then fire the **new** definition. `TaskScheduler.fire`
     * looks the name up in `current` rather than capturing the definition at registration time;
     * replacing that lookup with a capture passes every other test in this phase.
     *
     * The new definition is a scratch CTAS and never touches `probe_ds`, so a captured old
     * definition would park at the probe and be visible as an entry that must not exist.
     */
    @Test
    fun aScheduledFiringAfterAReloadUsesTheNewDefinitionEvenWhenTheCronIsUnchanged() {
        val probe = world.probe("probe_ds")
        val original = P7Tasks.parking("wip-summary", "probe_ds", cron = "0 */10 * * * ?")
        val admin = world.admin(original)
        startingSchedule(original)
        val mark = world.cron.mark()

        val directory = P7Tasks.directory(
            root.resolve("tasks-same-cron"),
            "wip.yaml" to P7Tasks.yaml("wip-summary", cron = "0 */10 * * * ?"),
        )
        val report = admin.reload(directory)
        assertNull(report) { "the reload was rejected: ${report?.errors}" }
        assertTrue(world.cron.since(mark).isEmpty()) {
            "the cron did not change, so the registration is kept as it stands; churn was " +
                "${world.cron.since(mark)}"
        }

        world.cron.fire("wip-summary")
        val runId = Trig.awaitFinishedRun(admin, "wip-summary").runId

        assertTrue(probe.threads.isEmpty()) {
            "the firing ran the reloaded definition, which never reaches the probe; probe threads " +
                "were ${probe.threads}"
        }
        Trig.awaitSucceeded(admin, "wip-summary", runId)
    }

    /**
     * The definition is captured at run start and never swapped mid-run.
     *
     * The reload replaces `wip-summary` with a version that has a different cron and does not
     * mention the probe datasource at all. The parked run is sitting inside the probe, which only
     * the *old* definition reaches, so a run that had picked up the new definition could not
     * finish from where it is. The cron assertion is what stops the test being vacuous: it proves
     * the reload really did commit while the run was in flight.
     */
    @Test
    fun aReloadDuringARunDoesNotAffectThatRun() {
        val probe = world.probe("probe_ds")
        val original = P7Tasks.parking("wip-summary", "probe_ds", cron = "0 */10 * * * ?")
        val admin = world.admin(original)
        startingSchedule(original)

        val runId = Trig.acceptedRunId(admin.trigger("wip-summary", "ops"))
        probe.awaitEntry()

        val directory = P7Tasks.directory(
            root.resolve("tasks-swap"),
            "wip.yaml" to P7Tasks.yaml("wip-summary", cron = "0 */2 * * * ?"),
        )
        val report = admin.reload(directory)

        assertAll(
            { assertNull(report) { "the reload was rejected: ${report?.errors}" } },
            {
                assertEquals(mapOf("wip-summary" to "0 */2 * * * ?"), world.cron.registered) {
                    "the reload committed while the run was still parked"
                }
            },
        )

        probe.release()
        Trig.awaitSucceeded(admin, "wip-summary", runId)

        assertEquals(1, probe.threads.size) {
            "the run finished the definition it started with, exactly once; probe threads were " +
                "${probe.threads}"
        }
    }
}
