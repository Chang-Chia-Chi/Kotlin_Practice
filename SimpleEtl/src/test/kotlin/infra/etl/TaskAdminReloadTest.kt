package infra.etl

import infra.etl.task.TaskDefinition
import infra.etl.task.TriggerResult
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * Spec 8.5: reload is atomic, and a running task keeps the definition it started with.
 *
 * The reload oracle is [RecordingCron.registered] rather than `TaskAdmin.list()`, because the
 * schedule registry is the one observable spec 11.2 actually pins - `TaskStatus`'s shape is the
 * engineer's. It is also the stronger oracle for the atomicity item: it changes only if the new
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
        assertThat(Trig.apply(world.scheduler, listOf(definition))).isNull()
    }

    @Test
    fun reloadWithOneInvalidFileChangesNothingAndReturnsTheErrors() {
        world.probe("probe_ds")
        val original = P7Tasks.parking("wip-summary", "probe_ds", cron = "0 */10 * * * ?")
        val admin = world.admin(original)
        startingSchedule(original)
        val before = world.cron.registered
        // Without this the "unchanged" assertion below would be satisfied by two empty maps.
        assertThat(before).isNotEmpty()

        val directory = P7Tasks.directory(
            root.resolve("tasks-invalid"),
            "a-good.yaml" to P7Tasks.yaml("loaded-a", cron = "0 */5 * * * ?"),
            "b-bad.yaml" to P7Tasks.yaml("loaded-b", cron = "0 0 * * * ?", datasource = "not_configured"),
        )

        val report = admin.reload(directory)

        assertThat(report).isNotNull()
        assertThat(report!!.errors.map { it.file })
            .describedAs("the report names the file that failed and no phantom errors for the other")
            .containsOnly("b-bad.yaml")
        assertThat(world.cron.registered)
            .describedAs("a bad edit cannot take the scheduler down (spec 8.5)")
            .containsExactlyInAnyOrderEntriesOf(before)
        assertThat(admin.trigger("loaded-a", "ops"))
            .describedAs("the valid file in the rejected batch was not applied either")
            .isEqualTo(TriggerResult.Unknown)
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

        assertThat(report).isNull()
        assertThat(world.cron.registered).containsExactlyInAnyOrderEntriesOf(
            mapOf("loaded-a" to "0 */5 * * * ?", "loaded-b" to "0 0 * * * ?"),
        )
        assertThat(admin.trigger("no-longer-there", "ops")).isEqualTo(TriggerResult.Unknown)
    }

    /**
     * The other half of "atomic". Every file here loads cleanly, so the loader passes it; the
     * *scheduler* is what refuses, because one cron is shaped legally but is unparseable - which
     * is exactly the split spec 8.6 names, "make `CronScheduler.schedule` throw on an unparseable
     * cron, or 8.5's atomic reload silently accepts a bad cron".
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
        assertThat(before).isNotEmpty()
        world.cron.rejectCron = { it == "0 0 30 * * ?" }

        val directory = P7Tasks.directory(
            root.resolve("tasks-cron-rejected"),
            "a-good.yaml" to P7Tasks.yaml("loaded-a", cron = "0 */5 * * * ?"),
            "b-unparseable.yaml" to P7Tasks.yaml("loaded-b", cron = "0 0 30 * * ?"),
        )

        val report = admin.reload(directory)

        assertThat(report).isNotNull()
        assertThat(report!!.errors.map { it.message }.joinToString("\n")).contains("loaded-b")
        assertThat(world.cron.registered).containsExactlyInAnyOrderEntriesOf(before)
        assertThat(admin.trigger("loaded-a", "ops"))
            .describedAs("the definition set must not be swapped when the scheduler refused")
            .isEqualTo(TriggerResult.Unknown)
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
        assertThat(admin.reload(directory)).isNull()
        assertThat(world.cron.since(mark))
            .describedAs("the cron did not change, so the registration is kept as it stands")
            .isEmpty()

        world.cron.fire("wip-summary")
        val runId = Trig.awaitFinishedRun(admin, "wip-summary").runId

        assertThat(probe.threads)
            .describedAs("the firing ran the reloaded definition, which never reaches the probe")
            .isEmpty()
        Trig.awaitSucceeded(admin, "wip-summary", runId)
    }

    /**
     * Spec 8.5: the definition is captured at run start and never swapped mid-run.
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

        assertThat(report).isNull()
        assertThat(world.cron.registered)
            .describedAs("the reload committed while the run was still parked")
            .containsExactlyEntriesOf(mapOf("wip-summary" to "0 */2 * * * ?"))

        probe.release()
        Trig.awaitSucceeded(admin, "wip-summary", runId)

        assertThat(probe.threads)
            .describedAs("the run finished the definition it started with, exactly once")
            .hasSize(1)
    }
}
