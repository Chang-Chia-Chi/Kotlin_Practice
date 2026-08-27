package infra.etl

import infra.etl.task.Outcome
import infra.etl.task.TriggerResult
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * Spec 8.2's framework surface: the sealed result the host's `AdminResource` maps to
 * 202 / 409 / 404 / 400, and the asynchrony that keeps a 30 minute run out of an HTTP request.
 * No HTTP is involved, and none is simulated.
 *
 * Each of the three rejections is paired with an acceptance in the same test. A `TaskAdmin` that
 * answered `Unknown` to everything would satisfy the unknown-task assertion, one that answered
 * `Disabled` to everything would satisfy the disabled assertion, and neither would survive its
 * partner assertion two lines below - the same pairing discipline P6 needed for its eighteen
 * deliberately broken files.
 *
 * The `etl-admin` role check is deliberately absent: spec 8.6 makes it the host's obligation and
 * records that it is not tested in this repository.
 */
class TaskAdminTriggerTest {

    @TempDir
    lateinit var root: Path

    private val world: P7World by lazy { P7World(root) }

    @AfterEach
    fun tearDown() = world.close()

    /**
     * Done-when: `trigger` returns `Accepted(runId)` while the run is parked, so a run outliving
     * an HTTP timeout is never held open.
     *
     * The load-bearing assertion is the last one: at the moment the run is demonstrably still in
     * progress - parked inside its own step, on its own thread - the caller already has its
     * runId back and the run has no outcome yet.
     */
    @Test
    fun triggerReturnsAcceptedWhileTheRunIsStillInProgress() {
        val probe = world.probe("probe_ds")
        val definition = P7Tasks.parking("wip-summary", "probe_ds")
        val admin = world.admin(definition)

        val runId = Trig.acceptedRunId(admin.trigger("wip-summary", "ops"))
        probe.awaitEntry()

        assertThat(runId).isNotBlank()
        assertThat(admin.run("wip-summary", runId))
            .describedAs("the trigger returned before the run reached an outcome")
            .isNull()

        probe.release()
        val outcome = Trig.awaitSucceeded(admin, "wip-summary", runId)

        // The runId has to discriminate, or `Trig.awaitFinished` - which every wait in this phase
        // routes through - would be satisfied by any finished run of the same task.
        assertThat(outcome.runId).isEqualTo(runId)
        assertThat(admin.run("wip-summary", "not-a-run-id")).isNull()
    }

    /**
     * The `Error` path, and the phase's only FAILED run through the admin surface. `TaskEngine.run`
     * deliberately does not catch `Error`, so a `cacheCopy` step's `NotImplementedError` escapes
     * the coroutine entirely and is recorded only by the runner's completion handler. Without that
     * branch the run stays `running == true` for the life of the process and its task can never be
     * triggered again - and nothing else in this suite would notice, because everything else waits
     * on a success.
     */
    @Test
    fun aRunThatDiesOnAnErrorIsRecordedFailedAndNoLongerRunning() {
        val admin = world.admin(
            Etl.task("cache-read", Etl.phase("copy", Etl.cacheCopy("copy-out", "wip_cache", "select 1", "wip"))),
        )

        val runId = Trig.acceptedRunId(admin.trigger("cache-read", "ops"))
        val outcome = Trig.awaitFinished(admin, "cache-read", runId)

        assertThat(outcome.outcome).isEqualTo(Outcome.FAILED)
        assertThat(outcome.failure).isInstanceOf(NotImplementedError::class.java)
        assertThat(admin.list().single { it.name == "cache-read" }.running)
            .describedAs("a run killed by an Error must not be left looking in flight")
            .isFalse()
        assertThat(admin.trigger("cache-read", "ops"))
            .describedAs("and its task must be triggerable again")
            .isInstanceOf(TriggerResult.Accepted::class.java)
    }

    /**
     * Spec 8.2's listing - "tasks, schedules, last run outcome". Not a done-when item, but it is
     * public surface, and P3's lesson was that a public form with no test ships a real defect
     * while everything else is green. `running` is the field that carries weight: `TaskOutcome`
     * has only SUCCEEDED and FAILED, so it is the only way the listing can say "in flight".
     */
    @Test
    fun theListingCarriesEachTasksScheduleAndWhetherItIsRunning() {
        val probe = world.probe("probe_ds")
        val admin = world.admin(
            P7Tasks.parking("wip-summary", "probe_ds", cron = "0 */10 * * * ?"),
            P7Tasks.parking("api-only", "probe_ds", enabled = false),
        )

        assertThat(admin.list().map { Triple(it.name, it.enabled, it.cron) })
            .containsExactlyInAnyOrder(
                Triple("wip-summary", true, "0 */10 * * * ?"),
                Triple("api-only", false, null),
            )
        assertThat(admin.list().map { it.running }).containsOnly(false)

        val runId = Trig.acceptedRunId(admin.trigger("wip-summary", "ops"))
        probe.awaitEntry()
        assertThat(admin.list().single { it.name == "wip-summary" }.running)
            .describedAs("the run is parked inside its own step right now")
            .isTrue()

        probe.release()
        Trig.awaitSucceeded(admin, "wip-summary", runId)
        assertThat(admin.list().single { it.name == "wip-summary" }.running).isFalse()
    }

    @Test
    fun anUnknownTaskIsUnknownAndAKnownTaskIsAccepted() {
        val probe = world.probe("probe_ds")
        probe.parking = false
        val admin = world.admin(P7Tasks.parking("wip-summary", "probe_ds"))

        assertThat(admin.trigger("no-such-task", "ops")).isEqualTo(TriggerResult.Unknown)
        assertThat(admin.trigger("wip-summary", "ops")).isInstanceOf(TriggerResult.Accepted::class.java)
    }

    @Test
    fun aDisabledTaskIsDisabledAndAnEnabledSiblingIsAccepted() {
        val probe = world.probe("probe_ds")
        probe.parking = false
        val admin = world.admin(
            P7Tasks.parking("switched-off", "probe_ds", enabled = false),
            P7Tasks.parking("switched-on", "probe_ds"),
        )

        assertThat(admin.trigger("switched-off", "ops")).isEqualTo(TriggerResult.Disabled)
        assertThat(admin.trigger("switched-on", "ops")).isInstanceOf(TriggerResult.Accepted::class.java)
    }

    /**
     * Spec 8.2: `TaskAdmin.trigger` takes the caller identity as a parameter and performs no
     * authorisation of its own. Three identities that any authorising implementation would have
     * to discriminate between - absent, named, and empty - and all three are accepted.
     */
    @Test
    fun everyCallerIdentityIsAcceptedBecauseTaskAdminAuthorisesNothing() {
        val probe = world.probe("probe_ds")
        probe.parking = false
        val admin = world.admin(P7Tasks.parking("wip-summary", "probe_ds"))

        listOf(null, "alice", "").forEach { identity ->
            val runId = Trig.awaitAccepted(admin, "wip-summary", identity)
            Trig.awaitSucceeded(admin, "wip-summary", runId)
        }

        assertThat(probe.threads)
            .describedAs("all three identities produced a real run")
            .hasSize(3)
    }
}
