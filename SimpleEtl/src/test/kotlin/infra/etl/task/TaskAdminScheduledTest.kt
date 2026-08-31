package infra.etl.task

import infra.etl.P7Tasks
import infra.etl.P7World
import infra.etl.Trig
import java.nio.file.Path
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir

/**
 * E14: `TaskStatus.scheduled`, the observable form of the host obligation "call
 * `TaskScheduler.apply` yourself when you build definitions in code".
 *
 * That obligation exists because `TaskAdmin` and `TaskScheduler` hold a definition map each, which
 * E11 declined to unify. Its stated symptom is `list()` reporting every task and not one of them
 * ever firing, *with no error raised* - a permanent silent disagreement, and the only host
 * obligation whose evidence is already inside this process. `scheduled` is that evidence, and this
 * file is the pair of tests that would fail if it were hard-wired either way.
 *
 * **The oracle is deliberately `cron` versus `scheduled` on one row.** Asserting `scheduled` alone
 * against a fixed expectation is satisfied by a constant; asserting the *disagreement* is not,
 * because [scheduledIsTrueOnceTheHostCallsApply] builds the identical definition and differs only
 * in whether `apply` was called.
 *
 * These definitions never run: `P7Tasks.scheduled` exists to be diffed, so nothing here needs a
 * datasource, an engine or a released probe.
 */
class TaskAdminScheduledTest {

    @TempDir
    lateinit var root: Path

    private val world: P7World by lazy { P7World(root) }

    @AfterEach
    fun tearDown() = world.close()

    private fun statusOf(admin: TaskAdmin, name: String): TaskStatus =
        admin.list().singleOrNull { it.name == name }
            ?: error("task '$name' is not in the listing; listed: ${admin.list().map { it.name }}")

    /**
     * The forgotten-`apply` case. The host handed `TaskAdmin` a task carrying a cron and never
     * registered it, so the listing must not claim a schedule that does not exist.
     */
    @Test
    fun aDefinitionWithACronThatWasNeverRegisteredIsListedAsNotScheduled() {
        val admin = world.admin(P7Tasks.scheduled("nightly-roll", cron = "0 0 2 * * ?"))

        val status = statusOf(admin, "nightly-roll")

        assertAll(
            { assertEquals("0 0 2 * * ?", status.cron) { "the definition's own cron is still reported" } },
            {
                assertFalse(status.scheduled) {
                    "the host never called TaskScheduler.apply, so nothing will ever fire this task - " +
                        "the listing must not report it as scheduled (spec 8.6)"
                }
            },
            { assertTrue(world.cron.registered.isEmpty()) { "nothing should have registered itself" } },
        )
    }

    /**
     * The conforming host, same definition. Without this the test above passes for a `scheduled`
     * hard-wired to false.
     */
    @Test
    fun scheduledIsTrueOnceTheHostCallsApply() {
        val definition = P7Tasks.scheduled("nightly-roll", cron = "0 0 2 * * ?")
        val admin = world.admin(definition)
        assertNull(Trig.apply(world.scheduler, listOf(definition))) { "the starting schedule was rejected" }

        val status = statusOf(admin, "nightly-roll")

        assertAll(
            { assertEquals("0 0 2 * * ?", status.cron) },
            { assertTrue(status.scheduled) { "the cron is registered, so the listing must say so" } },
        )
    }

    /**
     * An API-only task is the normal case of `cron == null`, and must not read as the failure
     * above. Nothing registers a task with no expression, so `scheduled` is false here for a
     * reason that is not a host mistake - which is why the obligation's symptom is stated as a
     * *non-null* cron with `scheduled = false`.
     */
    @Test
    fun aTaskWithNoCronIsNotScheduledAndThatIsNotADisagreement() {
        val definition = P7Tasks.scheduled("on-demand", cron = null)
        val admin = world.admin(definition)
        assertNull(Trig.apply(world.scheduler, listOf(definition)))

        val status = statusOf(admin, "on-demand")

        assertAll(
            { assertNull(status.cron) },
            { assertFalse(status.scheduled) },
        )
    }

    /**
     * The mirror direction: the host called `apply` and did *not* pass the same set to `TaskAdmin`.
     * Such a task cannot appear in the listing at all - `list()` iterates the definitions it holds
     * - so there is no `TaskStatus` field that could carry it and `list()` logs a WARN instead.
     *
     * What is asserted is the input that WARN is computed from, not the log line. A test that
     * installed an appender would be asserting on JBoss Logging; `sameDatasourcePipeUsers` is
     * split out of `reportPoolMinimums` for exactly this reason, and the same choice is made here.
     */
    @Test
    fun aRegistrationWithNoDefinitionIsInvisibleToTheListingAndIsWhatTheWarningReports() {
        val admin = world.admin(P7Tasks.scheduled("known", cron = "0 0 2 * * ?"))
        Trig.apply(
            world.scheduler,
            listOf(
                P7Tasks.scheduled("known", cron = "0 0 2 * * ?"),
                P7Tasks.scheduled("stranger", cron = "0 0 3 * * ?"),
            ),
        )

        assertAll(
            {
                assertEquals(listOf("known"), admin.list().map { it.name }) {
                    "the listing can only report definitions TaskAdmin holds"
                }
            },
            {
                assertEquals(setOf("known", "stranger"), world.scheduler.registeredNames()) {
                    "both crons really are live, so the disagreement is real and not a setup error"
                }
            },
            {
                assertEquals(
                    setOf("stranger"),
                    world.scheduler.registeredNames() - admin.list().map { it.name }.toSet(),
                ) { "'stranger' fires into nothing and no listing row can say so - hence the WARN" }
            },
        )
    }
}
