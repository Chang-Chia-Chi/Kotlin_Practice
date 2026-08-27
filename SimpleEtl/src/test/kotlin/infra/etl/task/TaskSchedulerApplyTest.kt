package infra.etl.task

import infra.etl.P7Tasks
import infra.etl.P7World
import infra.etl.RecordingCron
import infra.etl.Trig
import infra.etl.task.TaskDefinition
import infra.etl.task.ValidationReport
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
 * Spec 8.1 and 8.5, and P7's first done-when item: `apply` registers exactly the enabled tasks
 * carrying a cron, unregisters removed ones, and re-registers only those whose cron changed.
 *
 * **All three directions are here on purpose.** An implementation that cancels every registration
 * and re-creates it on every `apply` produces exactly the right final state, so it satisfies both
 * "registers the right set" and "unregisters removed ones" while being the thing spec 8.5's
 * wording exists to forbid. Only [anUnchangedCronIsNeitherCancelledNorReRegistered] can tell them
 * apart, and it asserts on [RecordingCron.events] rather than on [RecordingCron.registered]
 * because the state is identical either way.
 *
 * The bad-cron pair is the same shape as P6's eighteen rule tests: the rejection
 * ([anUnparseableCronLeavesTheRegistryUnchangedAndYieldsAReport]) would pass against a
 * `TaskScheduler` that refused every definition it was handed, so it is paired with an
 * acceptance ([aBatchOfParseableCronsYieldsNoReportAndRegisters]) that would not.
 */
class TaskSchedulerApplyTest {

    @TempDir
    lateinit var root: Path

    private val world: P7World by lazy { P7World(root) }

    private val cron get() = world.cron

    @AfterEach
    fun tearDown() = world.close()

    private fun apply(vararg definitions: TaskDefinition): ValidationReport? =
        Trig.apply(world.scheduler, definitions.toList())

    @Test
    fun registersExactlyTheEnabledTasksThatCarryACron() {
        val report = apply(
            P7Tasks.scheduled("scheduled-a", cron = "0 */10 * * * ?"),
            P7Tasks.scheduled("scheduled-b", cron = "0 */5 * * * ?"),
            P7Tasks.scheduled("api-only", cron = null),
            P7Tasks.scheduled("disabled", cron = "0 0 * * * ?", enabled = false),
        )

        assertAll(
            { assertNull(report) { "a batch of valid crons was rejected: ${report?.errors}" } },
            {
                assertEquals(
                    mapOf("scheduled-a" to "0 */10 * * * ?", "scheduled-b" to "0 */5 * * * ?"),
                    cron.registered,
                )
            },
        )
    }

    @Test
    fun unregistersATaskThatIsNoLongerInTheDefinitions() {
        apply(
            P7Tasks.scheduled("kept", cron = "0 */10 * * * ?"),
            P7Tasks.scheduled("removed", cron = "0 */5 * * * ?"),
        )
        val mark = cron.mark()

        apply(P7Tasks.scheduled("kept", cron = "0 */10 * * * ?"))

        assertAll(
            { assertEquals(setOf("kept"), cron.registered.keys) },
            { assertEquals(listOf("cancel:removed"), cron.since(mark)) },
        )
    }

    @Test
    fun unregistersATaskThatHasBecomeDisabled() {
        apply(P7Tasks.scheduled("switchable", cron = "0 */10 * * * ?"))
        val mark = cron.mark()

        apply(P7Tasks.scheduled("switchable", cron = "0 */10 * * * ?", enabled = false))

        assertAll(
            { assertTrue(cron.registered.isEmpty()) { "still registered: ${cron.registered}" } },
            { assertEquals(listOf("cancel:switchable"), cron.since(mark)) },
        )
    }

    @Test
    fun unregistersATaskThatHasLostItsCron() {
        apply(P7Tasks.scheduled("was-scheduled", cron = "0 */10 * * * ?"))
        val mark = cron.mark()

        apply(P7Tasks.scheduled("was-scheduled", cron = null))

        assertAll(
            { assertTrue(cron.registered.isEmpty()) { "still registered: ${cron.registered}" } },
            { assertEquals(listOf("cancel:was-scheduled"), cron.since(mark)) },
        )
    }

    /**
     * The direction that rejects "cancel everything and re-register everything". The unchanged
     * task must be absent from the churn entirely - not merely present in the final state.
     */
    @Test
    fun anUnchangedCronIsNeitherCancelledNorReRegistered() {
        apply(
            P7Tasks.scheduled("steady", cron = "0 */10 * * * ?"),
            P7Tasks.scheduled("going", cron = "0 */5 * * * ?"),
        )
        val mark = cron.mark()

        apply(
            P7Tasks.scheduled("steady", cron = "0 */10 * * * ?"),
            P7Tasks.scheduled("arriving", cron = "0 0 * * * ?"),
        )

        assertAll(
            {
                assertEquals(
                    setOf("cancel:going", "schedule:arriving:0 0 * * * ?"),
                    cron.since(mark).toSet(),
                ) { "'steady' kept its cron, so nothing about it may appear in the churn" }
            },
            {
                assertEquals(
                    mapOf("steady" to "0 */10 * * * ?", "arriving" to "0 0 * * * ?"),
                    cron.registered,
                )
            },
        )
    }

    @Test
    fun reRegistersATaskWhoseCronChanged() {
        apply(P7Tasks.scheduled("moving", cron = "0 */10 * * * ?"))
        val mark = cron.mark()

        apply(P7Tasks.scheduled("moving", cron = "0 */2 * * * ?"))

        assertAll(
            { assertEquals(listOf("cancel:moving", "schedule:moving:0 */2 * * * ?"), cron.since(mark)) },
            { assertEquals(mapOf("moving" to "0 */2 * * * ?"), cron.registered) },
        )
    }

    /**
     * Spec 8.5: a bad cron is rejected atomically rather than taking the scheduler down. The
     * assertion that bites is not the report - it is that `sibling`, which is perfectly valid and
     * sits in the same batch, is **not** registered afterwards. A `TaskScheduler` that registered
     * as it went and let the throw escape would leave it live and the previous registration dead.
     */
    @Test
    fun anUnparseableCronLeavesTheRegistryUnchangedAndYieldsAReport() {
        apply(P7Tasks.scheduled("already-live", cron = "0 */10 * * * ?"))
        val before = cron.registered
        // Without this the "unchanged" assertion below would be satisfied by two empty maps.
        assertTrue(before.isNotEmpty()) { "nothing was registered to begin with, so 'unchanged' would be vacuous" }
        cron.rejectCron = { it == "not a cron" }

        val report = Trig.apply(
            world.scheduler,
            listOf(
                P7Tasks.scheduled("already-live", cron = "0 */10 * * * ?"),
                P7Tasks.scheduled("sibling", cron = "0 */5 * * * ?"),
                P7Tasks.scheduled("broken", cron = "not a cron"),
            ),
        )

        assertNotNull(report) { "a batch carrying an unparseable cron was accepted" }
        assertAll(
            { assertTrue(report!!.errors.isNotEmpty()) { "the report carries no errors" } },
            {
                val messages = report!!.errors.map { it.message }.joinToString("\n")
                assertTrue("broken" in messages) {
                    "the report has to say which task's cron is unusable; messages were: $messages"
                }
            },
            {
                assertEquals(before, cron.registered) {
                    "nothing changes: neither the new sibling nor the broken task"
                }
            },
        )
    }

    /**
     * The rollback branch, which the test above never builds.
     *
     * There it holds an *unchanged* cron, so `cancelled` is empty and the restore loop never runs -
     * the case survived deleting the restore line. Here the batch changes `steady`'s cron **and**
     * carries a bad one, so `steady` is cancelled before `broken` throws. Without the restore, the
     * report is still returned and the registry is still free of the new expression, yet `steady`
     * has silently lost its schedule: a live task stops firing because of an edit to a different
     * task, and nothing says so.
     */
    @Test
    fun aRejectedApplyRestoresTheRegistrationItHadAlreadyCancelled() {
        apply(P7Tasks.scheduled("steady", cron = "0 */10 * * * ?"))
        cron.rejectCron = { it == "not a cron" }

        val report = apply(
            P7Tasks.scheduled("steady", cron = "0 */2 * * * ?"),
            P7Tasks.scheduled("broken", cron = "not a cron"),
        )

        assertAll(
            { assertNotNull(report) { "a batch carrying an unparseable cron was accepted" } },
            {
                assertEquals(mapOf("steady" to "0 */10 * * * ?"), cron.registered) {
                    "'steady' was cancelled to make way for its new cron and must be put back"
                }
            },
        )
    }

    /**
     * The acceptance half of the pair. Without it, every bad-cron assertion above is satisfied by
     * a `TaskScheduler` that reports an error for every definition it is ever handed.
     */
    @Test
    fun aBatchOfParseableCronsYieldsNoReportAndRegisters() {
        cron.rejectCron = { it == "not a cron" }

        val report = apply(
            P7Tasks.scheduled("good-a", cron = "0 */10 * * * ?"),
            P7Tasks.scheduled("good-b", cron = "0 */5 * * * ?"),
        )

        assertAll(
            { assertNull(report) { "a batch of parseable crons was rejected: ${report?.errors}" } },
            { assertEquals(setOf("good-a", "good-b"), cron.registered.keys) },
        )
    }
}
