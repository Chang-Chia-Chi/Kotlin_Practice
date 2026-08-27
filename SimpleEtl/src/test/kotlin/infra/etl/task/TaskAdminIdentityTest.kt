package infra.etl.task

import infra.etl.P7Tasks
import infra.etl.P7World
import infra.etl.Trig
import infra.etl.task.TaskAdmin
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * Spec 8.2: "`TaskAdmin.trigger` takes the caller identity as a parameter, for
 * `TaskContext.triggeredBy`, and performs no authorisation of its own."
 *
 * ### Why this is a separate file
 *
 * The second half of that sentence is tested without any assumed shape, in
 * `TaskAdminTriggerTest.everyCallerIdentityIsAcceptedBecauseTaskAdminAuthorisesNothing`. The
 * first half - that the identity is *recorded into the run* - has no observable on any frozen
 * surface at P7:
 *
 * - `TaskOutcome` is frozen as `(runId, outcome, failure)`. No identity.
 * - `TaskEngine.run(definition, trigger)` is frozen with no identity parameter at all, so the
 *   value cannot reach the run through the seam spec 11.2 declares.
 * - `TaskContext.triggeredBy`, which spec 8.2 names as the destination, is spec 11.3 and P8's.
 * - `TaskStatus`'s shape is explicitly the engineer's to choose.
 *
 * So this file assumes one, in [triggeredBy] and nowhere else, and is kept apart so a different
 * choice costs this file alone. The two candidates are a field on `TaskStatus`'s last-run record
 * and a widened run record reachable from `TaskAdmin`; redirect the one expression.
 *
 * The assertions themselves are shape-independent and are what matter: **three triggers with
 * three identities are recorded distinctly**. A recorder that stored a constant, stored the task
 * name, or stored nothing at all fails all three - which is the pairing discipline P6 needed,
 * applied to a value rather than to a rejection.
 */
class TaskAdminIdentityTest {

    @TempDir
    lateinit var root: Path

    private val world: P7World by lazy { P7World(root) }

    @AfterEach
    fun tearDown() = world.close()

    /** INTEGRATE: the only assumed shape in this file. */
    private fun triggeredBy(admin: TaskAdmin, task: String): String? =
        admin.list().single { it.name == task }.lastRun?.triggeredBy

    @Test
    fun theCallerIdentityOfEachTriggerIsRecordedIntoTheRun() {
        val probe = world.probe("probe_ds")
        probe.parking = false
        val admin = world.admin(P7Tasks.parking("wip-summary", "probe_ds"))

        val recorded = listOf("alice", "bob", null).map { identity ->
            val runId = Trig.awaitAccepted(admin, "wip-summary", identity)
            Trig.awaitSucceeded(admin, "wip-summary", runId)
            triggeredBy(admin, "wip-summary")
        }

        assertThat(recorded)
            .describedAs("each trigger's own identity, not a constant and not the task name")
            .containsExactly("alice", "bob", null)
    }
}
