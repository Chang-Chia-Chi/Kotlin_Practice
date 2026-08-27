package infra.etl.task

import infra.etl.Etl
import infra.etl.EventTrace
import infra.etl.ListenerCall
import infra.etl.TaskHarness
import infra.etl.task.Outcome
import infra.etl.task.TaskDefinition
import infra.etl.task.TaskRunListener
import infra.etl.taskContext
import java.nio.file.Path
import java.sql.SQLTransientException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

/**
 * P8a, contract 4 items 11 and 12: **a listener that throws is the listener's problem.**
 *
 * Spec 9.2's seam exists so the host's own logging can plug in. A logging plug-in that fails an
 * ETL run inverts the point of it, so every one of the seven call sites catches, logs and
 * continues - and `TaskRunListener.of` applies the same isolation *per listener*, so a thrower at
 * position 1 does not rob positions 2..n of the event.
 *
 * ### The two traps this file is written around
 *
 * 1. **"The run survived" is not evidence of isolation.** An engine that never calls the listener
 *    at all passes that assertion at every call site. So each case also asserts that the site
 *    under test actually threw ([infra.etl.RecordingListener.thrown]) and that all seven sites
 *    fired in order afterwards ([infra.etl.RecordingListener.calls]) - which is what separates
 *    "caught and continued" from "caught and gave up on the listener".
 * 2. **A test of the ENGINE's isolation must attach the thrower directly** (contract 2.5).
 *    Attaching it through `of` would prove the composite's own catch and call it the engine's, and
 *    the engine's catch - which is not redundant, because a host may attach a bare listener -
 *    would be untested and would look tested.
 *
 * ### One scenario reaches all seven sites
 *
 * A pipe step whose source fails once transiently and then succeeds produces `onTaskStart`,
 * `onPhaseStart`, `onStepStart`, `onStepError`, `onStepEnd`, `onPhaseEnd`, `onTaskEnd` - each
 * exactly once - and still ends SUCCEEDED. That last part is what makes "the outcome did not
 * change" a real assertion rather than a restatement of the failure that was already there.
 */
class TaskListenerIsolationTest {

    @TempDir
    lateinit var root: Path

    /** The seven sites, in the order this scenario produces them. */
    private val allSites = listOf(
        ListenerCall.TASK_START,
        ListenerCall.PHASE_START,
        ListenerCall.STEP_START,
        ListenerCall.STEP_ERROR,
        ListenerCall.STEP_END,
        ListenerCall.PHASE_END,
        ListenerCall.TASK_END,
    )

    private fun retryingTask(): TaskDefinition = Etl.task(
        "wip-isolation",
        Etl.phase(
            "extract",
            Etl.pipe(
                name = "load-wip",
                sourceDatasource = "oracle_mes",
                sql = "select lot_id, lot_code, qty from wip",
                table = "wip_stg",
                retries = 1,
            ),
        ),
    )

    /** A harness whose source fails its first execution transiently and succeeds afterwards. */
    private fun harnessFailingOnce(directory: String): TaskHarness =
        TaskHarness(root.resolve(directory)).apply {
            datasource("oracle_mes").apply {
                createSourceTable("wip", rows = 6, marker = "w")
                failFirst(count = 1, afterRows = 2) { SQLTransientException("probe: transient") }
            }
        }

    /** Contract 4.11, once per call site of spec 9.2. */
    @ParameterizedTest(name = "{0}")
    @EnumSource(ListenerCall::class)
    fun aThrowingListenerNeverChangesTheOutcomeAtAnyCallSite(site: ListenerCall) {
        harnessFailingOnce(site.name.lowercase()).use { harness ->
            // Attached directly, never through `of` - see the class KDoc.
            val thrower = EventTrace().listener(failAt = setOf(site))
            harness.listener = thrower

            val outcome = harness.run(retryingTask())

            assertThat(outcome.outcome)
                .describedAs("a listener that throws from %s must not fail the run", site)
                .isEqualTo(Outcome.SUCCEEDED)
            assertThat(thrower.thrown)
                .describedAs("the site under test actually fired; an engine that never calls one cannot fail this")
                .isEqualTo(1)
            assertThat(thrower.calls)
                .describedAs("the engine kept calling the listener after it threw")
                .containsExactlyElementsOf(allSites)
        }
    }

    /**
     * Contract 4.12, the part that needs no engine: fan-out happens in argument order, and a
     * thrower at position 1 does not stop positions 2 and 3 receiving the event.
     *
     * Asserting only that the call returned would pass against a fan-out that aborts on the first
     * throw, so the assertion is on what the later listeners recorded, in order, in one shared
     * trace.
     */
    @Test
    fun ofFansOutInArgumentOrderAndIsolatesEachListenerFromTheOthers() {
        val trace = EventTrace()
        val first = trace.listener(label = "first", failAt = setOf(ListenerCall.TASK_START))
        val second = trace.listener(label = "second")
        val third = trace.listener(label = "third")

        TaskRunListener.of(first, second, third).onTaskStart(taskContext())

        assertThat(trace.entries).containsExactly(
            "first:onTaskStart(wip-summary)",
            "second:onTaskStart(wip-summary)",
            "third:onTaskStart(wip-summary)",
        )
        assertThat(first.thrown).describedAs("the thrower really threw").isEqualTo(1)
        assertThat(TaskRunListener.of()).describedAs("of() is the no-op").isSameAs(TaskRunListener.NONE)
        assertThat(TaskRunListener.of(second)).describedAs("of(one) is that one, unwrapped").isSameAs(second)
    }

    /**
     * The other half of contract 4.12, through a real run: all seven methods fan out, not just the
     * one asserted directly above. A composite that forwarded `onTaskStart` and quietly dropped
     * `onStepError` would pass the test above and fail this one.
     */
    @Test
    fun aCompositeDeliversEveryCallSiteToTheListenersBehindAThrower() {
        harnessFailingOnce("composite").use { harness ->
            val trace = EventTrace()
            val thrower = trace.listener(label = "thrower", failAt = ListenerCall.entries.toSet())
            val downstream = trace.listener(label = "downstream")
            harness.listener = TaskRunListener.of(thrower, downstream)

            val outcome = harness.run(retryingTask())

            assertThat(outcome.outcome).isEqualTo(Outcome.SUCCEEDED)
            assertThat(thrower.thrown).describedAs("it threw from every site it was given").isEqualTo(allSites.size)
            assertThat(downstream.calls)
                .describedAs("every event still reached the listener behind it")
                .containsExactlyElementsOf(allSites)
            assertThat(trace.entries.first())
                .describedAs("argument order, so the thrower is the one that goes first")
                .isEqualTo("thrower:onTaskStart(wip-isolation)")
        }
    }
}
