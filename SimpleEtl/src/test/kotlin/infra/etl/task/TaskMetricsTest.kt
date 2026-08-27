package infra.etl.task

import infra.etl.DuckFile
import infra.etl.Etl
import infra.etl.EventTrace
import infra.etl.ListenerCall
import infra.etl.MetricsCall
import infra.etl.TaskHarness
import java.nio.file.Path
import java.sql.SQLTransientException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

/**
 * P8b, acceptance criteria 2, 6, 7 and 8: **what the engine tells spec 9.3's metrics seam, and
 * when.** Nothing here knows what Micrometer is; that binding is asserted in
 * `infra.etl.micrometer.MetricLabelContractTest`, and the split is the same one the package
 * boundary makes - the engine talks to `TaskMetrics`, and only a host names a `MeterRegistry`.
 *
 * ### Metrics and listener calls share one trace
 *
 * Every ordering clause of contract 3.2 is a *relative* one, so both seams and the hooks write
 * into a single [EventTrace] and every ordering assertion is `assertEquals` over the whole trace
 * of a run - P8a's rule, for P8a's reason: a subsequence assertion is satisfied by an empty list
 * and would pass unchanged against an engine with no metrics call sites in it at all.
 *
 * ### Which orderings carry a failure mode
 *
 * Two of the four do, and both are in the traces below:
 *
 * - **`scratchBytes` before `ScratchDb.close()`**, hence before any hook and before `taskEnded`.
 *   Sampling after the close reads an emptied directory, so the gauge is 0 on every run forever,
 *   silently, and an operator sizing spec 7.2's volume sees a flat zero. The `bytes > 0`
 *   assertion on the successful run is what catches it; "a run with no scratch reports 0" is the
 *   *symptom* of that bug and would pass while it stood.
 * - **`stepRetried` before `onStepError(willRetry = true)`**, and every metric before the
 *   listener call describing the same moment, because `Events.isolate` catches `Exception` and
 *   not `Throwable` (`P8aCoverageTest` pins that). A listener throwing an `Error` therefore
 *   escapes the engine, and a metric sequenced after it would be lost on exactly the run an
 *   operator most needs it.
 *
 * The first trace also falsifies progress.md's fifth carried finding at no extra cost: the hook
 * appears *after* the scratch sample, so hooks did not move inside the `use`.
 *
 * ### Nothing here sleeps
 *
 * `TaskHarness`'s sleeper advances the injected clock by exactly the backoff the engine asked
 * for, so `taskEnded`'s `durationMs` is an exact number - 6000 for a run with 2s and 4s of
 * backoff in it - rather than a stopwatch reading. Against a wall clock that assertion could only
 * have been a range, and a range does not separate an engine that timed the run from one that
 * timed the last attempt.
 */
class TaskMetricsTest {

    @TempDir
    lateinit var root: Path

    /** The four metrics sites, in the order a successful-with-retry run produces them. */
    private val allMetricSites = listOf(
        MetricsCall.STEP_RETRIED,
        MetricsCall.STEP_ENDED,
        MetricsCall.SCRATCH_BYTES,
        MetricsCall.TASK_ENDED,
    )

    /** The seven listener sites, in the order that same run produces them. */
    private val allListenerSites = listOf(
        ListenerCall.TASK_START,
        ListenerCall.PHASE_START,
        ListenerCall.STEP_START,
        ListenerCall.STEP_ERROR,
        ListenerCall.STEP_END,
        ListenerCall.PHASE_END,
        ListenerCall.TASK_END,
    )

    private fun source(harness: TaskHarness, rows: Int = 6): DuckFile =
        harness.datasource("oracle_mes").also { it.createSourceTable("wip", rows = rows, marker = "w") }

    /** A scratch-targeted pipe: the one step type that moves rows, and the one that opens scratch. */
    private fun loadWip(retries: Int? = null) = Etl.pipe(
        name = "load-wip",
        sourceDatasource = "oracle_mes",
        sql = "select lot_id, lot_code, qty from wip",
        table = "wip_stg",
        retries = retries,
    )

    // ------------------------------------------------------------------------------------
    // Criterion 2: logging: false suppresses the listener and nothing else.
    // ------------------------------------------------------------------------------------

    /**
     * Spec 9.3 is explicit that `logging: false` does not suppress metrics, and P8a implements
     * the flag by binding the run's listener `sink` to `TaskRunListener.NONE` inside `Events` -
     * so the whole clause reduces to "metrics must not travel through that sink".
     *
     * Asserted as a pair on one harness, because "metrics still fired under `logging: false`"
     * alone passes against an engine that ignores the flag entirely. The loud run fixes what a
     * complete trace looks like; the quiet run then has to lose exactly the listener half of it.
     * The second run also attaches a *new* recorder to both seams, which is what proves the
     * harness hands the engine forwarders rather than whatever it held when the engine was first
     * built - without that, the quiet recorder would read empty and this test would pass for the
     * wrong reason.
     */
    @Test
    fun loggingFalseSuppressesEveryListenerCallAndNoMetricAtAll() {
        TaskHarness(root).use { harness ->
            source(harness)
            val trace = EventTrace()
            val definition = Etl.task("wip-metrics", Etl.phase("extract", loadWip()))

            harness.listener = trace.listener()
            harness.metrics = trace.metrics()
            harness.runExpectingSuccess(Etl.withLogging(definition, logging = true))

            assertEquals(
                listOf(
                    "onTaskStart(wip-metrics)",
                    "onPhaseStart(extract)",
                    "onStepStart(extract/load-wip)",
                    "metric.stepEnded(extract/load-wip, attempt=1, read=6, written=6)",
                    "onStepEnd(extract/load-wip, attempt=1)",
                    "onPhaseEnd(extract, SUCCEEDED)",
                    "metric.scratchBytes(wip-metrics)",
                    "metric.taskEnded(wip-metrics, SUCCEEDED)",
                    "onTaskEnd(wip-metrics, SUCCEEDED)",
                ),
                trace.entries,
            ) { "logging: true - both seams report" }

            trace.clear()
            val quietListener = trace.listener()
            val quietMetrics = trace.metrics()
            harness.listener = quietListener
            harness.metrics = quietMetrics
            harness.runExpectingSuccess(Etl.withLogging(definition, logging = false))

            assertAll(
                {
                    assertTrue(quietListener.calls.isEmpty()) {
                        "logging: false suppresses every listener call site; calls were ${quietListener.calls}"
                    }
                },
                {
                    assertEquals(
                        listOf(
                            "metric.stepEnded(extract/load-wip, attempt=1, read=6, written=6)",
                            "metric.scratchBytes(wip-metrics)",
                            "metric.taskEnded(wip-metrics, SUCCEEDED)",
                        ),
                        trace.entries,
                    ) { "metrics do not travel through the listener sink that the flag binds to NONE" }
                },
                {
                    assertEquals(
                        listOf(MetricsCall.STEP_ENDED, MetricsCall.SCRATCH_BYTES, MetricsCall.TASK_ENDED),
                        quietMetrics.calls,
                    ) { "and they reached the recorder attached after the engine was built" }
                },
            )
        }
    }

    // ------------------------------------------------------------------------------------
    // Criterion 7, run 1: the ordered trace of a successful run with a retry.
    // ------------------------------------------------------------------------------------

    /**
     * Contract 3.2, in full, on the run that produces every one of its four clauses:
     * `stepRetried` before each `onStepError(willRetry = true)`, `stepEnded` before `onStepEnd`,
     * `scratchBytes` after the last `onPhaseEnd` and before the `onSuccess` hook, and `taskEnded`
     * before `onTaskEnd`.
     *
     * `stepRetried` fires once per retried **attempt**, not once per retried step, so a step with
     * `retries: 2` that failed twice emits exactly two - which is what makes
     * `etl_step_retries_total` a count of retried attempts. An implementation that emitted one per
     * step would leave a trace one line shorter and go red here.
     *
     * `durationMs` on `taskEnded` is read in `run`'s own `finally`, so it spans the hooks and
     * `ScratchDb.close()` as well as the engine's work. The hook here advances the injected clock
     * by 500ms itself, which is the only way that clause is falsifiable at all: with a clock the
     * sleeper alone moves, "includes the hooks" and "engine work only" are the same number. So the
     * expected value is 6500 - 2s and 4s of requested backoff, plus the hook - and a duration read
     * before the hooks ran reports 6000 instead. A wall clock answers something small and
     * arbitrary; `Instant.now()` answers something enormous.
     */
    @Test
    fun everyMetricIsOrderedBeforeTheListenerCallForTheSameMoment() {
        TaskHarness(root).use { harness ->
            val mes = source(harness)
            mes.failFirst(count = 2, afterRows = 2) { SQLTransientException("probe: transient") }
            val trace = EventTrace()
            val metrics = trace.metrics()
            // Written out rather than taken from `trace.hook`, because it has to do a second
            // thing: move the clock, so that `taskEnded`'s duration can tell the two readings
            // apart. The line it records is the same grammar `RecordingHook` produces.
            harness.hooks.register("notify") {
                trace.record("hook(notify)")
                harness.clock.advance(500)
            }
            harness.listener = trace.listener()
            harness.metrics = metrics

            harness.runExpectingSuccess(
                Etl.withHooks(
                    Etl.task("wip-order", Etl.phase("extract", loadWip(retries = 2))),
                    onSuccess = "notify",
                ),
            )

            assertAll(
                {
                    assertEquals(
                        listOf(
                            "onTaskStart(wip-order)",
                            "onPhaseStart(extract)",
                            "onStepStart(extract/load-wip)",
                            "metric.stepRetried(extract/load-wip)",
                            "onStepError(extract/load-wip, attempt=1, willRetry=true)",
                            "metric.stepRetried(extract/load-wip)",
                            "onStepError(extract/load-wip, attempt=2, willRetry=true)",
                            "metric.stepEnded(extract/load-wip, attempt=3, read=6, written=6)",
                            "onStepEnd(extract/load-wip, attempt=3)",
                            "onPhaseEnd(extract, SUCCEEDED)",
                            "metric.scratchBytes(wip-order)",
                            "hook(notify)",
                            "metric.taskEnded(wip-order, SUCCEEDED)",
                            "onTaskEnd(wip-order, SUCCEEDED)",
                        ),
                        trace.entries,
                    )
                },
                {
                    assertEquals(2, metrics.retries.size) {
                        "one per retried attempt, not one per retried step; saw ${metrics.retries.size}"
                    }
                },
                {
                    val sample = metrics.scratchSamples.single()
                    assertTrue(sample.bytes > 0) {
                        "the run wrote six rows into scratch, so the sample is taken while the " +
                            "directory still has something in it; bytes were ${sample.bytes}"
                    }
                },
                {
                    assertEquals(6_500L, metrics.taskEndings.single().durationMs) {
                        "2s + 4s of requested backoff plus the 500ms the hook spent, read from " +
                            "run's finally on the engine's injected clock; requested delays were " +
                            "${harness.delaysMillis} and the clock stands at ${harness.clock.elapsedMillis}"
                    }
                },
            )
        }
    }

    // ------------------------------------------------------------------------------------
    // Criteria 6 and 7, run 2: a terminal step failure is still metered.
    // ------------------------------------------------------------------------------------

    /**
     * `metrics.stepEnded` fires on terminal failure as well as on success, carrying rows 0/0, the
     * attempt that failed terminally and a `durationMs` spanning every attempt and the backoff
     * between them. The timer has no `outcome` tag, and without this call a step that always fails
     * would have no `etl_step_duration_seconds` series at all - which is the one shape an operator
     * is most likely to go looking for.
     *
     * `listener.onStepEnd` stays success-only, and the second assertion is what stops the two
     * being conflated: a `stepEnded` implemented by simply forwarding the listener's call site
     * would leave the metrics recorder empty here.
     *
     * The rows are 0/0 rather than "the rows the failed attempt managed to flush", and a `pipe`
     * step is used deliberately - it is the only step type whose `StepResult` ever carries a
     * non-zero pair, so it is the only one where reporting a partial count is even possible.
     */
    @Test
    fun aTerminalStepFailureIsMeteredWithNoRowsAndKeepsTheSameOrder() {
        TaskHarness(root).use { harness ->
            val mes = source(harness)
            mes.failAlways(afterRows = 2) { SQLTransientException("probe: always transient") }
            val trace = EventTrace()
            val metrics = trace.metrics()
            val listener = trace.listener()
            harness.hooks.register("alert", trace.hook("alert"))
            harness.listener = listener
            harness.metrics = metrics

            val outcome = harness.run(
                Etl.withHooks(
                    Etl.task("wip-broken", Etl.phase("extract", loadWip(retries = 1))),
                    onFailure = "alert",
                ),
            )

            assertAll(
                { assertEquals(Outcome.FAILED, outcome.outcome) },
                {
                    assertEquals(
                        listOf(
                            "onTaskStart(wip-broken)",
                            "onPhaseStart(extract)",
                            "onStepStart(extract/load-wip)",
                            "metric.stepRetried(extract/load-wip)",
                            "onStepError(extract/load-wip, attempt=1, willRetry=true)",
                            "metric.stepEnded(extract/load-wip, attempt=2, read=0, written=0)",
                            "onStepError(extract/load-wip, attempt=2, willRetry=false)",
                            "onPhaseEnd(extract, FAILED)",
                            "metric.scratchBytes(wip-broken)",
                            "hook(alert)",
                            "metric.taskEnded(wip-broken, FAILED)",
                            "onTaskEnd(wip-broken, FAILED)",
                        ),
                        trace.entries,
                    )
                },
                {
                    assertEquals(
                        StepResult(rowsRead = 0, rowsWritten = 0, durationMs = 2_000, attempt = 2),
                        metrics.result("load-wip"),
                    ) { "rows 0/0, the attempt that failed terminally, and the whole step's duration" }
                },
                {
                    assertTrue(listener.stepEnds.isEmpty()) {
                        "onStepEnd stays success-only; the listener saw ${listener.stepEnds}"
                    }
                },
            )
        }
    }

    // ------------------------------------------------------------------------------------
    // Criterion 8: a throwing TaskMetrics never changes a run's outcome.
    // ------------------------------------------------------------------------------------

    /**
     * Contract 3.5, at each of the four call sites.
     *
     * **"The run survived" is not evidence of isolation**, because an engine with no metrics call
     * sites at all satisfies it for free - so each case also asserts that the site under test
     * actually threw, and that every metrics site *and* every listener site after it still fired.
     * That is what separates "caught and continued" from "caught and gave up on the seam", and
     * from "never called it".
     *
     * `SCRATCH_BYTES` is the case with teeth. It is sampled from a `finally` inside the `use`, and
     * a Kotlin `finally` that throws **replaces** the in-flight exception rather than being
     * suppressed by it - so the guard has to enclose the `diskBytes()` call as well as the
     * `TaskMetrics` call. `TASK_ENDED` is the same hazard in `run`'s outer `finally`, where an
     * unguarded throw would replace the returned [TaskOutcome] itself.
     *
     * One scenario reaches all four sites and still ends SUCCEEDED, which is what makes "the
     * outcome did not change" an assertion rather than a restatement of a failure already there.
     */
    @ParameterizedTest(name = "{0}")
    @EnumSource(MetricsCall::class)
    fun aThrowingMetricsRecorderNeverChangesTheOutcomeAtAnyCallSite(site: MetricsCall) {
        TaskHarness(root.resolve(site.name.lowercase())).use { harness ->
            val mes = source(harness)
            mes.failFirst(count = 1, afterRows = 2) { SQLTransientException("probe: transient") }
            val trace = EventTrace()
            val listener = trace.listener()
            val thrower = trace.metrics(failAt = setOf(site))
            harness.listener = listener
            harness.metrics = thrower

            val outcome = harness.run(Etl.task("wip-isolated", Etl.phase("extract", loadWip(retries = 1))))

            assertAll(
                {
                    assertEquals(Outcome.SUCCEEDED, outcome.outcome) {
                        "a TaskMetrics that throws from $site must not fail the run: ${outcome.failure}"
                    }
                },
                {
                    assertTrue(thrower.thrown > 0) {
                        "the site under test actually fired; an engine that never calls one cannot " +
                            "fail this. thrown was ${thrower.thrown}"
                    }
                },
                {
                    assertEquals(allMetricSites, thrower.calls) {
                        "the engine kept metering after $site threw"
                    }
                },
                {
                    assertEquals(allListenerSites, listener.calls) {
                        "and the listener sites after $site fired too"
                    }
                },
            )
        }
    }
}
