package infra.etl.task

import infra.etl.Etl
import infra.etl.EventTrace
import infra.etl.P7Tasks
import infra.etl.Probe
import infra.etl.ProbeDatasource
import infra.etl.TIMEOUT_SECONDS
import infra.etl.TaskHarness
import infra.etl.rendered
import java.nio.file.Path
import java.sql.SQLTransientException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit.SECONDS
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.onSubscription
import kotlinx.coroutines.launch
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir

/**
 * P8c: the coroutine-native mirror of spec 9.2's listener - `TaskEventFlow`, its `SharedFlow`, and
 * the two counters that make a loss on it interpretable.
 *
 * ### Every ordering assertion is a whole rendered list, read from `replayCache`
 *
 * The ordering tests have no collector, no dispatcher and nothing to wait for. `TaskHarness` runs
 * the engine as an ordinary blocking call on the test's thread, the engine calls the listener
 * synchronously and `tryEmit` does not suspend, so a run is one sequential program on one thread
 * and the replay cache afterwards is a settled record of it. Assertions are whole lists rather than
 * subsequences, for P8a's reason: an empty list satisfies every subsequence assertion.
 *
 * **One `TaskEventFlow` per run, in every test that reads `replayCache`.** The cache is cumulative
 * across runs, and `resetReplayCache()` is declared on `MutableSharedFlow` rather than on the
 * `SharedFlow` this class exposes, so a test cannot clear it between two runs of one harness.
 *
 * **Size is asserted before contents.** `replay` is a truncating ceiling that silently keeps the
 * *tail* - `replay = 4` over ten emissions holds events 6 to 9, with no error anywhere - so a size
 * assertion first turns a truncation into "the wrong length" rather than a baffling off-by-N.
 * [REPLAY] is far above every run here, so a correct implementation never reaches the ceiling.
 *
 * ### Rendered strings for order, typed events for payload
 *
 * `infra.etl.rendered` carries the identifying half of an event and the tests write the expected
 * lines out as literals. Value equality over whole `TaskEvent`s is not available: each carries the
 * run's `TaskContext`, whose `runId` is a fresh UUID, so an expected event could only be built by
 * reading the actual one first. What no string survives - a `StepResult`, the *identity* of a
 * `Throwable` or of the run's `TaskContext` - is asserted from the typed events, cross-checked
 * against a co-attached [infra.etl.RecordingListener] and not against the flow's view of itself.
 *
 * ### Nothing here sleeps
 *
 * Every wait is a `CountDownLatch.await`, a `LinkedBlockingQueue.poll` or a `Thread.join`, each
 * with a [TIMEOUT_SECONDS] deadline whose expiry is a loud failure rather than a slower pass.
 *
 * ### What this file deliberately does not test
 *
 * That a suspending `emit` would back-pressure a run, and that a collector on `Dispatchers
 * .Unconfined` or started `UNDISPATCHED` runs inline on the ETL thread inside `tryEmit` and parks
 * the run if it blocks. Both were measured during the contract round; the second is a host
 * obligation documented in the `events` KDoc, and reproducing it would mean shipping a
 * deliberately wrong collector and asserting that it breaks the run.
 */
class TaskEventFlowTest {

    @TempDir
    lateinit var root: Path

    // ------------------------------------------------------------------------------------
    // Criterion 1 (and 7): order, read from the replay cache of a run with no collector.
    // ------------------------------------------------------------------------------------

    /** P8a contract 2.1's order, in the flow's vocabulary, for a run where everything works. */
    @Test
    fun everyEventOfASuccessfulRunIsReplayedInOrder() {
        val flow = TaskEventFlow(replay = REPLAY)
        val expected = listOf(
            "TaskStarted(wip-summary)",
            "PhaseStarted(stage)",
            "StepStarted(stage/stage-rows)",
            "StepEnded(stage/stage-rows, attempt=1)",
            "PhaseEnded(stage, SUCCEEDED)",
            "PhaseStarted(publish)",
            "StepStarted(publish/publish-summary)",
            "StepEnded(publish/publish-summary, attempt=1)",
            "PhaseEnded(publish, SUCCEEDED)",
            "TaskEnded(wip-summary, SUCCEEDED)",
        )

        TaskHarness(root).use { harness ->
            harness.listener = flow
            harness.runExpectingSuccess(twoPhaseTask())
        }

        val replay = flow.events.replayCache
        assertEquals(expected.size, replay.size) {
            "replay is a truncating ceiling that keeps the tail, so length comes first: ${rendered(replay)}"
        }
        assertEquals(expected, rendered(replay))
    }

    /**
     * P8a contract 2.2's order for a retried step, and with it the whole of criterion 7: the run
     * below reaches **all seven** of spec 9.2's call sites, so one trace pins the entire mapping.
     *
     * Every payload assertion compares against a `TaskRunListener` attached beside the flow through
     * `TaskRunListener.of`. `error` is compared by identity, which a listener that rebuilt a
     * `Throwable` of its own would fail. The `task` identity assertion pins the delegating `task`
     * overrides on the five phase- and step-shaped subtypes: an implementation that built a second
     * `TaskContext` per event satisfies every rendered line here and fails that one.
     */
    @Test
    fun aRetriedRunReplaysAllSevenCallSitesWithTheirPayloadsIntact() {
        val flow = TaskEventFlow(replay = REPLAY)
        val trace = EventTrace()
        val recording = trace.listener()
        val expected = listOf(
            "TaskStarted(wip-retried)",
            "PhaseStarted(extract)",
            "StepStarted(extract/load-wip)",
            "StepError(extract/load-wip, attempt=1, willRetry=true)",
            "StepEnded(extract/load-wip, attempt=2)",
            "PhaseEnded(extract, SUCCEEDED)",
            "TaskEnded(wip-retried, SUCCEEDED)",
        )

        TaskHarness(root).use { harness ->
            val mes = harness.datasource("oracle_mes").also { it.createSourceTable("wip", rows = 6, marker = "w") }
            mes.failFirst(count = 1, afterRows = 2) { SQLTransientException("probe: transient") }
            harness.listener = TaskRunListener.of(recording, flow)

            harness.runExpectingSuccess(
                Etl.task(
                    "wip-retried",
                    Etl.phase(
                        "extract",
                        Etl.pipe(
                            name = "load-wip",
                            sourceDatasource = "oracle_mes",
                            sql = "select lot_id, lot_code, qty from wip",
                            table = "wip_stg",
                            retries = 2,
                        ),
                    ),
                ),
            )
        }

        val replay = flow.events.replayCache
        assertEquals(expected.size, replay.size) { "the whole run should be replayed: ${rendered(replay)}" }

        val context = recording.taskStarts.single()
        val stepError = replay.filterIsInstance<TaskEvent.StepError>().single()
        val stepEnded = replay.filterIsInstance<TaskEvent.StepEnded>().single()
        val phaseEnded = replay.filterIsInstance<TaskEvent.PhaseEnded>().single()
        val taskEnded = replay.filterIsInstance<TaskEvent.TaskEnded>().single()

        assertAll(
            { assertEquals(expected, rendered(replay)) },
            {
                val subtypes = replay.map { it::class }.toSet()
                assertEquals(7, subtypes.size) {
                    "each of spec 9.2's seven call sites maps to its own subtype; saw ${subtypes.map { it.simpleName }}"
                }
            },
            {
                assertSame(recording.stepErrors.single().error, stepError.error) {
                    "the failure travels by identity, not as a copy; the flow carried ${stepError.error}"
                }
            },
            { assertEquals(recording.stepErrors.single().ctx, stepError.step) },
            { assertEquals(recording.stepErrors.single().attempt, stepError.attempt) },
            { assertEquals(recording.stepErrors.single().willRetry, stepError.willRetry) },
            { assertEquals(recording.result("load-wip"), stepEnded.result) },
            { assertEquals(recording.stepEnds.single().ctx, stepEnded.step) },
            { assertEquals(recording.phaseEnds.single().first, phaseEnded.phase) },
            { assertEquals(Outcome.SUCCEEDED, phaseEnded.outcome) },
            { assertEquals(Outcome.SUCCEEDED, taskEnded.outcome) },
            {
                assertTrue(replay.all { it.task === context }) {
                    "every event of one run carries that run's own TaskContext, but the flow reported " +
                        "${replay.map { it.task }.distinct()} against $context"
                }
            },
        )
    }

    // ------------------------------------------------------------------------------------
    // Criterion 2: a wedged collector loses the newest, and the loss is counted.
    // ------------------------------------------------------------------------------------

    /**
     * The only test in the phase that separates `SUSPEND` from `DROP_OLDEST` on **content**: a
     * wedged collector released after the run receives the run's *first* [CAP] events. Under
     * `DROP_OLDEST` it would receive the last two instead, with every counter assertion here still
     * green - measured during the contract round as `[0,1]` against `[10,11]`.
     *
     * ### The wedge, and why each step of it is load-bearing
     *
     * 1. the collector is launched on a **dedicated single-thread** `ExecutorCoroutineDispatcher`,
     *    so nothing else can be scheduled past it and nothing runs inline on the ETL thread;
     * 2. the test waits for `onSubscription` - a `SharedFlow` extension that fires *after* the
     *    subscription slot is registered, which `subscriptionCount` alone cannot promise;
     * 3. a blocking task is then submitted to **that same executor**, and the test waits until it
     *    is *observably running*. Without that last wait the run's emissions race the collector's
     *    resumption: measured, 8 distinct outcomes in 500 runs without it, 1000/1000 identical
     *    with it. It is deterministic because the single thread is FIFO - the wedge task is queued
     *    behind the dispatch that registered the subscription, so it starts only once the collector
     *    has suspended, and every later resumption queues behind the wedge.
     *
     * The closed accounting equation is `delivered + buffered + dropped == emitted`, and only two
     * of the three are observable from outside - which is why the assertion reads
     * `dropped == emitted - extraBufferCapacity` plus nothing delivered during the run. The naive
     * `delivered + dropped == emitted` is false against correct code: the `extraBufferCapacity`
     * events sitting accepted-but-undelivered are counted in neither bucket.
     */
    @Test
    fun aWedgedCollectorLosesTheNewestEventsAndTheLossIsCounted() {
        val flow = TaskEventFlow(replay = 0, extraBufferCapacity = CAP)
        val executor = Executors.newSingleThreadExecutor { runnable -> Thread(runnable, "p8c-collector") }
        val dispatcher = executor.asCoroutineDispatcher()
        val scope = CoroutineScope(dispatcher)
        val subscribed = CountDownLatch(1)
        val wedged = CountDownLatch(1)
        val release = CountDownLatch(1)
        val arrivals = LinkedBlockingQueue<TaskEvent>()

        try {
            scope.launch {
                flow.events.onSubscription { subscribed.countDown() }.collect { arrivals.put(it) }
            }
            await(subscribed, "the collector never subscribed")

            executor.execute {
                wedged.countDown()
                release.await(TIMEOUT_SECONDS, SECONDS)
            }
            await(wedged, "the wedge task never started running on the collector's own thread")

            val outcome = TaskHarness(root).use { harness ->
                harness.listener = flow
                harness.run(oneStepTask())
            }

            val duringRun = ArrayList(arrivals)
            val emitted = flow.emitted
            val dropped = flow.dropped
            val subscribers = flow.subscriptionCount.value

            release.countDown()
            val drained = rendered(List(CAP) { take(arrivals) })

            assertAll(
                { assertEquals(Outcome.SUCCEEDED, outcome.outcome) { "the run failed: ${outcome.failure}" } },
                {
                    assertEquals(1, subscribers) {
                        "one collector was attached, and subscriptionCount is how a host learns that"
                    }
                },
                {
                    assertTrue(duringRun.isEmpty()) {
                        "a wedged collector cannot be handed anything while the run is in flight, " +
                            "but it received ${rendered(duringRun)}"
                    }
                },
                {
                    assertEquals(emitted - CAP, dropped) {
                        "every emission past the buffer is a counted loss: emitted=$emitted, dropped=$dropped, " +
                            "extraBufferCapacity=$CAP"
                    }
                },
                {
                    assertTrue(dropped > 0) {
                        "the run must overflow a buffer of $CAP for this test to mean anything; " +
                            "emitted=$emitted, dropped=$dropped"
                    }
                },
                {
                    assertEquals(listOf("TaskStarted(wip-summary)", "PhaseStarted(stage)"), drained) {
                        "SUSPEND drops the newest, so the collector gets the run's first $CAP events in order"
                    }
                },
                {
                    assertTrue(arrivals.isEmpty()) {
                        "exactly $CAP events were ever buffered, but ${rendered(ArrayList(arrivals))} followed them"
                    }
                },
            )
        } finally {
            release.countDown()
            scope.cancel()
            dispatcher.close()
            executor.shutdownNow()
        }
    }

    // ------------------------------------------------------------------------------------
    // Criterion 3: nothing collecting is not a counted loss.
    // ------------------------------------------------------------------------------------

    /**
     * With `replay = 0` and no subscriber, `tryEmit` returns true and the event is discarded -
     * measured, under every overflow policy - so no counter of any design can report "nobody was
     * listening". This pins that `dropped` does not pretend otherwise.
     *
     * **The co-attached recorder is not decoration.** `dropped == 0` alone is green against a
     * `TaskEventFlow` whose seven overrides are empty bodies, which is the first thing anyone
     * writes. Asserting that the run reached all six call sites, and that `emitted` counted exactly
     * them, is what makes the zero mean "nothing was lost" rather than "nothing happened".
     */
    @Test
    fun anAbsentSubscriberIsNotACountedLoss() {
        val flow = TaskEventFlow()
        val trace = EventTrace()
        val recording = trace.listener()

        val outcome = TaskHarness(root).use { harness ->
            harness.listener = TaskRunListener.of(recording, flow)
            harness.run(oneStepTask())
        }

        assertAll(
            { assertEquals(Outcome.SUCCEEDED, outcome.outcome) { "the run failed: ${outcome.failure}" } },
            {
                assertEquals(CALL_SITES_OF_A_ONE_STEP_RUN, recording.calls.size) {
                    "the flow was attached beside a recorder through of(); the run reported ${trace.entries}"
                }
            },
            {
                assertEquals(0L, flow.dropped) {
                    "an event discarded because nothing was collecting is not a lagging subscriber's loss"
                }
            },
            {
                assertEquals(recording.calls.size.toLong(), flow.emitted) {
                    "emitted counts every call site whatever tryEmit answered; the recorder saw ${recording.calls}"
                }
            },
        )
    }

    // ------------------------------------------------------------------------------------
    // Criterion 4: two runs, one engine, one flow.
    // ------------------------------------------------------------------------------------

    /**
     * Spec 8.4's concurrent runs through spec 9.2's single listener seam: one `TaskEngine`, one
     * `TaskEventFlow`, two tasks live at once on two plain threads.
     *
     * **Filtering by `runId` is not enough on its own.** A stream carrying all of A's events and
     * then all of B's satisfies "each run is in order after filtering" perfectly, and that is the
     * stream a per-run flow, or a lock around the whole run, would produce. The assertion that
     * bites is that **both** runs' `TaskStarted` precede **either** run's `TaskEnded`. It is
     * deterministic rather than lucky: each run parks inside its own [ProbeDatasource] and neither
     * is released until both have arrived. `BackgroundRun.outcome`'s `Thread.join` then supplies
     * the happens-before between the threads that emitted and the thread that asserts.
     */
    @Test
    fun twoConcurrentRunsInterleaveThroughOneEngineAndOneFlow() {
        val flow = TaskEventFlow(replay = REPLAY)
        val probeA = Probe("probe_a")
        val probeB = Probe("probe_b")
        val sourceA = ProbeDatasource(probeA)
        val sourceB = ProbeDatasource(probeB)

        try {
            TaskHarness(root).use { harness ->
                harness.register("probe_a", Jdbi.create(sourceA))
                harness.register("probe_b", Jdbi.create(sourceB))
                harness.listener = flow

                val runA = harness.start(P7Tasks.parking("task-a", "probe_a"))
                probeA.awaitEntry()
                val runB = harness.start(P7Tasks.parking("task-b", "probe_b"))
                probeB.awaitEntry()

                probeA.release()
                probeB.release()
                val a = runA.outcome()
                val b = runB.outcome()

                val replay = flow.events.replayCache
                assertEquals(2 * CALL_SITES_OF_A_ONE_STEP_RUN, replay.size) {
                    "one flow carries both runs: ${rendered(replay)}"
                }

                val starts = replay.indices.filter { replay[it] is TaskEvent.TaskStarted }
                val ends = replay.indices.filter { replay[it] is TaskEvent.TaskEnded }

                assertAll(
                    { assertEquals(Outcome.SUCCEEDED, a.outcome) { "task-a failed: ${a.failure}" } },
                    { assertEquals(Outcome.SUCCEEDED, b.outcome) { "task-b failed: ${b.failure}" } },
                    { assertEquals(parkingRunEvents("task-a"), rendered(replay.filter { it.task.runId == a.runId })) },
                    { assertEquals(parkingRunEvents("task-b"), rendered(replay.filter { it.task.runId == b.runId })) },
                    {
                        val lastStart = starts.maxOrNull()
                        val firstEnd = ends.minOrNull()
                        assertTrue(lastStart != null && firstEnd != null && lastStart < firstEnd) {
                            "both runs had started before either ended, so the stream must interleave; " +
                                "starts were at $starts, ends at $ends, stream was ${rendered(replay)}"
                        }
                    },
                )
            }
        } finally {
            sourceA.close()
            sourceB.close()
        }
    }

    // ------------------------------------------------------------------------------------
    // Criteria 5 and 6: suppression, and the construction guard.
    // ------------------------------------------------------------------------------------

    /**
     * Spec 9.2's per-task switch silences the flow exactly as it silences any other listener,
     * because the flow *is* one: the engine binds the run's sink to `NONE` and never reaches it.
     *
     * A pair, because the `false` half alone is satisfied by a flow that never emits at all, and a
     * separate `TaskEventFlow` per run, because the replay cache is cumulative and cannot be reset
     * through the `SharedFlow` this class exposes.
     */
    @Test
    fun loggingFalseSilencesTheFlowAndLoggingTrueDoesNot() {
        val loud = TaskEventFlow(replay = REPLAY)
        val silent = TaskEventFlow(replay = REPLAY)

        TaskHarness(root).use { harness ->
            val definition = oneStepTask("wip-logged")

            harness.listener = loud
            harness.runExpectingSuccess(definition)

            harness.listener = silent
            harness.runExpectingSuccess(Etl.withLogging(definition, logging = false))
        }

        assertAll(
            { assertEquals(oneStepRunEvents("wip-logged"), rendered(loud.events.replayCache)) },
            {
                assertEquals(emptyList<String>(), rendered(silent.events.replayCache)) {
                    "logging: false binds the run's sink to NONE, so the flow sees nothing at all"
                }
            },
            {
                assertEquals(0L, silent.emitted) {
                    "a suppressed run reaches no call site, so there is nothing to count as emitted"
                }
            },
        )
    }

    /**
     * A flow with neither replay nor buffer refuses **every** emission once a subscriber is present
     * - measured - so it would report 100% loss forever. The guard is this class's own: measured,
     * `MutableSharedFlow(0, 0, SUSPEND)` constructs silently, because `SUSPEND` is the default
     * policy and only a non-default one makes a zero capacity illegal to the library. The message
     * is asserted on because it is the only thing that tells a host which number to raise.
     */
    @Test
    fun aFlowWithNeitherReplayNorBufferIsRefusedAtConstruction() {
        val thrown = assertThrows<IllegalArgumentException> { TaskEventFlow(replay = 0, extraBufferCapacity = 0) }
        val message = thrown.message ?: ""

        assertAll(
            { assertTrue(message.contains("replay")) { "the message must name replay, was: $message" } },
            {
                assertTrue(message.contains("extraBufferCapacity")) {
                    "the message must name extraBufferCapacity, was: $message"
                }
            },
        )
    }

    // ------------------------------------------------------------------------------------
    // Definitions and waits
    // ------------------------------------------------------------------------------------

    /** One phase, one step, six call sites: the smallest run that reaches every task-level event. */
    private fun oneStepTask(name: String = "wip-summary"): TaskDefinition = Etl.task(
        name,
        Etl.phase(
            "stage",
            Etl.materialize(name = "stage-rows", output = "staged", sql = "select i from range(0, 10) t(i)"),
        ),
    )

    /** Two phases, so the ordering test can show a phase closing before the next one opens. */
    private fun twoPhaseTask(): TaskDefinition = Etl.task(
        "wip-summary",
        Etl.phase(
            "stage",
            Etl.materialize(name = "stage-rows", output = "staged", sql = "select i from range(0, 10) t(i)"),
        ),
        Etl.phase("publish", Etl.sql("publish-summary", Etl.SCRATCH, "select count(*) as n from staged")),
    )

    private fun oneStepRunEvents(task: String): List<String> = listOf(
        "TaskStarted($task)",
        "PhaseStarted(stage)",
        "StepStarted(stage/stage-rows)",
        "StepEnded(stage/stage-rows, attempt=1)",
        "PhaseEnded(stage, SUCCEEDED)",
        "TaskEnded($task, SUCCEEDED)",
    )

    /** P7's parking task, whose single `sql` step stops inside its datasource until released. */
    private fun parkingRunEvents(task: String): List<String> = listOf(
        "TaskStarted($task)",
        "PhaseStarted(work)",
        "StepStarted(work/park)",
        "StepEnded(work/park, attempt=1)",
        "PhaseEnded(work, SUCCEEDED)",
        "TaskEnded($task, SUCCEEDED)",
    )

    /** A bounded wait whose expiry means hung, not slow. */
    private fun await(latch: CountDownLatch, what: String) {
        check(latch.await(TIMEOUT_SECONDS, SECONDS)) { "$what within ${TIMEOUT_SECONDS}s" }
    }

    private fun take(queue: LinkedBlockingQueue<TaskEvent>): TaskEvent =
        queue.poll(TIMEOUT_SECONDS, SECONDS)
            ?: error("the released collector received nothing within ${TIMEOUT_SECONDS}s")

    private companion object {

        /** Far above any run in this file, so a truncating replay ceiling is never reached. */
        const val REPLAY = 32

        /** The buffer of the wedged collector. The default 256 can never overflow a run this size. */
        const val CAP = 2

        /** `TaskStarted`, `PhaseStarted`, `StepStarted`, `StepEnded`, `PhaseEnded`, `TaskEnded`. */
        const val CALL_SITES_OF_A_ONE_STEP_RUN = 6
    }
}
