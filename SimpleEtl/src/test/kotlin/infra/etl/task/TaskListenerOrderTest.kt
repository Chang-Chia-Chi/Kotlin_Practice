package infra.etl.task

import infra.etl.DuckFile
import infra.etl.Etl
import infra.etl.EventTrace
import infra.etl.TaskHarness
import infra.etl.duckdb.CreateTable
import infra.etl.pipe.RowTransform
import infra.etl.task.Outcome
import infra.etl.task.Step
import infra.etl.task.TriggerSource
import java.nio.file.Path
import java.sql.SQLException
import java.sql.SQLTransientException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

/**
 * P8a, contract 4 items 1 to 5 and 14: **what the engine tells the run listener, and when.**
 *
 * ### Whole traces, never subsequences
 *
 * Every ordering assertion here is `containsExactly` over the complete trace of a run. That is a
 * deliberate rule and not a stylistic one: `containsSubsequence` is satisfied by an empty list, so
 * a suite written that way passes unchanged against an engine that has no listener call sites in
 * it at all - and this project's progress.md already records three separate mechanisms for a test
 * that passes while proving nothing. A whole-trace assertion also gets two clauses for free that a
 * targeted assertion would have to state separately: that nothing fired *twice*, and that after a
 * terminal step failure no later step and no later phase started.
 *
 * ### Nothing here sleeps, and every duration is exact
 *
 * `TaskHarness`'s sleeper advances the injected clock by the backoff the engine asked for and
 * nothing else moves it, so a step retried twice has `durationMs = 6000` and an unretried step has
 * `durationMs = 0`. Both are equalities. Against a wall clock the second would be "about 2", which
 * is indistinguishable from an engine that measures the last attempt only - the case contract 2.6
 * exists to pin.
 *
 * ### What is deliberately not asserted
 *
 * Contract 2.3 notes a further consequence of hooks running after `ScratchDb.close()`: a run whose
 * phases all succeed but whose `close()` throws reports `onPhaseEnd(SUCCEEDED)` for every phase
 * and `onTaskEnd(FAILED)`. Nothing in this suite asserts it, because the only two ways to make
 * `close()` throw are to leave a temporary table behind - which P4's `NoTempTableTest` scans this
 * very file for, and which no test may write - or to make a file under the scratch root
 * undeletable, which is platform-specific. No assertion here contradicts the clause; none can
 * currently falsify it either, and pretending otherwise would be a fourth mechanism.
 */
class TaskListenerOrderTest {

    @TempDir
    lateinit var root: Path

    /** Six rows, `lot_code` woven with a marker so a dropped row is identifiable by value. */
    private fun source(harness: TaskHarness, rows: Int = 6): DuckFile =
        harness.datasource("oracle_mes").also { it.createSourceTable("wip", rows = rows, marker = "w") }

    private fun loadWip(retries: Int? = null, transform: RowTransform? = null) = Etl.pipe(
        name = "load-wip",
        sourceDatasource = "oracle_mes",
        sql = "select lot_id, lot_code, qty from wip",
        table = "wip_stg",
        retries = retries,
        transform = transform,
    )

    // ------------------------------------------------------------------------------------
    // Contract 4.1: the whole ordered trace, for the three runs the contract names.
    // ------------------------------------------------------------------------------------

    /** Contract 2.1, a run where everything works. */
    @Test
    fun everyCallSiteFiresInOrderForASuccessfulRun() {
        TaskHarness(root).use { harness ->
            source(harness)
            harness.datasource("report_oracle")
            val trace = EventTrace()
            harness.listener = trace.listener()

            harness.runExpectingSuccess(
                Etl.task(
                    "wip-summary",
                    Etl.phase("extract", loadWip()),
                    Etl.phase(
                        "publish",
                        Etl.sql(
                            "publish-summary",
                            "report_oracle",
                            "create or replace table published as select 1 as ok",
                        ),
                    ),
                ),
            )

            assertEquals(
                listOf(
                    "onTaskStart(wip-summary)",
                    "onPhaseStart(extract)",
                    "onStepStart(extract/load-wip)",
                    "onStepEnd(extract/load-wip, attempt=1)",
                    "onPhaseEnd(extract, SUCCEEDED)",
                    "onPhaseStart(publish)",
                    "onStepStart(publish/publish-summary)",
                    "onStepEnd(publish/publish-summary, attempt=1)",
                    "onPhaseEnd(publish, SUCCEEDED)",
                    "onTaskEnd(wip-summary, SUCCEEDED)",
                ),
                trace.entries,
            )
        }
    }

    /**
     * Contract 2.2, a terminal failure in phase 2 after phase 1 succeeded. The definition carries
     * a second step in the failing phase and a third phase after it, both of which the trace shows
     * were never started - which is what "no later step and no later phase starts" means and what
     * a targeted assertion on the failing step alone would not notice.
     */
    @Test
    fun aTerminalFailureEndsItsPhaseAndStartsNoLaterStepOrPhase() {
        TaskHarness(root).use { harness ->
            source(harness)
            harness.datasource("report_oracle")
            val trace = EventTrace()
            val events = trace.listener()
            harness.listener = events

            val outcome = harness.run(
                Etl.task(
                    "wip-failing",
                    Etl.phase("extract", loadWip()),
                    Etl.phase(
                        "build",
                        // A real DuckDB syntax error: non-transient, so retries do not apply.
                        Etl.sql("bad-step", "report_oracle", "this is not sql"),
                        Etl.sql("later-step", "report_oracle", "create or replace table late as select 1 as ok"),
                    ),
                    Etl.phase("publish", Etl.sql("later-phase-step", "report_oracle", "select 1")),
                ),
            )

            assertAll(
                { assertEquals(Outcome.FAILED, outcome.outcome) },
                {
                    assertEquals(
                        listOf(
                            "onTaskStart(wip-failing)",
                            "onPhaseStart(extract)",
                            "onStepStart(extract/load-wip)",
                            "onStepEnd(extract/load-wip, attempt=1)",
                            "onPhaseEnd(extract, SUCCEEDED)",
                            "onPhaseStart(build)",
                            "onStepStart(build/bad-step)",
                            "onStepError(build/bad-step, attempt=1, willRetry=false)",
                            "onPhaseEnd(build, FAILED)",
                            "onTaskEnd(wip-failing, FAILED)",
                        ),
                        trace.entries,
                    )
                },
                {
                    val chain = causeChainOf(events.stepErrors.single().error)
                    assertTrue(chain.any { it is SQLException }) {
                        "the listener is handed the failure itself, not a marker object; chain was $chain"
                    }
                },
            )
        }
    }

    /**
     * Contract 2.2 and 4.5: `onStepStart` once, an `onStepError` per failed attempt numbered from
     * 1, and `onStepEnd` only on success carrying the attempt that succeeded.
     */
    @Test
    fun aRetriedStepReportsEachFailedAttemptOnceAndThenTheAttemptThatSucceeded() {
        TaskHarness(root).use { harness ->
            val mes = source(harness)
            mes.failFirst(count = 2, afterRows = 2) { SQLTransientException("probe: transient") }
            val trace = EventTrace()
            val events = trace.listener()
            harness.listener = events

            harness.runExpectingSuccess(Etl.task("wip-retried", Etl.phase("extract", loadWip(retries = 2))))

            assertAll(
                {
                    assertEquals(
                        listOf(
                            "onTaskStart(wip-retried)",
                            "onPhaseStart(extract)",
                            "onStepStart(extract/load-wip)",
                            "onStepError(extract/load-wip, attempt=1, willRetry=true)",
                            "onStepError(extract/load-wip, attempt=2, willRetry=true)",
                            "onStepEnd(extract/load-wip, attempt=3)",
                            "onPhaseEnd(extract, SUCCEEDED)",
                            "onTaskEnd(wip-retried, SUCCEEDED)",
                        ),
                        trace.entries,
                    )
                },
                {
                    assertEquals(3, events.result("load-wip").attempt) {
                        "with retries: 2 the attempts run 1..3 and the third is the one that worked"
                    }
                },
                {
                    assertEquals(1, events.stepStarts.size) {
                        "onStepStart fires once per step, not per attempt; starts were ${events.stepStarts}"
                    }
                },
            )
        }
    }

    /**
     * The half of `willRetry` that the two tests above cannot reach: a failure that *is* transient
     * and still will not be retried, because the attempts ran out. Without this case `willRetry`
     * could be implemented as `isTransient(failure)` alone and every other assertion in the file
     * would stay green.
     *
     * [TaskHarness.delaysMillis] carries the second half: exactly one backoff was requested, so
     * the last `onStepError` was emitted without a sleep behind it.
     */
    @Test
    fun aTransientFailureThatRunsOutOfAttemptsReportsWillRetryFalseOnTheLast() {
        TaskHarness(root).use { harness ->
            val mes = source(harness)
            mes.failAlways(afterRows = 2) { SQLTransientException("probe: always transient") }
            val trace = EventTrace()
            harness.listener = trace.listener()

            val outcome = harness.run(Etl.task("wip-exhausted", Etl.phase("extract", loadWip(retries = 1))))

            assertAll(
                { assertEquals(Outcome.FAILED, outcome.outcome) },
                {
                    assertEquals(
                        listOf(
                            "onTaskStart(wip-exhausted)",
                            "onPhaseStart(extract)",
                            "onStepStart(extract/load-wip)",
                            "onStepError(extract/load-wip, attempt=1, willRetry=true)",
                            "onStepError(extract/load-wip, attempt=2, willRetry=false)",
                            "onPhaseEnd(extract, FAILED)",
                            "onTaskEnd(wip-exhausted, FAILED)",
                        ),
                        trace.entries,
                    )
                },
                {
                    assertEquals(listOf(2_000L), harness.delaysMillis) {
                        "willRetry is decided and reported before the sleeper is asked for anything"
                    }
                },
            )
        }
    }

    /**
     * Contract 2.1's first sentence, and the reason it is a sentence: `onTaskStart` fires before
     * `ScratchDb` is constructed, so a run that dies in `ScratchDb`'s own `init` is still a run the
     * listener saw start and end. An engine that emitted `onTaskStart` one line lower would report
     * a run that only ever ended.
     */
    @Test
    fun aRunThatDiesBeforeScratchExistsStillStartedAndEnded() {
        TaskHarness(root).use { harness ->
            val trace = EventTrace()
            harness.listener = trace.listener()

            val outcome = harness.run(
                Etl.withScratchMemoryLimitMb(
                    Etl.task("wip-no-scratch", Etl.phase("extract", Etl.sql("touch", Etl.SCRATCH, "select 1"))),
                    limit = 0,
                ),
            )

            assertAll(
                { assertEquals(Outcome.FAILED, outcome.outcome) },
                {
                    assertEquals(
                        listOf(
                            "onTaskStart(wip-no-scratch)",
                            "onTaskEnd(wip-no-scratch, FAILED)",
                        ),
                        trace.entries,
                    )
                },
            )
        }
    }

    // ------------------------------------------------------------------------------------
    // Contract 4.2: logging: false, asserted in pairs.
    // ------------------------------------------------------------------------------------

    /**
     * The `logging: false` flag, and contract 2.7's "hooks are unaffected".
     *
     * The same task runs three times on one harness. "No events under `logging: false`" alone
     * would pass against an engine with no call sites at all, so the run before it asserts the
     * full trace and the run after it asserts that the call sites came back - the second of which
     * also proves the harness hands the engine a forwarder rather than the listener it happened to
     * hold when the engine was first built.
     */
    @Test
    fun loggingFalseSuppressesEveryListenerCallAndLeavesTheHookAlone() {
        TaskHarness(root).use { harness ->
            source(harness)
            val trace = EventTrace()
            val notify = trace.hook("notify")
            harness.hooks.register("notify", notify)
            val definition = Etl.withHooks(
                Etl.task("wip-logging", Etl.phase("extract", loadWip())),
                onSuccess = "notify",
            )
            val loud = listOf(
                "onTaskStart(wip-logging)",
                "onPhaseStart(extract)",
                "onStepStart(extract/load-wip)",
                "onStepEnd(extract/load-wip, attempt=1)",
                "onPhaseEnd(extract, SUCCEEDED)",
                "hook(notify)",
                "onTaskEnd(wip-logging, SUCCEEDED)",
            )

            harness.listener = trace.listener()
            harness.runExpectingSuccess(Etl.withLogging(definition, logging = true))
            assertEquals(loud, trace.entries) { "logging: true" }

            trace.clear()
            val quiet = trace.listener()
            harness.listener = quiet
            harness.runExpectingSuccess(Etl.withLogging(definition, logging = false))
            assertAll(
                {
                    assertTrue(quiet.calls.isEmpty()) {
                        "logging: false suppresses every call site; calls were ${quiet.calls}"
                    }
                },
                {
                    assertEquals(listOf("hook(notify)"), trace.entries) {
                        "a hook is not a listener call, so the flag does not reach it"
                    }
                },
            )

            trace.clear()
            harness.listener = trace.listener()
            harness.runExpectingSuccess(Etl.withLogging(definition, logging = true))
            assertEquals(loud, trace.entries) { "logging: true again, on a third listener" }
        }
    }

    // ------------------------------------------------------------------------------------
    // Contract 4.3 and 4.4: what StepResult carries.
    // ------------------------------------------------------------------------------------

    /**
     * Contract 2.6: `durationMs` spans every attempt, including the injected backoff, and comes
     * from the injected clock.
     *
     * The unretried run pins the second half. Only a clock nothing advanced answers 0; a wall
     * clock answers a small positive number, and a small positive number is exactly what an engine
     * that timed the *last attempt only* would also report on the retried run.
     */
    @Test
    fun durationMsSpansEveryAttemptAndComesFromTheInjectedClock() {
        TaskHarness(root.resolve("plain")).use { harness ->
            source(harness)
            val events = EventTrace().listener()
            harness.listener = events

            harness.runExpectingSuccess(Etl.task("wip-quick", Etl.phase("extract", loadWip())))

            assertAll(
                {
                    assertTrue(harness.delaysMillis.isEmpty()) {
                        "requested backoff was ${harness.delaysMillis}"
                    }
                },
                {
                    assertEquals(0L, events.result("load-wip").durationMs) {
                        "nothing slept, so no time passed at all"
                    }
                },
            )
        }

        TaskHarness(root.resolve("retried")).use { harness ->
            val mes = source(harness)
            mes.failFirst(count = 2, afterRows = 2) { SQLTransientException("probe: transient") }
            val events = EventTrace().listener()
            harness.listener = events

            harness.runExpectingSuccess(Etl.task("wip-slow", Etl.phase("extract", loadWip(retries = 2))))

            assertAll(
                { assertEquals(listOf(2_000L, 4_000L), harness.delaysMillis) },
                {
                    assertEquals(harness.delaysMillis.sum(), events.result("load-wip").durationMs) {
                        "all three attempts and both backoffs, cross-checked against what was requested"
                    }
                },
                { assertEquals(6_000L, events.result("load-wip").durationMs) },
            )
        }
    }

    /**
     * Contract 2.6's rows ruling: a pipe reports the real pair, every other step type reports
     * 0 / 0 because only `pipe` moves rows through the JVM.
     *
     * The transform drops one row, so `rowsRead` and `rowsWritten` are two different numbers.
     * Without that they would be one number twice, and an engine reporting `rowsRead` for both
     * would pass.
     */
    @Test
    fun stepResultRowsAreRealForAPipeAndZeroForEveryOtherStepType() {
        TaskHarness(root).use { harness ->
            source(harness)
            harness.datasource("report_oracle")
            val events = EventTrace().listener()
            harness.listener = events

            harness.runExpectingSuccess(
                Etl.task(
                    "wip-rows",
                    Etl.phase(
                        "work",
                        loadWip(transform = RowTransform { row -> if (row["lot_code"] == "w-0") null else row }),
                        Etl.materialize("build-summary", output = "summary", sql = "select count(*) as n from wip_stg"),
                        Etl.sql(
                            "touch",
                            "report_oracle",
                            "create or replace table touched as select 1 as ok",
                        ),
                        Etl.export("read-one", "report_oracle", "one" to "select 1 as one"),
                    ),
                ),
            )

            assertAll(
                { assertEquals(6L, events.result("load-wip").rowsRead) { "rows read" } },
                {
                    assertEquals(5L, events.result("load-wip").rowsWritten) {
                        "the transform dropped one row, which is why these are two numbers"
                    }
                },
            )
            listOf("build-summary", "touch", "read-one").forEach { step ->
                assertAll(
                    { assertEquals(0L, events.result(step).rowsRead) { "$step rows read" } },
                    { assertEquals(0L, events.result(step).rowsWritten) { "$step rows written" } },
                )
            }
        }
    }

    // ------------------------------------------------------------------------------------
    // The context itself.
    // ------------------------------------------------------------------------------------

    /**
     * `TaskContext` carries what the listener contract says it carries, and `startedAt` comes from
     * the injected clock (contract 1.3). An engine calling `Instant.now()` reports today's date,
     * which is not this clock's instant - that is the assertion the injected clock makes possible.
     */
    @Test
    fun theTaskContextCarriesTheRunTheTriggerAndTheClocksInstant() {
        TaskHarness(root).use { harness ->
            source(harness)
            val trace = EventTrace()
            val triggered = trace.listener()
            harness.listener = triggered
            val definition = Etl.task("wip-context", Etl.phase("extract", loadWip()))

            val outcome = harness.runTriggeredBy(definition, TriggerSource.API, by = "alice")

            assertEquals(Outcome.SUCCEEDED, outcome.outcome)
            val ctx = triggered.taskStarts.single()
            assertAll(
                { assertEquals(outcome.runId, ctx.runId) { "one id names the run everywhere" } },
                { assertEquals("wip-context", ctx.taskName) },
                { assertEquals(TriggerSource.API, ctx.triggerSource) },
                { assertEquals("alice", ctx.triggeredBy) { "the caller identity of spec 8.2" } },
                {
                    assertEquals(harness.clock.instant(), ctx.startedAt) {
                        "startedAt is read from the injected clock, not from Instant.now()"
                    }
                },
                {
                    assertEquals(ctx, triggered.taskEnds.single().first) {
                        "start and end describe the same run"
                    }
                },
            )

            val scheduled = trace.listener()
            harness.listener = scheduled
            harness.runExpectingSuccess(definition)

            assertAll(
                {
                    val by = scheduled.taskStarts.single().triggeredBy
                    assertNull(by) { "a scheduled firing has no caller identity, but carried '$by'" }
                },
                { assertEquals(TriggerSource.SCHEDULE, scheduled.taskStarts.single().triggerSource) },
            )
        }
    }

    // ------------------------------------------------------------------------------------
    // Contract 4.14: guards report like any other terminal failure.
    // ------------------------------------------------------------------------------------

    companion object {

        /**
         * The two guards contract 2.2 names, both of which reject a step *before* it runs. Both
         * carry the same step name, so one expected trace serves both.
         *
         * These are the discriminating cases for "onStepStart fires before any guard on the step":
         * a guard that ran before the call site would leave a trace with no `onStepStart` in it,
         * and one that reported nothing at all would leave a phase that failed with no step in it.
         */
        @JvmStatic
        fun guardRejectedSteps(): List<Arguments> = listOf(
            Arguments.of(
                "negative retries",
                Etl.sql("bad-step", "report_oracle", "select 1", retries = -1),
            ),
            Arguments.of(
                "a retried scratch REQUIRED target",
                Etl.pipe(
                    name = "bad-step",
                    sourceDatasource = "oracle_mes",
                    sql = "select lot_id, lot_code, qty from wip",
                    table = "wip_req",
                    createTable = CreateTable.REQUIRED,
                    retries = 3,
                ),
            ),
        )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("guardRejectedSteps")
    fun aGuardRejectedStepReportsAStepStartAndThenATerminalStepError(label: String, step: Step) {
        TaskHarness(root.resolve(label.filter { it.isLetter() })).use { harness ->
            source(harness)
            harness.datasource("report_oracle")
            val trace = EventTrace()
            val events = trace.listener()
            harness.listener = events

            val outcome = harness.run(Etl.task("wip-guard", Etl.phase("only", step)))

            assertAll(
                { assertEquals(Outcome.FAILED, outcome.outcome) { label } },
                {
                    assertEquals(
                        listOf(
                            "onTaskStart(wip-guard)",
                            "onPhaseStart(only)",
                            "onStepStart(only/bad-step)",
                            "onStepError(only/bad-step, attempt=1, willRetry=false)",
                            "onPhaseEnd(only, FAILED)",
                            "onTaskEnd(wip-guard, FAILED)",
                        ),
                        trace.entries,
                    ) { label }
                },
                {
                    val message = events.stepErrors.single().error.message
                    assertTrue(message?.contains("bad-step") == true) {
                        "the listener is handed the guard's own diagnostic; message was: $message"
                    }
                },
                {
                    assertTrue(harness.delaysMillis.isEmpty()) {
                        "a rejected step is not retried; delays were ${harness.delaysMillis}"
                    }
                },
            )
        }
    }

    /** The cause chain, bounded, for the assertions that ask what a reported failure really was. */
    private fun causeChainOf(failure: Throwable): List<Throwable> =
        generateSequence(failure) { if (it.cause === it) null else it.cause }.take(16).toList()
}
