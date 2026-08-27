package infra.etl.task

import infra.etl.Etl
import infra.etl.EventTrace
import infra.etl.ListenerCall
import infra.etl.TaskHarness
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir

/**
 * P8b, contract 6: **three clauses P8a shipped and stated but never observed.**
 *
 * None of them is new behaviour and none of them is a metric. Each was proved uncovered the same
 * way - by mutating the production code that implements it and re-running the whole P8a suite,
 * which stayed green - so each is a clause the documents assert and the build does not. Closing
 * them here rather than in `TaskMetricsTest` keeps the metrics file about metrics, and keeps this
 * one deletable in the unlikely event the clauses move.
 *
 * The mutation each test is written against is named in its own KDoc. A test that does not go red
 * against a stated wrong implementation is the thing this file exists to stop being written.
 *
 * Two further P8a clauses that revision 1 of this contract carried stay open and are recorded in
 * progress.md as such: `TaskRunner`'s caller identity, which needs a runner over a listened engine
 * and `SchedulingFixtures` is outside this phase's boundary, and the two start times, whose
 * diagnosis changed (contract 0) and which is nobody's phase yet.
 */
class P8aCoverageTest {

    @TempDir
    lateinit var root: Path

    /**
     * Contract 6.1: `TaskHooks.names` is a **live view**, not a snapshot.
     *
     * That is what lets a host write `TaskFileLoader(hooks = registry.names)` at startup without
     * depending on the order its registering beans happen to fire in - validation rule 5 then sees
     * every hook, whenever it was registered. The set is taken *before* the second registration
     * and read afterwards, through the same reference.
     *
     * RED against `Collections.unmodifiableSet(hooks.keys.toSet())`, or any other defensive copy:
     * the held set would still read `[first]`. No engine is involved; this is a property of
     * `TaskHooks` alone.
     */
    @Test
    fun hookNamesIsALiveViewAndNotASnapshot() {
        val registry = TaskHooks()
        registry.register("first", TaskHook { })

        val held = registry.names
        registry.register("second", TaskHook { })

        assertAll(
            {
                assertTrue("second" in held) {
                    "the set was taken before 'second' was registered and must see it; held $held"
                }
            },
            { assertEquals(setOf("first", "second"), held) },
        )
    }

    /**
     * Contract 6.2 (and acceptance criterion 3): `Events.isolate` catches `Exception`, **not**
     * `Throwable`.
     *
     * The distinction is load-bearing rather than stylistic, and P8b is what makes it so: it is
     * why every metrics call site is ordered *before* the listener call that describes the same
     * moment. A listener throwing an `Error` escapes the engine, so anything sequenced after it
     * never happens - and a metric recorded after such a listener would be a metric silently lost
     * on exactly the path an operator most needs it.
     *
     * The `Error` is thrown from `onStepStart`, which is recorded before it is thrown, so the
     * trace shows the site fired. It then unwinds past a phase catch that is also `Exception`-only
     * - hence no `onPhaseEnd` - and reaches `run`'s `finally`, which still reports the run ended
     * FAILED. Both halves matter: `assertThrows` alone would pass against an engine that caught
     * nothing anywhere, and the trace alone would pass against `catch (e: Throwable)`.
     *
     * RED against `catch (e: Throwable)` in `isolate`: the `Error` would be swallowed, the run
     * would succeed, and `assertThrows` would fail with "Expected ProbeError to be thrown".
     */
    @Test
    fun anErrorFromAListenerEscapesTheEngineAndTheRunStillReportsItEnded() {
        TaskHarness(root).use { harness ->
            harness.datasource("report_oracle")
            val trace = EventTrace()
            val thrower = trace.listener(failAt = setOf(ListenerCall.STEP_START)) { ProbeError() }
            harness.listener = thrower
            val definition = Etl.task(
                "wip-error",
                Etl.phase("extract", Etl.sql("touch", "report_oracle", "select 1")),
            )

            assertThrows<ProbeError> { harness.run(definition) }

            assertAll(
                {
                    assertEquals(
                        listOf(
                            "onTaskStart(wip-error)",
                            "onPhaseStart(extract)",
                            "onStepStart(extract/touch)",
                            "onTaskEnd(wip-error, FAILED)",
                        ),
                        trace.entries,
                    ) { "an Error skips onPhaseEnd; only onTaskEnd is reached, from a finally" }
                },
                {
                    assertEquals(1, thrower.thrown) {
                        "the site under test fired; an engine that never calls a listener cannot fail this"
                    }
                },
            )
        }
    }

    /**
     * Contract 6.3: a `materialize` step on a **non-scratch** datasource reports 0 rows read and 0
     * written, like every step type but `pipe`.
     *
     * P8a asserted the scratch branch only, and the two branches are different code: the
     * non-scratch one runs a `CREATE TABLE AS SELECT` through JDBI, which on DuckDB 1.1.3 hands
     * back an affected-row count of -1 and on another driver would hand back something else
     * entirely. Either number reaching `StepResult` would make one field mean "rows piped" for one
     * step type and "rows the database says it touched" for another.
     *
     * RED against a branch that returned the update count: -1 / -1 on this driver.
     */
    @Test
    fun aNonScratchMaterializeReportsNoRows() {
        TaskHarness(root).use { harness ->
            val reports = harness.datasource("report_oracle")
            val events = EventTrace().listener()
            harness.listener = events

            harness.runExpectingSuccess(
                Etl.task(
                    "wip-report",
                    Etl.phase(
                        "build",
                        Etl.materialize(
                            name = "build-report",
                            datasource = "report_oracle",
                            output = "report",
                            sql = "select 7 as ok",
                        ),
                    ),
                ),
            )

            val result = events.result("build-report")
            assertAll(
                { assertEquals(0L, result.rowsRead) { "no row passes through the JVM in a materialize step" } },
                { assertEquals(0L, result.rowsWritten) { "and none is written by this JVM either" } },
                {
                    assertEquals(7L, reports.longAt("select ok from report")) {
                        "the step really ran on the external datasource, so the 0/0 above is not " +
                            "the reading of a step that did nothing"
                    }
                },
            )
        }
    }
}

/** An `Error`, not an `Exception`, so `isolate`'s catch clause is the thing under test. */
private class ProbeError : Error("probe: a listener threw an Error")
