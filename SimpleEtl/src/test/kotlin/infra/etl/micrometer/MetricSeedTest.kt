package infra.etl.micrometer

import infra.etl.Etl
import infra.etl.TaskHarness
import infra.etl.task.CronScheduler
import infra.etl.task.EtlWiring
import infra.etl.task.Outcome
import infra.etl.task.TriggerSource
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.nio.file.Path
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir

/**
 * E15b: `MicrometerTaskMetrics.seed`, read out of a real `SimpleMeterRegistry` (spec 9.3).
 *
 * The defect is that `etl_task_runs_total` materialises on a task's **first run**, so a task that
 * has never succeeded emits no series and `etl_task_runs_total{outcome="succeeded"} == 0` matches
 * nothing - it does not fire and it does not error. Spec 8.6 states the workaround (alert on
 * absence); `seed` is the other half.
 *
 * ### The oracle is series *identity*, not series count
 *
 * [seededSeriesExistAtZeroAndARunIncrementsTheSameOne] would pass against a `seed` that registered
 * four differently-tagged series if it only asserted "a counter reads 1 after the run". So it
 * asserts the counter total stays at **four** across the run: a run that landed on a fifth series
 * because a tag value differed - `SCHEDULE` against `schedule`, say, and Prometheus label values
 * are case sensitive - shows up as five counters, not as a wrong number on one.
 *
 * ### Why the exclusion test is here
 *
 * Spec 9.3 excludes the other five meters per meter, and four of those exclusions are the reason
 * `seed` takes only task names. [seedingRegistersTheRunsCounterAndNothingElse] is what stops a
 * later session "completing" the seeding by adding a zero gauge for `etl_scratch_file_bytes`,
 * which would assert a measured footprint of zero bytes for a task that has never opened a file.
 */
class MetricSeedTest {

    @TempDir
    lateinit var root: Path

    private val registry = SimpleMeterRegistry()

    private val harness: TaskHarness by lazy { TaskHarness(root) }

    @AfterEach
    fun tearDown() {
        harness.close()
        registry.close()
    }

    /** The smallest run that needs no external datasource: one `sql` step on scratch. */
    private fun task(name: String) =
        Etl.task(name, Etl.phase("only", Etl.sql("touch", Etl.SCRATCH, "create table marker as select 1 as i")))

    private fun runsCounters() = registry.find("etl_task_runs_total").counters()

    private fun runsCount(task: String, trigger: String, outcome: String): Double =
        registry.find("etl_task_runs_total")
            .tags("task", task, "trigger", trigger, "outcome", outcome)
            .counter()
            ?.count()
            ?: error(
                "no etl_task_runs_total series for task=$task trigger=$trigger outcome=$outcome; " +
                    "present: " + runsCounters().map { it.id.tags.map { tag -> "${tag.key}=${tag.value}" } },
            )

    /**
     * The whole point of the phase, in one test: the series an alert compares against exists at 0
     * before anything has run, and the run then lands on **that** series.
     */
    @Test
    fun seededSeriesExistAtZeroAndARunIncrementsTheSameOne() {
        val metrics = MicrometerTaskMetrics(registry)
        metrics.seed(listOf("never-run"))

        val seeded = runsCounters().size
        val before = runsCount("never-run", "schedule", "succeeded")

        harness.metrics = metrics
        harness.runExpectingSuccess(task("never-run"), TriggerSource.SCHEDULE)

        assertAll(
            {
                assertEquals(
                    TriggerSource.entries.size * Outcome.entries.size,
                    seeded,
                ) { "one series per trigger/outcome pair, and no others" }
            },
            {
                assertEquals(0.0, before) {
                    "the staleness alert's own comparison must be answerable before the first run - " +
                        "that is the whole defect (spec 9.3)"
                }
            },
            {
                assertEquals(1.0, runsCount("never-run", "schedule", "succeeded")) {
                    "the run must increment the seeded series"
                }
            },
            {
                assertEquals(seeded, runsCounters().size) {
                    "the run landed on a NEW series instead of the seeded one - seed and taskEnded " +
                        "disagree about a tag value"
                }
            },
            {
                assertEquals(0.0, runsCount("never-run", "api", "failed")) {
                    "the three series the run did not touch stay at zero"
                }
            },
        )
    }

    /**
     * Re-seeding is what a host does after every reload (spec 8.6), by which time the process has
     * been running for weeks. A `seed` that re-registered would zero every counter in the fleet on
     * an operator's routine reload - a data loss with no error and no log line.
     */
    @Test
    fun reSeedingDoesNotResetACountAlreadyRecorded() {
        val metrics = MicrometerTaskMetrics(registry)
        metrics.seed(listOf("reloaded"))
        harness.metrics = metrics
        harness.runExpectingSuccess(task("reloaded"), TriggerSource.SCHEDULE)
        val afterRun = runsCount("reloaded", "schedule", "succeeded")

        metrics.seed(listOf("reloaded", "added-by-the-reload"))

        assertAll(
            { assertEquals(1.0, afterRun) { "the run was not counted, so the re-seed proves nothing" } },
            {
                assertEquals(1.0, runsCount("reloaded", "schedule", "succeeded")) {
                    "re-seeding an existing series must return the live counter, not a fresh zero"
                }
            },
            {
                assertEquals(0.0, runsCount("added-by-the-reload", "schedule", "succeeded")) {
                    "a name new to the reload is seeded like any other"
                }
            },
            {
                assertEquals(8, runsCounters().size) {
                    "two tasks, four series each; re-seeding a survivor must not add a series either"
                }
            },
        )
    }

    /**
     * Spec 9.3's exclusion table, asserted rather than left as prose. Seeding registers series of a
     * metric 9.3 already lists and adds no meter of its own, which is the argument that it is not a
     * seventh metric and that `TaskMetrics` stays closed.
     */
    @Test
    fun seedingRegistersTheRunsCounterAndNothingElse() {
        MicrometerTaskMetrics(registry).seed(listOf("never-run"))

        assertEquals(setOf("etl_task_runs_total"), registry.meters.map { it.id.name }.toSet()) {
            "seed must touch exactly one of spec 9.3's six meters - a seeded gauge would assert a " +
                "measured zero, and the step meters need a phase and a step no task name carries"
        }
    }

    /**
     * The depth-review deepening (2026-08-30): seeding is no longer a call-site obligation the
     * host pairs with load and reload by hand - the host passes `metrics::seed` once, as
     * `EtlWiring(onTasksLoaded = ...)`, and **the framework invokes it at both moments**.
     *
     * The discriminating property is the absence: this test contains **no call to `seed`**. If
     * the series exists at zero anyway, the framework made it exist - which is exactly what the
     * old spec 8.6 row claimed was impossible ("`infra.etl.task` may not name `io.micrometer`")
     * and what refuted it: invoking a `(Set<String>) -> Unit` names nothing.
     */
    @Test
    fun theFrameworkSeedsThroughOnTasksLoadedWithNoHostCallSite() {
        val metrics = MicrometerTaskMetrics(registry)
        val wired = EtlWiring(
            scratchDirectory = root.resolve("wiring-scratch"),
            cron = CronScheduler { _, _, _ -> AutoCloseable { } },
            metrics = metrics,
            onTasksLoaded = metrics::seed,
        ).start(listOf(task("wired-seeded")))

        assertEquals(
            0.0,
            runsCount("wired-seeded", "schedule", "succeeded"),
        ) { "the framework, not this test, must have seeded the series - no seed call exists here" }
        (wired as infra.etl.task.WiringResult.Wired).close()
    }
}
