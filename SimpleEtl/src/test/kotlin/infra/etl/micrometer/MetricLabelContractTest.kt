package infra.etl.micrometer

import infra.etl.Etl
import infra.etl.TaskHarness
import infra.etl.task.Outcome
import infra.etl.task.TriggerSource
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.nio.file.Path
import java.sql.SQLTransientException
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir

/** Spec 9.3's six meters, named here once so a typo in the test cannot agree with a typo in the code. */
private const val RUNS = "etl_task_runs_total"
private const val TASK_DURATION = "etl_task_duration_seconds"
private const val STEP_DURATION = "etl_step_duration_seconds"
private const val ROWS = "etl_step_rows_total"
private const val RETRIES = "etl_step_retries_total"
private const val SCRATCH = "etl_scratch_file_bytes"

/**
 * P8b, acceptance criteria 1, 4 and 5: **the label contract, read out of a real
 * `SimpleMeterRegistry` that a real engine run filled.**
 *
 * Nothing here asserts a constant against itself. The meters exist because two `TaskEngine` runs
 * went through `MicrometerTaskMetrics` into one registry, which is the only arrangement that can
 * catch a meter that is named correctly and never registered, or registered under a tag value the
 * host will see and the test never looked at.
 *
 * ### Two runs, and why each is shaped as it is
 *
 * Run 1 succeeds on `SCHEDULE`, retries a step twice, and pipes into scratch - a scratch pipe with
 * a retry is the one step shape that registers all six meters by itself. Run 2 fails **on a step**
 * on `API`; a run failed by a *hook* would never register `etl_step_*` at all and would leave the
 * failure half of this contract untested. Between them the `outcome` and `trigger` tags take both
 * of their values, which is what makes a value assertion possible - a key-only assertion passes
 * happily against `outcome="SUCCEEDED"`, and Prometheus label values are case sensitive.
 *
 * ### The order the assertions are in is load-bearing
 *
 * The outcomes are asserted first and sequentially rather than inside an `assertAll`: a run that
 * threw registers nothing, and every assertion after it would then be over an empty collection and
 * would fail with a message about meters instead of about the run. The name set comes next as a
 * single equality, which catches a typo, a missing meter and a stray seventh in one assertion.
 * Only then does anything map a meter to its tags, and each of those blocks proves the meter
 * exists before reading it.
 *
 * ### Two measured facts this file is written around
 *
 * - **`Meter.Id.getTags()` returns tags sorted by key**, not in spec 9.3's table order:
 *   `etl_step_rows_total` reads back `[direction, phase, step, task]`. Every assertion here is
 *   over a **set**; an assertion in table order would fail against correct code.
 * - **A `Timer` takes milliseconds in and reports seconds out.** `record(durationMs, SECONDS)` is
 *   off by a factor of 1000 and passes every name and tag assertion ever written, so the retried
 *   step's `totalTime(SECONDS)` is checked against the exact number the harness's injected clock
 *   fixes: 6.0, being the 2s and 4s of backoff the sleeper was asked for.
 *
 * A `Timer` also exports as `_count` + `_sum` plus a separate `_max` **gauge** on a Prometheus
 * scrape. That is a scrape artefact and not a seventh `Meter`: `registry.meters` holds six, which
 * is what the name assertion below relies on.
 */
class MetricLabelContractTest {

    @TempDir
    lateinit var root: Path

    @Test
    fun twoRealRunsRegisterExactlyTheSixMetersOfSpecNineThreeWithTheirTagsAndValues() {
        val registry = SimpleMeterRegistry()
        TaskHarness(root).use { harness ->
            harness.metrics = MicrometerTaskMetrics(registry)
            val mes = harness.datasource("oracle_mes")
            mes.createSourceTable("wip", rows = 6, marker = "w")
            mes.failFirst(count = 2, afterRows = 2) { SQLTransientException("probe: transient") }
            harness.datasource("report_oracle")

            val succeeded = harness.run(
                Etl.task(
                    "wip-labels",
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
                TriggerSource.SCHEDULE,
            )
            val failed = harness.run(
                Etl.task(
                    "wip-broken",
                    // A real DuckDB syntax error: non-transient, so this is one attempt and a
                    // terminal step failure - not a hook failure, which registers no step meter.
                    Etl.phase("build", Etl.sql("bad-step", "report_oracle", "this is not sql")),
                ),
                TriggerSource.API,
            )

            // Guards, in order. Sequential on purpose - see the class KDoc.
            assertEquals(Outcome.SUCCEEDED, succeeded.outcome) { "run 1: ${succeeded.failure}" }
            assertEquals(Outcome.FAILED, failed.outcome) { "run 2 must fail on its step" }
            assertEquals(
                setOf(RUNS, TASK_DURATION, STEP_DURATION, ROWS, RETRIES, SCRATCH),
                registry.meters.map { it.id.name }.toSet(),
            ) { "exactly spec 9.3's six meters, no more and no fewer" }

            assertAll(
                { assertTagKeys(registry, RUNS, setOf("task", "trigger", "outcome")) },
                { assertTagKeys(registry, TASK_DURATION, setOf("task")) },
                { assertTagKeys(registry, STEP_DURATION, setOf("task", "phase", "step")) },
                { assertTagKeys(registry, ROWS, setOf("task", "phase", "step", "direction")) },
                { assertTagKeys(registry, RETRIES, setOf("task", "phase", "step")) },
                { assertTagKeys(registry, SCRATCH, setOf("task")) },
            )

            assertAll(
                {
                    assertEquals(setOf("succeeded", "failed"), tagValues(registry, RUNS, "outcome")) {
                        "enum tag values are name.lowercase(), used verbatim"
                    }
                },
                {
                    assertEquals(setOf("schedule", "api"), tagValues(registry, RUNS, "trigger")) {
                        "both triggers of spec 8.2, lowercased the same way"
                    }
                },
                {
                    assertEquals(setOf("wip-labels", "wip-broken"), tagValues(registry, RUNS, "task")) {
                        "the task tag is the definition's own name, unsanitised and unprefixed"
                    }
                },
                {
                    assertEquals(setOf("read", "written"), tagValues(registry, ROWS, "direction")) {
                        "both directions of the row counter"
                    }
                },
            )

            val stepTimer = registry.get(STEP_DURATION)
                .tags("task", "wip-labels", "phase", "extract", "step", "load-wip")
                .timer()
            assertAll(
                {
                    assertEquals(6.0, stepTimer.totalTime(TimeUnit.SECONDS), 1e-9) {
                        "milliseconds in, seconds out. The step spanned 2s + 4s of injected backoff, " +
                            "so a Timer fed SECONDS instead of MILLISECONDS reads 6000.0 here"
                    }
                },
                { assertEquals(1L, stepTimer.count()) { "one record per step that ended, not per attempt" } },
                {
                    assertEquals(
                        2.0,
                        registry.get(RETRIES)
                            .tags("task", "wip-labels", "phase", "extract", "step", "load-wip")
                            .counter().count(),
                    ) { "etl_step_retries_total counts retried attempts, not retried steps" }
                },
                {
                    // Measured: increment(0.0) registers the meter with count() == 0.0. A step that
                    // moved no row still needs both series, or its task's dashboard has a hole in it.
                    assertEquals(
                        0.0,
                        registry.get(ROWS)
                            .tags("task", "wip-broken", "phase", "build", "step", "bad-step", "direction", "written")
                            .counter().count(),
                    ) { "both direction series are registered on every step that ends, including at 0" }
                },
            )
        }
    }

    /**
     * Criteria 4 and 5: `etl_scratch_file_bytes` is **one strongly held gauge per task name**,
     * registered once and updated per run.
     *
     * Both runs are of the **same task name** deliberately. With two names the meter ids differ,
     * the second registration never happens, and the criterion would pass against exactly the bug
     * it targets. Measured (lead M3): Micrometer holds a gauge's referent weakly, so a gauge over
     * a locally scoped object reads `NaN` once it is collected; and re-registering an id is
     * ignored with a WARNING, keeping the first object. A per-run `registry.gauge(...)` therefore
     * fails this either way - run 1's value if the referent is still live, `NaN` if it is not -
     * without the test depending on when a GC happens.
     *
     * The first run's `> 0` is also the assertion that catches the severe ordering bug of contract
     * 3.2: sample after `ScratchDb.close()` and the directory is already empty, so the gauge reads
     * 0 on every run forever and an operator sizing spec 7.2's volume sees a flat zero. The second
     * run's exact `0.0` is criterion 5, and on its own it is the *symptom* of that bug rather than
     * a guard against it - which is why the two are one test.
     */
    @Test
    fun theScratchGaugeIsRegisteredOncePerTaskAndReportsTheLatestRun() {
        val registry = SimpleMeterRegistry()
        TaskHarness(root).use { harness ->
            harness.metrics = MicrometerTaskMetrics(registry)
            harness.datasource("oracle_mes").createSourceTable("wip", rows = 6, marker = "w")
            harness.datasource("report_oracle")

            harness.runExpectingSuccess(
                Etl.task(
                    "wip-gauge",
                    Etl.phase(
                        "extract",
                        Etl.pipe("load-wip", "oracle_mes", "select lot_id, lot_code, qty from wip", "wip_stg"),
                    ),
                ),
            )
            val afterScratch = scratchGauge(registry)

            // Same task NAME, a shape that never mentions scratch, so ScratchDb is never opened.
            harness.runExpectingSuccess(
                Etl.task("wip-gauge", Etl.phase("check", Etl.sql("noop", "report_oracle", "select 1"))),
            )
            val afterNoScratch = scratchGauge(registry)

            assertAll(
                {
                    assertTrue(afterScratch > 0.0) {
                        "the run wrote six rows into a DuckDB file that is still on disk when the " +
                            "sample is taken; the gauge read $afterScratch"
                    }
                },
                {
                    assertEquals(0.0, afterNoScratch, 0.0) {
                        "the second run of the same task never opened scratch, and the one gauge " +
                            "that already exists has to follow it down; it read $afterNoScratch"
                    }
                },
                {
                    assertEquals(1, registry.find(SCRATCH).gauges().size) {
                        "one gauge per task name, registered once: " +
                            "${registry.find(SCRATCH).gauges().map { it.id }}"
                    }
                },
            )
        }
    }

    /** Proves the meter exists before reading its tags, so a tag assertion is never over nothing. */
    private fun assertTagKeys(registry: MeterRegistry, name: String, keys: Set<String>) {
        val meters = registry.meters.filter { it.id.name == name }
        assertTrue(meters.isNotEmpty()) {
            "no meter named '$name' was registered; the registry holds " +
                "${registry.meters.map { it.id.name }.toSet()}"
        }
        assertEquals(setOf(keys), meters.map { meter -> meter.id.tags.map { it.key }.toSet() }.toSet()) {
            "every '$name' series carries exactly those tag keys - asserted as a set, because " +
                "Meter.Id.getTags() sorts by key rather than keeping spec 9.3's table order"
        }
    }

    private fun tagValues(registry: MeterRegistry, name: String, key: String): Set<String?> =
        registry.meters.filter { it.id.name == name }.map { it.id.getTag(key) }.toSet()

    private fun scratchGauge(registry: MeterRegistry): Double =
        registry.get(SCRATCH).tags("task", "wip-gauge").gauge().value()
}
