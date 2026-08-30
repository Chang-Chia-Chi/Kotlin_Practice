package infra.etl.micrometer

import infra.etl.task.Outcome
import infra.etl.task.StepContext
import infra.etl.task.StepResult
import infra.etl.task.TaskContext
import infra.etl.task.TaskMetrics
import infra.etl.task.TriggerSource
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/** Spec 9.3's six meter names, used verbatim - no sanitising, no prefix, no registry convention. */
private const val RUNS = "etl_task_runs_total"
private const val TASK_DURATION = "etl_task_duration_seconds"
private const val STEP_DURATION = "etl_step_duration_seconds"
private const val ROWS = "etl_step_rows_total"
private const val RETRIES = "etl_step_retries_total"
private const val SCRATCH = "etl_scratch_file_bytes"

/**
 * Spec 9.3's six meters, bound to Micrometer. The only class in `infra.etl` that names
 * `io.micrometer`, and ArchUnit keeps it that way.
 *
 * `micrometer-core` is a **`provided`** dependency, not a compile one, because spec 2.1's whole
 * reason for existing is that Layer 1 ships to the snapshot cache without Layer 2 and Maven has no
 * layer granularity: at compile scope every Layer 1 consumer would inherit Micrometer. Measured on
 * a throwaway two-module reactor - a consumer of a `provided`-scoped library resolved **zero**
 * micrometer artefacts, while all five were on the library's own test classpath. A host wiring
 * this class necessarily already has a [MeterRegistry], so it necessarily already has the jar.
 *
 * ### What the host will see on a scrape
 *
 * Measured on micrometer 1.14.2 through `PrometheusMeterRegistry.scrape()`:
 *
 * - The names above export **verbatim**. The feared `etl_task_runs_total_total` does not happen -
 *   Micrometer recognises the `_total` a counter already carries and does not double it.
 * - A `Timer` exports as `_count` + `_sum` plus a separate `_max` **gauge**. That `_max` is a
 *   scrape artefact and not a seventh meter; `registry.meters` holds six.
 * - **Timers take milliseconds in and report seconds out.** Every duration this class is handed is
 *   a `durationMs`, so every `record` states [TimeUnit.MILLISECONDS]. Recording it as SECONDS is
 *   off by a factor of 1000 and passes every name and tag assertion ever written.
 * - `Meter.Id.getTags()` returns tags sorted by **key**, not in spec 9.3's table order (measured
 *   on 1.14.2 in the P8b review round, `SimpleMeterRegistry`, printed verbatim):
 *   `etl_step_rows_total` reads back `[direction, phase, step, task]`.
 *
 * ### Two operational consequences worth knowing before an alert is written against these
 *
 * A task's meters are registered on its **first run**, so a newly deployed task has no series at
 * all until it fires - an alert on the absence of a series will fire on deployment. [seed] closes
 * that for `etl_task_runs_total`, which is the meter staleness alerts are written against, and for
 * that one only; spec 9.3 states why each of the other five stays absence-only. And **this
 * binding** never removes one - `MeterRegistry.remove` and `clear` do exist and were measured to
 * work; nothing here calls them - so a task renamed across a reload leaves a stale
 * `etl_scratch_file_bytes{task=old}` behind forever, reading whatever its last run reported.
 *
 * Thread safe as [TaskMetrics] requires: [MeterRegistry] is, [ConcurrentHashMap] is, and
 * [AtomicLong] is. Nothing here blocks.
 */
class MicrometerTaskMetrics(private val registry: MeterRegistry) : TaskMetrics {

    /**
     * One strongly held [AtomicLong] per task name, which is what makes the scratch gauge work at
     * all.
     *
     * Measured (micrometer 1.14.2): the registry holds a gauge's referent **weakly**, so a gauge
     * over a locally scoped object read 99.0 before the reference was dropped and `NaN` after a
     * collection; and re-registering the same id is **ignored with a WARNING**, leaving the meter
     * reading the first object forever. A per-run `registry.gauge(...)` therefore fails either
     * way - stuck on run 1's value if the referent happens to still be live, `NaN` if it is not.
     *
     * The registration happens **inside** the `computeIfAbsent` mapping lambda rather than behind
     * an external "did I create it?" branch, because that branch is not atomic and reintroduces
     * exactly the double registration above under two tasks of one name starting together.
     */
    private val scratchGauges = ConcurrentHashMap<String, AtomicLong>()

    override fun taskEnded(ctx: TaskContext, outcome: Outcome, durationMs: Long) {
        runs(ctx.taskName, ctx.triggerSource, outcome).increment()
        registry.timer(TASK_DURATION, "task", ctx.taskName).record(durationMs, TimeUnit.MILLISECONDS)
    }

    /**
     * Pre-registers the zero-value [RUNS] series for every task in [taskNames], so a staleness
     * alert can be written as `etl_task_runs_total{outcome="succeeded"} == 0` rather than as an
     * absence query (spec 9.3, 8.6).
     *
     * Four series per name: [TriggerSource] and [Outcome] are closed two-valued enums, which is
     * what makes this meter the only one of spec 9.3's six whose full label set follows from a
     * task name. The other five need a phase and a step, or are a gauge whose zero would be a
     * measurement rather than a placeholder - 9.3 states the exclusion per meter.
     *
     * **The host calls this, after the initial load and after every reload** (spec 8.6). It cannot
     * be called from `infra.etl.task`, which may not name `io.micrometer`, and it is deliberately
     * not on the [TaskMetrics] interface: seeding series of a metric 9.3 already lists is not a
     * seventh metric, so nothing that implements that interface breaks.
     *
     * **Idempotent.** `MeterRegistry.counter` is get-or-create on the meter id, so re-seeding a
     * series that has since been incremented returns the same [io.micrometer.core.instrument.Counter]
     * with its value intact - it does not re-register and it does not reset. That is what lets the
     * reload call pass the whole name set, survivors and additions alike, instead of computing a
     * delta. It goes with the class's standing behaviour of never removing a meter: a task dropped
     * by a reload keeps the four zero series it was seeded with.
     *
     * Registers through the same [runs] helper [taskEnded] uses, so a seeded series and the series
     * a run increments cannot drift apart into two meter ids that differ by a tag.
     */
    fun seed(taskNames: Collection<String>) {
        for (task in taskNames) {
            for (trigger in TriggerSource.entries) {
                for (outcome in Outcome.entries) {
                    runs(task, trigger, outcome)
                }
            }
        }
    }

    private fun runs(task: String, trigger: TriggerSource, outcome: Outcome) = registry.counter(
        RUNS,
        "task", task,
        "trigger", trigger.name.lowercase(),
        "outcome", outcome.name.lowercase(),
    )

    /**
     * **Both** row directions are registered on every step that ends, including at 0 - measured,
     * `increment(0.0)` registers the meter with `count() == 0.0` - measured on 1.14.2, the meter is
     * findable afterwards. A step that moved no row and had
     * no series at all would leave a hole in its task's dashboard exactly where an operator looks
     * first. `read` is emitted before `written` so the two halves of a pair agree on order.
     */
    override fun stepEnded(ctx: StepContext, result: StepResult) {
        registry.timer(
            STEP_DURATION,
            "task", ctx.task.taskName,
            "phase", ctx.phase,
            "step", ctx.step,
        ).record(result.durationMs, TimeUnit.MILLISECONDS)
        rows(ctx, "read").increment(result.rowsRead.toDouble())
        rows(ctx, "written").increment(result.rowsWritten.toDouble())
    }

    override fun stepRetried(ctx: StepContext) {
        registry.counter(
            RETRIES,
            "task", ctx.task.taskName,
            "phase", ctx.phase,
            "step", ctx.step,
        ).increment()
    }

    override fun scratchBytes(ctx: TaskContext, bytes: Long) {
        scratchGauges.computeIfAbsent(ctx.taskName) { task ->
            AtomicLong().also { holder ->
                Gauge.builder(SCRATCH, holder) { it.get().toDouble() }
                    .tags(listOf(Tag.of("task", task)))
                    .register(registry)
            }
        }.set(bytes)
    }

    private fun rows(ctx: StepContext, direction: String) = registry.counter(
        ROWS,
        "task", ctx.task.taskName,
        "phase", ctx.phase,
        "step", ctx.step,
        "direction", direction,
    )
}
