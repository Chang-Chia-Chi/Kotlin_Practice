package host

import infra.etl.task.EtlWiring
import infra.etl.task.Outcome
import infra.etl.task.TaskAdmin
import infra.etl.task.TaskEvent
import infra.etl.task.TaskRunListener
import infra.etl.task.TriggerResult
import infra.etl.task.WiringResult
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * M1 and M2: the two measurements that turn a recommendation and a budget formula into numbers.
 *
 * Neither needs the snapshot cache, so neither uses [ComposedHost]. What they need is the *real*
 * `EtlWiring` -> `TaskRunner` -> `TaskEngine` -> `ScratchDb` path, which is what [EtlOnly] is: the
 * same front door spec 11.2 declares, with no cache bound and nothing doubled.
 *
 * **Absolute numbers here are machine-relative.** They were measured on one Windows 11 laptop,
 * Java 21, duckdb_jdbc 1.1.3. What travels to another machine is the *shape* of the answer - the
 * ratio in M1, the enforcement/additivity facts in M2 - not the milliseconds.
 */
@Tag("measurement")
class MeasurementsTest {

    // ------------------------------------------------------------------ M1: coroutine debug cost

    /**
     * **M1.** Both specs now recommend `-Dkotlinx.coroutines.debug=on` so that `LeaseInfo.owner`
     * reads `DefaultDispatcher-worker-1 @wip-summary#5` instead of the bare worker name. The flag
     * is not free: kotlinx-coroutines then installs a `CoroutineId` context element and *renames
     * the carrier thread on every dispatch and every resume*. This measures what that costs on the
     * one path a host actually pays it on - a task run.
     *
     * The property is read once, at the class-init of kotlinx-coroutines' `DebugKt`, so the two
     * states cannot share a JVM. This test measures **whichever state its own JVM is in** and
     * prints it; the comparison is made by running it twice:
     *
     * ```
     * mvn -o -pl composed-host-example test -Dtest=MeasurementsTest#M1* \
     *     -DfailIfNoSpecifiedTests=false -DextraArgs="-da -Dkotlinx.coroutines.debug=off"
     * mvn -o -pl composed-host-example test -Dtest=MeasurementsTest#M1* \
     *     -DfailIfNoSpecifiedTests=false -DextraArgs="-da -Dkotlinx.coroutines.debug=on"
     * ```
     *
     * `-da` is on **both** sides deliberately. Surefire's default `-ea` turns the coroutines DEBUG
     * property's `AUTO` default into `on`, so a plain `mvn test` and a `-DextraArgs=-da` run differ
     * in *two* variables, not one. Disabling assertions on both sides and setting the property
     * explicitly leaves exactly one.
     *
     * The task is spec 2.4 shape A - one `sql` step on a non-scratch datasource - on purpose. A
     * step that touches `scratch` opens a fresh DuckDB file per run, which costs tens of
     * milliseconds and would bury the thing being measured. What is left is what the framework
     * itself spends per run: one `submit`, one `launch` onto the task's
     * `limitedParallelism(1)` view, the engine body, and one `invokeOnCompletion`.
     *
     * ### Measured, 2026-08-30 (Windows 11, Java 21, kotlinx-coroutines 1.10.1)
     *
     * Three separate JVMs per state, 3,000 warm-up runs then 7 x 1,000 timed runs each. The
     * headline statistic is the **median of the seven round medians**, and the table reports the
     * median across the three JVMs of that:
     *
     * | state | round-median (us/task) | pooled median | pooled min |
     * |---|---|---|---|
     * | `debug=off` | 298.5 / 301.7 / 302.1 -> **301.7** | 302.9 | 209.2 |
     * | `debug=on`  | 314.3 / 304.5 / 308.1 -> **308.1** | 307.1 | 206.6 |
     *
     * **+6.4 us per task, +2.1%** - and that is an upper bound, not a reading. Within one JVM the
     * seven round medians spread over ~90 us (258 -> 348), the JVM-to-JVM spread of one state is
     * ~4 us, and the *pooled minimum* moves the other way (-2.6 us). The effect is at or below this
     * harness's resolution; what the three JVMs per state do establish is that it is **under 10 us
     * and under 3% of a task run**, which is the number worth quoting.
     *
     * That is the shape the mechanism predicts. The flag is charged per dispatch and per resume -
     * a `CoroutineId` context element and a `Thread.setName` - and this framework dispatches
     * roughly twice per run (the `launch`, then `invokeOnCompletion`). There is no amount of task
     * throughput that makes two thread renames matter: at one task every ten minutes per spec 8.1,
     * a host pays under a microsecond an hour.
     *
     * **Conclusion: recommend it unconditionally.** The flag was already the only way
     * `LeaseInfo.owner` names the task rather than a worker index, and its cost is not measurable
     * against the work a task does. The condition worth stating is not about cost at all: it is
     * that `-ea` *also* turns it on, so a JVM run without assertions and without this flag is the
     * one state in which attribution silently degrades - which is exactly what production is.
     */
    @Test
    fun `M1 - what -Dkotlinx coroutines debug costs per task run`(@TempDir root: Path) {
        // 3,000 warm-up runs, not 300: at 300 the seven round medians were still falling run over
        // run (355 -> 265 us), which is JIT, not the flag. A drifting baseline cannot be
        // A/B-compared across two JVMs at all.
        val warmup = intProperty("m1.warmup", 3_000)
        val perRound = intProperty("m1.n", 1_000)
        val rounds = intProperty("m1.rounds", 7)

        EtlOnly(root).use { host ->
            Files.writeString(host.tasks.resolve("ping.yaml"), PING_YAML)
            host.start()

            repeat(warmup) { host.runOnce("ping") }

            val all = mutableListOf<Long>()
            val roundMedians = (1..rounds).map {
                val samples = LongArray(perRound) { host.runOnce("ping") }
                all += samples.toList()
                median(samples.toList())
            }

            println("[M1] kotlinx.coroutines.debug property   = " + System.getProperty("kotlinx.coroutines.debug"))
            println("[M1] assertions enabled (-ea)?           = " + javaClass.desiredAssertionStatus())
            println("[M1] DEBUG effective? run thread name    = " + host.listener.lastThread)
            println("[M1]   (a '@ping#N' suffix means debug is ON - that suffix IS the cost)")
            println("[M1] warmup=" + warmup + " rounds=" + rounds + " x " + perRound + " runs")
            println("[M1] per-round medians (us/task)         = " + roundMedians.map { us(it) })
            println("[M1] MEDIAN OF ROUND MEDIANS (us/task)   = " + us(median(roundMedians)))
            println("[M1] pooled median / p90 (us/task)       = " + us(median(all)) + " / " + us(percentile(all, 90)))
            println("[M1] pooled min (us/task)                = " + us(all.min()))

            assertThat(host.listener.errors).isEmpty()
            assertThat(host.outcomeOfLast("ping")).isEqualTo(Outcome.SUCCEEDED)
        }
    }

    // ------------------------------------------- M2, premise 1: the limit is actually enforced

    /**
     * **M2 / enforcement.** The composed pod-budget formula
     * `N x EtlWiring.scratchMemoryLimitMb + servingMemoryLimit` assumes the first term is a real
     * bound. It is a `SET memory_limit` on a DuckDB 1.1.3 instance and nothing else, so this runs
     * a query that cannot fit in a deliberately tiny limit and records what 1.1.3 does: spill into
     * `ScratchDb`'s wired temp directory, or raise.
     *
     * Technique borrowed from `S4bSpillFactorSpike` (not run - read): spill space is released the
     * instant the query ends, so only a **sampled peak** means anything. An end-of-run reading is
     * always zero.
     *
     * The readback step publishes before the oversized one runs, so the limit that was in force is
     * observable whether or not the big query survives.
     *
     * ### Measured, 2026-08-30. The premise holds - and the formula is missing a term.
     *
     * Same query, one hash aggregate over N distinct keys, `-Dm2.limitMb` / `-Dm2.groups`:
     *
     * | memory_limit | distinct keys | outcome | peak spill |
     * |---|---|---|---|
     * | 64 MB   | 10,000,000 | ok, spilled | 3,717 MB (58x the limit) |
     * | 256 MB  | 10,000,000 | ok, spilled | 737 MB (2.9x) |
     * | 1024 MB | 10,000,000 | ok, in memory | 0 |
     * | 64 MB   | 40,000,000 | ok, spilled | 14,874 MB (232x) |
     *
     * **Enforcement: yes.** DuckDB 1.1.3 honours the setting, and what it does when a query cannot
     * fit is spill into `ScratchDb`'s wired temp directory rather than raise or grow. Not one of
     * these runs failed. The `N x scratchMemoryLimitMb` term of the pod budget is a real bound.
     *
     * **The surprise, and it is the operationally important half:** *shrinking the limit does not
     * shrink the run, it converts RAM into scratch-volume bytes at a very poor exchange rate.*
     * Dropping 256 MB to 64 MB - 192 MB of pod memory saved - cost **3 GB of extra spill** on the
     * same query, because DuckDB re-partitions an external aggregate more times the less memory it
     * has. A host tuning `scratchMemoryLimitMb` down to fit more tasks per pod is trading against a
     * term the budget formula does not contain. Spec 7.2 already sizes the scratch volume as file
     * plus spill; this is the exchange rate between the two budgets, measured.
     *
     * Peak is an *apparent* size (`Files.size`), and DuckDB's temp block file may be sparse. Two
     * things say the number is real work rather than preallocation: it scales exactly 4x with a 4x
     * input at a fixed limit, and it *falls* 5x when the limit rises 4x at fixed input - neither is
     * how a preallocated file behaves.
     */
    @Test
    fun `M2 - a small scratch memory_limit bounds the query that exceeds it`(@TempDir root: Path) {
        val limitMb = intProperty("m2.limitMb", 64)
        val groups = intProperty("m2.groups", 10_000_000)

        EtlOnly(root).use { host ->
            host.exec("report", "CREATE TABLE m2_settings (who VARCHAR, ml VARCHAR)")
            Files.writeString(host.tasks.resolve("bounded.yaml"), boundedYaml(limitMb, groups))
            host.start()

            val sampler = SpillSampler(host.scratchRoot)
            val outcome = host.runOnceSampling("bounded", sampler)

            val readback = host.query("report", "SELECT ml FROM m2_settings")
            println("[M2-ENFORCE] task asked for memoryLimitMb  = " + limitMb)
            println("[M2-ENFORCE] scratch reported memory_limit = " + readback)
            println("[M2-ENFORCE]   (DuckDB reads MB as 10^6 and echoes back in binary units)")
            println("[M2-ENFORCE] query: hash aggregate over " + groups + " distinct keys")
            println("[M2-ENFORCE] outcome = " + outcome.outcome)
            println("[M2-ENFORCE] failure = " + (outcome.failure?.toString()?.take(300) ?: "none"))
            println("[M2-ENFORCE] PEAK spill sampled under the run's temp_directory = " + mb(sampler.peak) + " MB")
            println("[M2-ENFORCE]   = " + "%.1f".format(sampler.peak.toDouble() / limitMb / 1024 / 1024) +
                "x the memory_limit it was bounded to  <- the DISK term the budget formula omits")
            println("[M2-ENFORCE]   (apparent size; DuckDB's temp block file may be sparse. -Dm2.groups=N to scale)")
            println("[M2-ENFORCE] spill left on disk after the run = " + mb(SpillSampler(host.scratchRoot).sample()) + " MB")

            // The premise, stated as an assertion: the limit reached the instance, and the query
            // that could not fit in it did NOT get to grow the heap silently.
            assertThat(readback).hasSize(1)
            assertThat(readback.single()).isNotEqualTo("0 bytes")
            assertThat(sampler.peak > 0 || outcome.outcome == Outcome.FAILED)
                .describedAs("a query far over the limit must either spill or fail, not just allocate")
                .isTrue()
            // Whatever happened, ScratchDb emptied the run directory (spec 7.2's only reclaim point).
            assertThat(SpillSampler(host.scratchRoot).sample()).isZero()
        }
    }

    // ------------------------------------------------ M2, premise 2: the limits are per-instance

    /**
     * **M2 / additivity.** The formula's `N x` assumes each concurrent task gets its own limit, not
     * a share of one. `memory_limit` is a *database-level* setting and spec 7.2 gives every run its
     * own [infra.etl.duckdb.ScratchDb] - so the claim is that two concurrent runs configured
     * differently each read back their own value, unaffected by the other's existence.
     *
     * Two things have to be true at once for that to be measured rather than asserted:
     *
     * 1. **The runs really overlap.** Proven twice - the test samples the scratch root and sees two
     *    live run directories (scenario 5's technique), and each task materialises `current_timestamp`
     *    at readback so the two readback instants can be checked to fall inside the other run's window.
     * 2. **The values differ.** 256 MB and 512 MB, not two of the same, so a shared setting would
     *    show up as one value rather than as a coincidence.
     *
     * Each task publishes into its **own** report database. One shared JDBC `Connection` written
     * from two task threads is spec 7.2's JVM-crash hazard, not a test flake.
     *
     * ### Measured, 2026-08-30: additive, with nothing left to interpret.
     *
     * Two live scratch run directories at the same instant; `mem-a` read back `244.1 MiB` and
     * `mem-b` `488.2 MiB` (DuckDB reads `MB` as a power of ten and echoes binary units, so those
     * *are* 256 and 512 honoured); both readbacks timestamped inside the other run's window. The
     * `N x` of the budget formula is per-instance, as `memory_limit` being a database-level setting
     * and spec 7.2 giving every run its own instance predicts.
     *
     * **What this does NOT measure, and no test in this repository can:** the operating point. That
     * N tasks at M megabytes each plus the serving limit *fits a given pod* is a statement about a
     * deployment's memory request, its page cache, its JVM heap and its actual concurrency - none
     * of which are framework facts, and all of which are the host's configuration. What is measured
     * here is only that the two terms are real and do not interfere. Choosing them is still sizing
     * work, and the M2-enforcement measurement above says the choice has a second axis: the scratch
     * volume pays for whatever the memory term does not.
     */
    @Test
    fun `M2 - two concurrent tasks each get their own scratch memory_limit`(@TempDir root: Path) {
        val burn = intProperty("m2.burn", 400_000_000)

        EtlOnly(root).use { host ->
            listOf("report_a", "report_b").forEach {
                host.exec(it, "CREATE TABLE m2_additivity (who VARCHAR, ml VARCHAR, at_ms BIGINT)")
            }
            Files.writeString(host.tasks.resolve("mem-a.yaml"), additivityYaml("mem-a", 256, "report_a", burn))
            Files.writeString(host.tasks.resolve("mem-b.yaml"), additivityYaml("mem-b", 512, "report_b", burn))
            host.start()

            val a = host.admin.trigger("mem-a", "tester") as TriggerResult.Accepted
            val b = host.admin.trigger("mem-b", "tester") as TriggerResult.Accepted

            // Overlap, proof 1: two live scratch run directories at the same instant.
            var peakLive = 0
            val deadline = System.nanoTime() + 300_000_000_000L
            while (System.nanoTime() < deadline) {
                peakLive = maxOf(peakLive, liveScratchDirs(host.scratchRoot))
                val done = host.admin.run("mem-a", a.runId) != null && host.admin.run("mem-b", b.runId) != null
                if (done) break
                Thread.onSpinWait()
            }
            host.awaitOutcome("mem-a", a.runId)
            host.awaitOutcome("mem-b", b.runId)

            val rowsA = host.query("report_a", "SELECT who || ' | ' || ml || ' | ' || at_ms FROM m2_additivity")
            val rowsB = host.query("report_b", "SELECT who || ' | ' || ml || ' | ' || at_ms FROM m2_additivity")
            val atA = host.query("report_a", "SELECT at_ms FROM m2_additivity").single().toLong()
            val atB = host.query("report_b", "SELECT at_ms FROM m2_additivity").single().toLong()
            val windows = host.admin.list().associate { it.name to it.lastRun }
            val overlapFrom = windows.values.maxOf { it!!.startedAt.toEpochMilli() }
            val overlapTo = windows.values.minOf { it!!.finishedAt!!.toEpochMilli() }

            println("[M2-ADDITIVE] peak concurrent scratch run directories = " + peakLive)
            println("[M2-ADDITIVE] mem-a asked 256 MB, read back: " + rowsA)
            println("[M2-ADDITIVE] mem-b asked 512 MB, read back: " + rowsB)
            windows.forEach { (name, run) ->
                println("[M2-ADDITIVE] " + name + " ran " + run?.startedAt + " .. " + run?.finishedAt)
            }
            println("[M2-ADDITIVE] overlap window (epoch ms) = " + overlapFrom + " .. " + overlapTo +
                "  readbacks at " + atA + " / " + atB)

            assertThat(peakLive).isEqualTo(2)
            // Both readbacks happened while BOTH runs were live: the settings are per-instance
            // at an instant when two instances existed, not one after the other.
            assertThat(atA).isBetween(overlapFrom, overlapTo)
            assertThat(atB).isBetween(overlapFrom, overlapTo)
            assertThat(rowsA.single()).contains("mem-a").contains("244.1")   // 256 MB, binary echo
            assertThat(rowsB.single()).contains("mem-b").contains("488.2")   // 512 MB, binary echo
            assertThat(host.outcomeOfLast("mem-a")).isEqualTo(Outcome.SUCCEEDED)
            assertThat(host.outcomeOfLast("mem-b")).isEqualTo(Outcome.SUCCEEDED)
        }
    }
}

// ------------------------------------------------------------------------------- the harness

/** Everything a [TaskRunListener] has to do for these two measurements, and nothing more. */
private class Latching : TaskRunListener {
    @Volatile var ended = CountDownLatch(1)
    @Volatile var lastThread: String = "(never ran)"
    val errors = CopyOnWriteArrayList<TaskEvent.StepError>()

    override fun on(event: TaskEvent) {
        when (event) {
            is TaskEvent.StepError -> errors += event
            is TaskEvent.TaskEnd -> {
                lastThread = Thread.currentThread().name
                ended.countDown()
            }
            else -> Unit
        }
    }
}

/**
 * `EtlWiring` with no cache bound: spec 2.1's Layer-2-alone host. Three DuckDB report databases,
 * one per datasource name, because a `Connection` shared by two task threads crashes the JVM.
 */
private class EtlOnly(root: Path, scratchMemoryLimitMb: Int = 4096) : AutoCloseable {

    val listener = Latching()
    val tasks: Path = root.resolve("tasks").also { Files.createDirectories(it) }
    val scratchRoot: Path = root.resolve("scratch").also { Files.createDirectories(it) }

    private val connections: Map<String, Connection> = listOf("report", "report_a", "report_b")
        .associateWith { DriverManager.getConnection("jdbc:duckdb:" + root.resolve("$it.db")) }

    private val wiring = EtlWiring(
        scratchDirectory = scratchRoot,
        cron = ManualCron(),
        datasources = connections.mapValues { Jdbi.create(it.value) },
        scratchMemoryLimitMb = scratchMemoryLimitMb,
        listener = listener,
    )

    lateinit var admin: TaskAdmin
        private set

    fun start() {
        val result = wiring.start(tasks)
        check(result is WiringResult.Wired) { "wiring failed: $result" }
        admin = result.admin
    }

    fun exec(datasource: String, sql: String) =
        connections.getValue(datasource).createStatement().use { it.execute(sql) }

    fun query(datasource: String, sql: String): List<String> =
        connections.getValue(datasource).createStatement().use { st ->
            st.executeQuery(sql).use { rows ->
                buildList { while (rows.next()) add(rows.getString(1)) }
            }
        }

    fun outcomeOfLast(name: String): Outcome? = admin.list().single { it.name == name }.lastRun?.outcome

    /** One accepted trigger to `TaskEnd`, in nanos. Retries the trigger: `busy` clears after the event. */
    fun runOnce(name: String): Long {
        listener.ended = CountDownLatch(1)
        var started: Long
        while (true) {
            started = System.nanoTime()
            if (admin.trigger(name, "m1") is TriggerResult.Accepted) break
            Thread.onSpinWait()
        }
        check(listener.ended.await(120, TimeUnit.SECONDS)) { "task $name never ended" }
        return System.nanoTime() - started
    }

    fun runOnceSampling(name: String, sampler: SpillSampler): infra.etl.task.TaskOutcome {
        listener.ended = CountDownLatch(1)
        val accepted = admin.trigger(name, "tester") as TriggerResult.Accepted
        while (listener.ended.count > 0L) sampler.sample()
        return awaitOutcome(name, accepted.runId)
    }

    fun awaitOutcome(name: String, runId: String): infra.etl.task.TaskOutcome {
        val deadline = System.nanoTime() + 300_000_000_000L
        while (System.nanoTime() < deadline) {
            admin.run(name, runId)?.let { return it }
            Thread.onSpinWait()
        }
        error("run $runId never recorded an outcome")
    }

    override fun close() {
        connections.values.forEach { runCatching { it.close() } }
    }
}

/** Peak bytes under every live run's `spill` directory. Sampled, because DuckDB frees it at query end. */
private class SpillSampler(private val scratchRoot: Path) {
    @Volatile var peak = 0L
        private set

    fun sample(): Long = runCatching {
        Files.walk(scratchRoot).use { paths ->
            paths.filter { Files.isRegularFile(it) && it.parent?.fileName?.toString() == "spill" }
                .mapToLong { runCatching { Files.size(it) }.getOrElse { 0L } }
                .sum()
        }
    }.getOrElse { 0L }.also { if (it > peak) peak = it }
}

private fun liveScratchDirs(scratchRoot: Path): Int = runCatching {
    Files.list(scratchRoot).use { s -> s.filter { Files.exists(it.resolve("scratch.duckdb")) }.count().toInt() }
}.getOrElse { 0 }

// ---------------------------------------------------------------------------------- task files

/**
 * Spec 2.4 shape A: no scratch reference at all, so `ScratchDb` stays lazy and never opens a file.
 * What is timed is therefore the framework's own per-run cost, not DuckDB's.
 */
private val PING_YAML = """
    name: ping
    phases:
      - name: p
        steps:
          - name: noop
            type: sql
            datasource: report
            idempotent: true
            statements:
              - "select 42"
""".trimIndent()

private fun boundedYaml(limitMb: Int, groups: Int) = """
    name: bounded
    scratch:
      memoryLimitMb: $limitMb
    phases:
      - name: p
        steps:
          - name: readback
            type: materialize
            datasource: scratch
            output: setting
            sql: select 'bounded' as who, current_setting('memory_limit') as ml
          - name: publish-setting
            type: pipe
            source:
              datasource: scratch
              sql: select who, ml from setting
            target:
              datasource: report
              table: m2_settings
          - name: oversized
            type: materialize
            datasource: scratch
            output: big
            sql: select count(*) as n from (select i, count(*) as c from range(1, $groups) t(i) group by i)
""".trimIndent()

private fun additivityYaml(name: String, limitMb: Int, target: String, burn: Int) = """
    name: $name
    scratch:
      memoryLimitMb: $limitMb
    phases:
      - name: p
        steps:
          - name: burn-before
            type: materialize
            datasource: scratch
            output: before
            sql: select count(*) as n from range(1, $burn)
          - name: readback
            type: materialize
            datasource: scratch
            output: seen
            sql: select '$name' as who, current_setting('memory_limit') as ml, epoch_ms(current_timestamp) as at_ms
          - name: burn-after
            type: materialize
            datasource: scratch
            output: after
            sql: select count(*) as n from range(1, $burn)
          - name: publish
            type: pipe
            source:
              datasource: scratch
              sql: select who, ml, at_ms from seen
            target:
              datasource: $target
              table: m2_additivity
""".trimIndent()

// ------------------------------------------------------------------------------------ numbers

private fun intProperty(key: String, fallback: Int) = System.getProperty(key)?.toIntOrNull() ?: fallback

private fun median(values: List<Long>): Long = values.sorted()[values.size / 2]

private fun percentile(values: List<Long>, p: Int): Long =
    values.sorted()[((values.size - 1) * p / 100)]

private fun us(nanos: Long) = "%.1f".format(nanos / 1000.0)

private fun mb(bytes: Long) = "%.1f".format(bytes / 1024.0 / 1024.0)
