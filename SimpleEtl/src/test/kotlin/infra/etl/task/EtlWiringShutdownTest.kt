package infra.etl.task

import infra.etl.Etl
import infra.etl.P7Tasks
import infra.etl.Probe
import infra.etl.ProbeDatasource
import infra.etl.Trig
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir

/**
 * E16: `WiringResult.Wired.close()` - the host's "stop the schedule at shutdown" obligation and
 * the framework seam that discharges it.
 *
 * Before this, a host could only stop its schedule by reloading an empty directory - which cancels
 * the registrations as a *side effect* of throwing the task list away, and cannot stop the runner
 * at all. Each test below is one half of what `close()` has to do, and each fails if only the
 * other half was implemented:
 *
 * - [aCancelledRegistrationCannotFire] fails if only the runner's scope was cancelled.
 * - [aTriggerAfterCloseDoesNotLaunch] fails if only the registrations were cancelled.
 *
 * ### Why the cron double is local rather than `RecordingCron`
 *
 * P7's `RecordingCron.fire` looks the callback up in its *live* map and errors when the
 * registration has been closed, which is the right shape for P7's tests and the wrong one here:
 * the case this file has to reach is a firing the host's scheduler had already dispatched when
 * `close()` ran, so the callback must survive its own cancellation. [CapturingCron] keeps it.
 * Nothing in P7's fixtures is modified.
 *
 * ### Nothing here sleeps
 *
 * `Trig`'s bounded waits and P7's `Probe` do every wait. [aRunInFlightAtCloseFinishesAndIsRecorded]
 * parks *inside* the run, so "close happened while the run was in flight" is a fact and not a
 * window.
 */
class EtlWiringShutdownTest {

    @TempDir
    lateinit var root: Path

    private val cron = CapturingCron()

    private val probeDatasource = ProbeDatasource(Probe("park"))

    @AfterEach
    fun closeProbe() = probeDatasource.close()

    private fun wiring() = EtlWiring(
        scratchDirectory = root.resolve("scratch"),
        cron = cron,
        datasources = mapOf("probe" to Jdbi.create(probeDatasource)),
        scratchMemoryLimitMb = Etl.MEMORY_LIMIT_MB,
    )

    private fun wired(vararg definitions: TaskDefinition): WiringResult.Wired =
        when (val result = wiring().start(definitions.toList())) {
            is WiringResult.Wired -> result
            is WiringResult.Invalid -> error("the wiring was rejected: ${result.report.errors}")
        }

    /**
     * Seam (a). `close()` cancels the registration, so the host's scheduler holds nothing that can
     * fire - and a firing it had *already* dispatched submits nothing either.
     *
     * The second assertion is not redundant with the first: a real `Scheduler` hands the callback
     * to a worker before this call runs, and the only thing that can stop that one is the state
     * `close()` leaves behind. `TaskRunner.submit` records the run *before* it launches, so a
     * `lastRun` that is still null is a submit that never happened rather than one not yet visible.
     */
    @Test
    fun aCancelledRegistrationCannotFire() {
        val handle = wired(P7Tasks.scheduled("nightly-roll", cron = "0 0 2 * * ?"))
        assertEquals(setOf("nightly-roll"), cron.live) { "nothing was registered to begin with" }

        handle.close()
        cron.callbacks.getValue("nightly-roll").invoke()

        assertAll(
            {
                assertTrue(cron.live.isEmpty()) {
                    "close() left a live cron registration; the host's scheduler will keep firing " +
                        "it (spec 8.6): ${cron.live}"
                }
            },
            {
                assertNull(handle.admin.list().single().lastRun) {
                    "a firing already dispatched when close() ran still submitted a run"
                }
            },
        )
    }

    /**
     * Seam (b). The API path, which no registration covers.
     *
     * `AlreadyRunning` is the answer, and it is a deliberate reuse rather than a fifth sealed case:
     * `TriggerResult` is frozen public surface, and a new case breaks every host's exhaustive
     * `when`. What the test pins is the *behaviour* either way - nothing was launched - so a later
     * session that decides the naming differently changes one assertion and not the shape of this
     * file.
     */
    @Test
    fun aTriggerAfterCloseDoesNotLaunch() {
        val handle = wired(P7Tasks.scheduled("on-demand", cron = null))

        handle.close()
        val result = handle.admin.trigger("on-demand", "tester")

        assertAll(
            { assertEquals(TriggerResult.AlreadyRunning, result) },
            {
                assertNull(handle.admin.list().single().lastRun) {
                    "close() cancelled the scope but the trigger still claimed the task and " +
                        "recorded a run that will never execute"
                }
            },
            {
                assertEquals(1, handle.admin.list().size) {
                    "close() must not empty the task list - that is exactly the cost of the " +
                        "reload-an-empty-directory workaround it replaces (spec 8.6)"
                }
            },
        )
    }

    /**
     * Seam (c). The engine is ordinary blocking code with no cancellation point, so a
     * cancelled scope cannot interrupt a run already inside one. The run finishes and its real
     * outcome is recorded - not the cancellation, which would otherwise reach `TaskRunner`'s
     * completion handler and be written as a failure.
     */
    @Test
    fun aRunInFlightAtCloseFinishesAndIsRecorded() {
        val handle = wired(P7Tasks.parking("slow-roll", probeDatasource = "probe"))
        val runId = Trig.acceptedRunId(handle.admin.trigger("slow-roll", "tester"))
        probeDatasource.probe.awaitEntry()

        handle.close()
        probeDatasource.probe.release()

        val outcome = Trig.awaitFinished(handle.admin, "slow-roll", runId)
        assertEquals(Outcome.SUCCEEDED, outcome.outcome) {
            "close() interrupted a run it cannot interrupt, or recorded the scope's cancellation " +
                "over the run's own outcome: ${outcome.failure}"
        }
    }

    /**
     * The review's blocking finding: `close()` is terminal for `TaskRunner`, and `TaskScheduler`
     * has to know it.
     *
     * The scenario is an *aborted* shutdown - `close()`, then an operator reloads the real
     * directory. Without the terminal flag, `apply` finds an empty registry, re-registers every
     * cron and answers success; `list()` then reports `scheduled = true` while every firing dies
     * on the cancelled scope and is discarded by `TaskScheduler.fire`, which has no one to tell.
     * A permanently stalled schedule reporting itself healthy - the failure E14 spent a phase
     * making visible, reintroduced by the shutdown seam.
     */
    @Test
    fun reloadAfterCloseIsRejectedRatherThanRegisteringEveryCronAgain() {
        val directory = P7Tasks.directory(
            root.resolve("tasks"),
            "nightly.yaml" to P7Tasks.yaml("nightly-roll", cron = "0 0 2 * * ?"),
        )
        val handle = wired(P7Tasks.scheduled("nightly-roll", cron = "0 0 2 * * ?"))
        handle.close()

        val report = handle.admin.reload(directory)

        assertAll(
            {
                assertNotNull(report) {
                    "reload after close reported success; every cron is registered again and every " +
                        "firing will be dropped by the cancelled runner, in silence"
                }
            },
            {
                assertTrue(report != null && report.errors.single().message.contains("closed")) {
                    "the report must name the terminal state, not merely be non-null: " +
                        report?.errors
                }
            },
            {
                assertTrue(report != null && report.errors.single().message.contains("EtlWiring.start")) {
                    "the report must name the recovery - a new wiring: ${report?.errors}"
                }
            },
            {
                assertTrue(cron.live.isEmpty()) {
                    "a rejected reload registers nothing (spec 8.5); still live: ${cron.live}"
                }
            },
        )
    }

    /** Seam (d). A host with both a shutdown hook and an explicit stop calls this twice. */
    @Test
    fun closeIsIdempotent() {
        val handle = wired(P7Tasks.scheduled("nightly-roll", cron = "0 0 2 * * ?"))

        handle.close()
        handle.close()

        assertAll(
            { assertEquals(1, cron.closes.get()) { "the registration handle was closed twice" } },
            { assertEquals(TriggerResult.AlreadyRunning, handle.admin.trigger("nightly-roll", null)) },
        )
    }
}

/**
 * A `CronScheduler` that remembers a callback after its registration is cancelled, which is what
 * lets a test fire the one the host's scheduler had already dispatched.
 */
private class CapturingCron : CronScheduler {

    val callbacks: MutableMap<String, () -> Unit> = ConcurrentHashMap()

    private val registered: MutableSet<String> = ConcurrentHashMap.newKeySet()

    /** How many times a registration handle has been closed - the idempotence oracle. */
    val closes = AtomicInteger()

    val live: Set<String> get() = registered.toSet()

    override fun schedule(taskName: String, cron: String, run: () -> Unit): AutoCloseable {
        callbacks[taskName] = run
        registered += taskName
        return AutoCloseable {
            closes.incrementAndGet()
            registered -= taskName
        }
    }
}
