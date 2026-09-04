package infra.shuttle.core

import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.InMemoryStateStore
import infra.shuttle.testkit.InMemoryTarget
import infra.shuttle.testkit.ScriptedFetcher
import infra.shuttle.testkit.ScriptedSource
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** Spec 10 on the virtual clock: restart with backoff, the up gauge, both readiness rules. */
class RouteSupervisorTest {
    @TempDir lateinit var staging: Path
    private val clock = ClockFixture()
    private val store = InMemoryStateStore(clock)
    private val target = InMemoryTarget("landing")
    private val fetcher = ScriptedFetcher(clock).file("a.csv", "a\n".toByteArray())
    private val registry = SimpleMeterRegistry()
    private val cause = IllegalStateException("wrong password")

    private fun runner(name: String): RouteRunner {
        val route = Route(name = name, source = Source.Poll("sftp", "/in", 1.minutes, onAck = AckAction.Move("done")), target = Target("minio", bucket = "landing"))
        val pipeline = TransferPipeline(route, DigestAlgorithm.MD5, store, target, ProcessingChain(emptyList(), DigestAlgorithm.MD5, Dispatchers.Unconfined), emptyMap(), { true }, {}, Hook.None, clock, registry, Staging(staging), usableSpace = { 10.gib })
        return RouteRunner(route, pipeline, fetcher, store, {}, clock, registry)
    }

    private fun supervisor(flows: Map<String, () -> Flow<RouteEvent>>, readiness: Readiness = Readiness.AllRoutesDown) =
        RouteSupervisor(flows.keys.map { runner(it) }, { flows.getValue(it.name)() }, Backoff(30.seconds, 15.minutes), readiness, registry)

    private fun restarts(route: String) = registry.counter(ShuttleMetrics.ROUTE_RESTARTS, "route", route).count()
    private fun up(route: String) = registry.find(ShuttleMetrics.ROUTE_UP).tag("route", route).gauge()!!.value()

    @Test
    fun I21_a_dead_route_is_restarted_with_backoff_doubling_from_initial_to_max() = runTest {
        val starts = mutableListOf<Long>()
        val supervisor = supervisor(mapOf("dead" to { flow { starts += testScheduler.currentTime; emit(RouteEvent.RouteDown(cause)) } }))
        backgroundScope.launch { supervisor.run() }

        advanceTimeBy(3000.seconds); runCurrent()

        assertEquals(listOf(0, 30, 90, 210, 450, 930, 1830, 2730).map { it * 1000L }, starts, "delays 30, 60, 120, 240, 480, then capped at 900 s")
        assertEquals(8.0, restarts("dead"), "every run's end is counted at once; the eighth is in its wait")
        assertEquals(0.0, up("dead"))
    }

    /**
     * Spec 10 "each restart logged and counted" against 14.2's `shuttle_route_up`: the counter moves at the instant
     * the route goes down, so a reader that sees the n-th restart counted sees the gauge at 0 for the whole of that
     * wait. Sampled inside every run and at every second of the virtual clock, the restarts counted are always the
     * runs that have ended, `starts - up`.
     */
    @Test
    fun the_restart_counter_and_the_route_gauge_never_disagree_about_a_route_being_down() = runTest {
        val starts = mutableListOf<Long>()
        val disagreements = mutableListOf<String>()
        fun sample(where: String) {
            val ended = starts.size - up("dead")
            if (restarts("dead") != ended) disagreements += "$where: ${restarts("dead")} restarts counted, $ended runs ended"
        }
        val supervisor = supervisor(mapOf("dead" to { flow { starts += testScheduler.currentTime; sample("inside run ${starts.size}"); emit(RouteEvent.RouteDown(cause)) } }))
        backgroundScope.launch { supervisor.run() }

        repeat(200) { runCurrent(); sample("at ${testScheduler.currentTime} ms"); advanceTimeBy(1.seconds) }

        assertEquals(listOf(0L, 30_000L, 90_000L), starts, "three runs in 200 s")
        assertEquals(emptyList<String>(), disagreements)
    }

    @Test
    fun the_backoff_resets_after_a_run_that_delivered_a_PollCompleted() = runTest {
        val starts = mutableListOf<Long>()
        val listsThenDies = flow { starts += testScheduler.currentTime; emit(RouteEvent.PollCompleted(clock.instant(), emptySet(), false)); emit(RouteEvent.RouteDown(cause)) }
        backgroundScope.launch { supervisor(mapOf("flaky" to { listsThenDies })).run() }

        advanceTimeBy(100.seconds); runCurrent()

        assertEquals(listOf(0L, 30_000L, 60_000L, 90_000L), starts, "every restart waits `initial`")
    }

    /** Spec 14.1: an operator restart cuts the current run short, or the wait before the next one, and the backoff is `initial` again. */
    @Test
    fun restart_cancels_the_current_run_and_a_restart_during_the_wait_cuts_it_short_and_resets_the_backoff() = runTest {
        val starts = mutableListOf<Long>()
        val supervisor = supervisor(mapOf("dead" to { flow { starts += testScheduler.currentTime; if (starts.size == 1) awaitCancellation() else emit(RouteEvent.RouteDown(cause)) } }))
        backgroundScope.launch { supervisor.run() }
        runCurrent()
        assertEquals(1.0, up("dead"), "the first run is live")

        assertEquals(true, supervisor.restart("dead"))
        runCurrent()
        assertEquals(listOf(0L, 0L), starts, "the run is cancelled and restarted with no wait")
        assertEquals(2.0, restarts("dead"), "the operator's restart, then the second run's death, each counted as it happened")

        // The second run died at once: the supervisor is 30 s into its wait; a restart at 10 s starts it then.
        advanceTimeBy(10.seconds); runCurrent()
        assertEquals(true, supervisor.restart("dead"))
        runCurrent()
        assertEquals(listOf(0L, 0L, 10_000L), starts)

        // It died again, and the wait is `initial` again, not the doubled one.
        advanceTimeBy(30.seconds); runCurrent()
        assertEquals(listOf(0L, 0L, 10_000L, 40_000L), starts)
        assertEquals(false, supervisor.restart("nobody"))
    }

    /** A live trigger never completes on its own: after its script it stays open until cancelled. */
    private fun live(script: ScriptedSource): () -> Flow<RouteEvent> = { flow { script.events().collect { emit(it) }; awaitCancellation() } }

    @Test
    fun S23_I21_two_routes_one_dead_the_other_keeps_completing_and_readiness_follows_the_rule() = runTest {
        val a = ScriptedSource.identity("a.csv", route = "alive")
        val alive = ScriptedSource(clock).seen(a).pollCompleted(setOf(a))
        val flows = mapOf("alive" to live(alive), "dead" to { flow { emit(RouteEvent.RouteDown(cause)) } })
        val allRoutesDown = supervisor(flows)
        backgroundScope.launch { allRoutesDown.run() }

        advanceTimeBy(1.seconds); runCurrent()

        assertEquals(TransferState.DONE, store.transfers.single().state)
        assertEquals(listOf(a), alive.acks)
        assertEquals(1.0, up("alive"))
        assertEquals(0.0, up("dead"))
        assertEquals(true, allRoutesDown.ready(), "all-routes-down: a partially healthy pod keeps serving")

        val anyRouteDown = RouteSupervisor(flows.keys.map { runner(it) }, { flows.getValue(it.name)() }, Backoff(30.seconds, 15.minutes), Readiness.AnyRouteDown, SimpleMeterRegistry())
        assertEquals(false, anyRouteDown.ready(), "unready before any route has started")
        backgroundScope.launch { anyRouteDown.run() }
        advanceTimeBy(1.seconds); runCurrent()
        assertEquals(false, anyRouteDown.ready(), "any-route-down: one dead route makes the pod unready")
    }
}
