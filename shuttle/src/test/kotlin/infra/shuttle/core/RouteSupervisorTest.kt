package infra.shuttle.core

import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.InMemoryStateStore
import infra.shuttle.testkit.InMemoryTarget
import infra.shuttle.testkit.ScriptedFetcher
import infra.shuttle.testkit.ScriptedSource
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
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
        val pipeline = TransferPipeline(route, DigestAlgorithm.MD5, store, target, ProcessingChain(emptyList(), DigestAlgorithm.MD5), emptyMap(), { true }, {}, Hook.None, clock, registry, Staging(staging)) { 10.gib }
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
        assertEquals(7.0, restarts("dead"))
        assertEquals(0.0, up("dead"))
    }

    @Test
    fun the_backoff_resets_after_a_run_that_delivered_a_PollCompleted() = runTest {
        val starts = mutableListOf<Long>()
        val listsThenDies = flow { starts += testScheduler.currentTime; emit(RouteEvent.PollCompleted(clock.instant(), emptySet(), false)); emit(RouteEvent.RouteDown(cause)) }
        backgroundScope.launch { supervisor(mapOf("flaky" to { listsThenDies })).run() }

        advanceTimeBy(100.seconds); runCurrent()

        assertEquals(listOf(0L, 30_000L, 60_000L, 90_000L), starts, "every restart waits `initial`")
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
