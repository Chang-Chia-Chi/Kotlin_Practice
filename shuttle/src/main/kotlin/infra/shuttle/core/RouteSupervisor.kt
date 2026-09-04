package infra.shuttle.core

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Spec 10: every route runs in its own child of one `SupervisorJob`; a route whose run ends, by `RouteDown`, by
 * any exception or by its flow completing, is restarted after a delay that doubles from `initial` to `max`,
 * counted and logged once per restart. The delay falls back to `initial` after a successful trigger, which is a
 * run that delivered a `Seen` or a `PollCompleted` (the trigger listed or produced something; a `PollFailed`
 * alone does not count). `shuttle_route_up{route}` is 1 while a run is in progress and 0 while it waits, and
 * [ready] reads those gauges under the configured rule. One instance per process; [events] is the route's
 * trigger, opened afresh at every start. [restart] is spec 14.1's operator restart: the route's current run, or
 * the wait before its next one, is cut short and the backoff reset to `initial`.
 */
class RouteSupervisor(
    private val runners: Collection<RouteRunner>,
    private val events: (Route) -> Flow<RouteEvent>,
    private val backoff: Backoff,
    private val readiness: Readiness,
    private val registry: MeterRegistry,
) {
    private val up = runners.associate { it.route.name to registry.gauge(ShuttleMetrics.ROUTE_UP, Tags.of("route", it.route.name), AtomicInteger())!! }
    private val phases = ConcurrentHashMap<String, Job>()
    private val restarts = ConcurrentHashMap.newKeySet<String>()

    /** `all-routes-down`: unready only when every route is down; `any-route-down`: unready as soon as one is. */
    fun ready(): Boolean = when (readiness) {
        Readiness.AllRoutesDown -> up.values.any { it.get() == 1 }
        Readiness.AnyRouteDown -> up.values.all { it.get() == 1 }
    }

    /** Restart [route] now: its run is cancelled and its pipelines with it, or its wait is cut short; false when no such route. */
    fun restart(route: String): Boolean {
        if (route !in up) return false
        restarts += route
        phases[route]?.cancel()
        return true
    }

    /** Runs until cancelled; cancellation reaches every route and its pipelines. */
    suspend fun run(): Unit = supervisorScope {
        for (runner in runners) launch { supervise(runner) }
    }

    private suspend fun supervise(runner: RouteRunner) {
        val route = runner.route
        val gauge = up.getValue(route.name)
        var wait = backoff.initial
        while (true) {
            var triggered = false
            gauge.set(1)
            val outcome = runCatching {
                phase(route.name) { runner.run(events(route).onEach { if (it is RouteEvent.Seen || it is RouteEvent.PollCompleted) triggered = true }) }
            }
            gauge.set(0)
            outcome.exceptionOrNull()?.let { if (it is CancellationException) throw it }
            // Counted the instant the route is down, before its wait: a reader that sees the n-th restart sees the gauge at 0.
            registry.counter(ShuttleMetrics.ROUTE_RESTARTS, "route", route.name).increment()
            val restarted = outcome.isSuccess && outcome.getOrNull() == null
            if (triggered || restarted) wait = backoff.initial
            if (restarted) {
                log.infov("route {0} restarted by the operator", route.name)
            } else {
                log.warnv("route {0} is down ({1}); restarting in {2}", route.name, outcome.exceptionOrNull()?.toString() ?: "trigger completed", wait)
                wait = if (phase(route.name) { delay(wait) } == null) backoff.initial else minOf(wait * backoff.factor, backoff.max)
            }
        }
    }

    /** One cancellable step of a route's life; null when [restart] cut it short, the block's failure otherwise. */
    private suspend fun phase(route: String, block: suspend () -> Unit): Unit? = supervisorScope {
        val job = async { block() }
        phases[route] = job
        try {
            job.await()
        } catch (e: CancellationException) {
            if (restarts.remove(route)) null else throw e
        } finally {
            phases.remove(route, job)
        }
    }

    private companion object {
        val log: Logger = Logger.getLogger(RouteSupervisor::class.java)
    }
}
