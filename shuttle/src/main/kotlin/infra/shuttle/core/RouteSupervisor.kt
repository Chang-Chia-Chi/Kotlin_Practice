package infra.shuttle.core

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import org.jboss.logging.Logger
import java.util.concurrent.atomic.AtomicInteger

/**
 * Spec 10: every route runs in its own child of one `SupervisorJob`; a route whose run ends, by `RouteDown`, by
 * any exception or by its flow completing, is restarted after a delay that doubles from `initial` to `max`,
 * counted and logged once per restart. The delay falls back to `initial` after a successful trigger, which is a
 * run that delivered a `Seen` or a `PollCompleted` (the trigger listed or produced something; a `PollFailed`
 * alone does not count). `shuttle_route_up{route}` is 1 while a run is in progress and 0 while it waits, and
 * [ready] reads those gauges under the configured rule. One instance per process; [events] is the route's
 * trigger, opened afresh at every start.
 */
class RouteSupervisor(
    private val runners: Collection<RouteRunner>,
    private val events: (Route) -> Flow<RouteEvent>,
    private val backoff: Backoff,
    private val readiness: Readiness,
    private val registry: MeterRegistry,
) {
    private val up = runners.associate { it.route.name to registry.gauge(ShuttleMetrics.ROUTE_UP, Tags.of("route", it.route.name), AtomicInteger())!! }

    /** `all-routes-down`: unready only when every route is down; `any-route-down`: unready as soon as one is. */
    fun ready(): Boolean = when (readiness) {
        Readiness.AllRoutesDown -> up.values.any { it.get() == 1 }
        Readiness.AnyRouteDown -> up.values.all { it.get() == 1 }
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
                runner.run(events(route).onEach { if (it is RouteEvent.Seen || it is RouteEvent.PollCompleted) triggered = true })
            }
            gauge.set(0)
            outcome.exceptionOrNull()?.let { if (it is CancellationException) throw it }
            if (triggered) wait = backoff.initial
            log.warnv("route {0} is down ({1}); restarting in {2}", route.name, outcome.exceptionOrNull()?.toString() ?: "trigger completed", wait)
            delay(wait)
            wait = minOf(wait * backoff.factor, backoff.max)
            registry.counter(ShuttleMetrics.ROUTE_RESTARTS, "route", route.name).increment()
        }
    }

    private companion object {
        val log: Logger = Logger.getLogger(RouteSupervisor::class.java)
    }
}
