package infra.shuttle.core

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.transformWhile
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Semaphore
import org.jboss.logging.Logger
import java.time.Clock
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.toJavaDuration

/**
 * The collector around the pipeline (spec 3.1, 4.1): one route's `RouteEvent` flow, collected in order under one
 * `SupervisorJob` scope. Every `Seen` is one pipeline coroutine behind `Semaphore(parallelism)`, so the collector
 * suspends on the trigger while the route is full and at most `parallelism` pipelines run at once (I19's share
 * of the runner); the permit and the in-flight gauge are released exactly once through `invokeOnCompletion`,
 * whether the pipeline ran, failed or was cancelled before it started. Poll failures and skips are counted and
 * touch no pipeline (spec 11). A complete `PollCompleted` reconciles (spec 4.6) and refreshes the stuck gauge; a
 * subscribed route has no poll, so its gauge rides a ticker in this same scope, beating every `inProgressEvery` (D51).
 * `RouteDown` is the trigger's last word: nothing after it is collected, the in-flight pipelines finish, and
 * [run] throws its cause so the supervisor restarts the route (spec 10). Cancelling [run] cancels the pipelines.
 */
class RouteRunner(
    val route: Route,
    private val pipeline: TransferPipeline,
    private val fetch: Fetcher,
    private val ledger: Ledger,
    private val clock: Clock,
    private val registry: MeterRegistry,
) {
    private val store = ledger.store
    private val name = RouteName(route.name)
    private val permits = Semaphore(route.parallelism)
    private val inflight = registry.gauge(ShuttleMetrics.INFLIGHT, Tags.of("route", route.name), AtomicInteger())!!
    private val stuck = registry.gauge(ShuttleMetrics.STUCK_TRANSFERS, Tags.of("route", route.name), AtomicInteger())!!

    suspend fun run(events: Flow<RouteEvent>) {
        var down: Throwable? = null
        supervisorScope {
            val ticker = (route.source as? Source.Subscribe)?.let { source ->
                launch { while (true) { delay(source.inProgressEvery); refreshStuck() } }
            }
            events.transformWhile { emit(it); it !is RouteEvent.RouteDown }.collect { event ->
                when (event) {
                    is RouteEvent.Seen -> {
                        permits.acquire()
                        inflight.incrementAndGet()
                        launch { pipeline.run(event, fetch) }.invokeOnCompletion { inflight.decrementAndGet(); permits.release() }
                    }
                    is RouteEvent.PollCompleted -> { poll("completed"); completed(event) }
                    is RouteEvent.PollFailed -> { poll("failed"); log.warnv("route {0}: poll failed: {1}", route.name, event.cause.toString()) }
                    RouteEvent.PollSkipped -> { poll("skipped"); log.warnv("route {0}: poll skipped, the previous one is still running", route.name) }
                    is RouteEvent.RouteDown -> down = event.cause
                }
            }
            ticker?.cancel()
        }
        down?.let { throw it }
    }

    /** Spec 11: the stuck gauge. A state store that fails here is logged and left for the next refresh. */
    private suspend fun refreshStuck() {
        try {
            stuck.set(store.stuck(name, clock.instant().minus(route.stuckWindow.toJavaDuration())))
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.warnv("route {0}: the stuck gauge could not be refreshed: {1}", route.name, e.toString())
        }
    }

    /**
     * Spec 4.6: a complete listing repairs every STORED row older than the poll's start that it did not list, through
     * the transition stage 4 uses; a truncated one skips the repair and counts it. Then the stuck gauge (spec 11).
     * A state store that fails here is logged and left for the next poll: nothing but cancellation reaches the collector.
     */
    private suspend fun completed(event: RouteEvent.PollCompleted) {
        try {
            if (event.truncated) {
                registry.counter(ShuttleMetrics.RECONCILE_SKIPPED, "route", route.name).increment()
                log.warnv("route {0}: listing truncated at {1} entries; reconciliation skipped", route.name, event.listed.size)
            } else {
                val ids = store.unlisted(name, event.startedAt, event.listed)
                for (id in ids) {
                    ledger.acked(id)
                    log.infov("route {0}: transfer {1} was moved but never recorded as acked; reconciled", route.name, id.value)
                }
                registry.counter(ShuttleMetrics.RECONCILED, "route", route.name).increment(ids.size.toDouble())
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.warnv("route {0}: end-of-poll repair failed, the next poll retries: {1}", route.name, e.toString())
        }
        refreshStuck()
    }

    private fun poll(result: String) = registry.counter(ShuttleMetrics.POLLS, "route", route.name, "result", result).increment()

    private companion object {
        val log: Logger = Logger.getLogger(RouteRunner::class.java)
    }
}
