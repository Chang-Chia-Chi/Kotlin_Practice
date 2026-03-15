package com.mapreduce.deadletter

import com.mapreduce.deadletter.repository.DeadLetterRepository
import com.mapreduce.event.TaskDeadLettered
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.ObservesAsync
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Observability for the dead-letter processor (§8).
 *
 * Exposes Prometheus metrics:
 * - `taskqueue.deadletter.total` (gauge) — current count by handler
 * - `taskqueue.deadletter.replayed` (counter) — tasks replayed
 * - `taskqueue.deadletter.cleaned` (counter) — tasks purged
 *
 * Subscribes to [TaskDeadLettered] via `@ObservesAsync` to keep the
 * per-handler gauge approximately current without polling.
 */
@ApplicationScoped
class DeadLetterMetrics(
    private val meterRegistry: MeterRegistry,
    private val repository: DeadLetterRepository,
) {

    private val log = Logger.getLogger(DeadLetterMetrics::class.java)
    private val handlerGauges = ConcurrentHashMap<String, AtomicLong>()

    fun onDeadLetter(@ObservesAsync event: TaskDeadLettered) {
        try {
            val gauge = handlerGauges.computeIfAbsent(event.handler) { handler ->
                AtomicLong(0).also { g ->
                    meterRegistry.gauge(
                        "taskqueue.deadletter.total",
                        listOf(Tag.of("handler", handler)),
                        g,
                    ) { it.toDouble() }
                }
            }
            gauge.incrementAndGet()
        } catch (e: Exception) {
            log.warnf(e, "Error updating dead-letter gauge for handler=%s", event.handler)
        }
    }

    /** Record a replay operation. Called by the service layer. */
    fun recordReplayed(handler: String, count: Int) {
        meterRegistry.counter("taskqueue.deadletter.replayed", "handler", handler)
            .increment(count.toDouble())

        // Adjust the gauge down
        handlerGauges[handler]?.addAndGet(-count.toLong())
    }

    /** Record a cleanup operation. Called by the cleanup handler. */
    fun recordCleaned(count: Int) {
        meterRegistry.counter("taskqueue.deadletter.cleaned").increment(count.toDouble())
    }
}
