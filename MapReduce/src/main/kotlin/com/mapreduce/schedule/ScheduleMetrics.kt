package com.mapreduce.schedule

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import jakarta.enterprise.context.ApplicationScoped
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Prometheus metrics for the scheduled/cron pattern.
 *
 * | Metric                              | Type    | Labels         |
 * |-------------------------------------|---------|----------------|
 * | taskqueue.schedule.fires            | Counter | schedule_name  |
 * | taskqueue.schedule.skipped          | Counter | schedule_name  |
 * | taskqueue.schedule.last_duration_ms | Gauge   | schedule_name  |
 * | taskqueue.schedule.overdue_seconds  | Gauge   | schedule_name  |
 */
@ApplicationScoped
class ScheduleMetrics(private val registry: MeterRegistry) {

    private val lastDurationGauges = ConcurrentHashMap<String, AtomicLong>()

    fun recordFired(scheduleName: String) {
        registry.counter(
            "taskqueue.schedule.fires",
            listOf(Tag.of("schedule_name", scheduleName)),
        ).increment()
    }

    fun recordSkipped(scheduleName: String) {
        registry.counter(
            "taskqueue.schedule.skipped",
            listOf(Tag.of("schedule_name", scheduleName)),
        ).increment()
    }

    fun recordLastDuration(scheduleName: String, durationMs: Long) {
        val gauge = lastDurationGauges.computeIfAbsent(scheduleName) { name ->
            val holder = AtomicLong(0)
            registry.gauge(
                "taskqueue.schedule.last_duration_ms",
                listOf(Tag.of("schedule_name", name)),
                holder,
            ) { it.toDouble() }
            holder
        }
        gauge.set(durationMs)
    }

    fun recordOverdue(scheduleName: String, overdueSeconds: Long) {
        registry.gauge(
            "taskqueue.schedule.overdue_seconds",
            listOf(Tag.of("schedule_name", scheduleName)),
            overdueSeconds,
        ) { it.toDouble() }
    }
}
