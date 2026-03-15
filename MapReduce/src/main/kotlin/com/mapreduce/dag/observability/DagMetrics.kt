package com.mapreduce.dag.observability

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.enterprise.context.ApplicationScoped
import java.util.concurrent.TimeUnit

/**
 * DAG-specific metrics following the spec's observability contract.
 *
 * | Metric                            | Type      | Labels                         |
 * |-----------------------------------|-----------|--------------------------------|
 * | dag_run_duration_seconds           | Histogram | dag_id, status                |
 * | dag_run_active_count               | Gauge     | dag_id, status                |
 * | dag_node_duration_seconds          | Histogram | dag_id, task_key, task_type   |
 * | dag_node_retry_total               | Counter   | dag_id, task_key              |
 * | dag_node_timeout_total             | Counter   | dag_id, task_key              |
 * | dag_dispatch_lag_seconds           | Histogram | dag_id                        |
 * | dag_leader_loop_duration_seconds   | Histogram | —                             |
 */
@ApplicationScoped
class DagMetrics(private val meterRegistry: MeterRegistry) {

    fun recordRunDuration(dagId: String, status: String, durationMs: Long) {
        Timer.builder("dag_run_duration_seconds")
            .tag("dag_id", dagId)
            .tag("status", status)
            .register(meterRegistry)
            .record(durationMs, TimeUnit.MILLISECONDS)
    }

    fun recordNodeDuration(dagId: String, taskKey: String, taskType: String, durationMs: Long) {
        Timer.builder("dag_node_duration_seconds")
            .tag("dag_id", dagId)
            .tag("task_key", taskKey)
            .tag("task_type", taskType)
            .register(meterRegistry)
            .record(durationMs, TimeUnit.MILLISECONDS)
    }

    fun incrementNodeRetry(dagId: String, taskKey: String) {
        meterRegistry.counter(
            "dag_node_retry_total",
            "dag_id", dagId,
            "task_key", taskKey,
        ).increment()
    }

    fun incrementNodeTimeout(dagId: String, taskKey: String) {
        meterRegistry.counter(
            "dag_node_timeout_total",
            "dag_id", dagId,
            "task_key", taskKey,
        ).increment()
    }

    fun recordDispatchLag(dagId: String, lagMs: Long) {
        Timer.builder("dag_dispatch_lag_seconds")
            .tag("dag_id", dagId)
            .register(meterRegistry)
            .record(lagMs, TimeUnit.MILLISECONDS)
    }

    fun recordLeaderLoopDuration(durationMs: Long) {
        Timer.builder("dag_leader_loop_duration_seconds")
            .register(meterRegistry)
            .record(durationMs, TimeUnit.MILLISECONDS)
    }
}
