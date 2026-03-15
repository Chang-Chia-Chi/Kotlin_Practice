package com.mapreduce.dag.observability

import com.mapreduce.dag.model.DagRunStatus
import com.mapreduce.dag.model.TaskInstanceStatus
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger
import java.time.Instant

/**
 * Structured event log for DAG state transitions.
 *
 * Every state change emits a structured JSON event for audit trails
 * and observability. In production, these events would be forwarded to
 * a dedicated log stream (NATS subject or DB table).
 */
@ApplicationScoped
class DagEventLog {

    private val log = Logger.getLogger("dag.events")

    fun nodeStateChange(
        runId: String,
        dagId: String,
        taskKey: String,
        fromStatus: TaskInstanceStatus,
        toStatus: TaskInstanceStatus,
        attempt: Int = 1,
        durationMs: Long? = null,
    ) {
        log.infof(
            """{"event":"NODE_STATE_CHANGE","run_id":"%s","dag_id":"%s","task_key":"%s","from_status":"%s","to_status":"%s","attempt":%d%s,"timestamp":"%s"}""",
            runId, dagId, taskKey, fromStatus, toStatus, attempt,
            if (durationMs != null) ""","duration_ms":$durationMs""" else "",
            Instant.now(),
        )
    }

    fun runStateChange(
        runId: String,
        dagId: String,
        fromStatus: DagRunStatus,
        toStatus: DagRunStatus,
        durationMs: Long? = null,
    ) {
        log.infof(
            """{"event":"RUN_STATE_CHANGE","run_id":"%s","dag_id":"%s","from_status":"%s","to_status":"%s"%s,"timestamp":"%s"}""",
            runId, dagId, fromStatus, toStatus,
            if (durationMs != null) ""","duration_ms":$durationMs""" else "",
            Instant.now(),
        )
    }

    fun timeoutReaped(
        runId: String,
        dagId: String,
        taskKey: String,
        attempt: Int,
    ) {
        log.warnf(
            """{"event":"NODE_TIMEOUT","run_id":"%s","dag_id":"%s","task_key":"%s","attempt":%d,"timestamp":"%s"}""",
            runId, dagId, taskKey, attempt, Instant.now(),
        )
    }

    fun slaBreached(
        runId: String,
        dagId: String,
        deadlineAt: Instant,
    ) {
        log.warnf(
            """{"event":"SLA_BREACH","run_id":"%s","dag_id":"%s","deadline_at":"%s","timestamp":"%s"}""",
            runId, dagId, deadlineAt, Instant.now(),
        )
    }

    fun retryScheduled(
        runId: String,
        dagId: String,
        taskKey: String,
        attempt: Int,
        nextAttempt: Int,
        delayMs: Long,
    ) {
        log.infof(
            """{"event":"RETRY_SCHEDULED","run_id":"%s","dag_id":"%s","task_key":"%s","attempt":%d,"next_attempt":%d,"delay_ms":%d,"timestamp":"%s"}""",
            runId, dagId, taskKey, attempt, nextAttempt, delayMs, Instant.now(),
        )
    }

}
