package com.mapreduce.dag.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.dag.model.DagRun
import com.mapreduce.dag.model.DagRunStatus
import com.mapreduce.dag.model.DagTaskInstance
import com.mapreduce.dag.model.TaskInstanceStatus
import com.mapreduce.dag.model.TriggerType
import com.mapreduce.dag.spi.DagNodeDef
import com.mapreduce.leader.FencedRepository
import com.mapreduce.leader.FencingTokenHolder
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Jdbi
import java.time.Instant
import java.util.UUID

/**
 * Layer 2 persistence — DAG orchestration specific.
 *
 * Extends [FencedRepository] for leader-only writes that participate in
 * the fenced leader election pattern. The fencing epoch is read from
 * [FencingTokenHolder] and applied as a SQL WHERE guard on all leader writes.
 */
@ApplicationScoped
class DagRepository(
    jdbi: Jdbi,
    private val objectMapper: ObjectMapper,
) : FencedRepository(jdbi) {

    /**
     * Atomic submission: create dag_run + all dag_task_instances in one transaction.
     * Root nodes (no dependencies) start as READY, others as BLOCKED.
     *
     * Not a leader-only write — called from the REST endpoint.
     */
    fun submitRun(
        runId: String,
        dagId: String,
        globalContext: String,
        nodes: List<DagNodeDef>,
        triggerType: TriggerType = TriggerType.MANUAL,
        triggerMetadata: String? = null,
        parentRunId: String? = null,
        deadlineAt: Instant? = null,
        defaultMaxAttempts: Int = 1,
    ) {
        jdbi.useTransaction<Exception> { h ->
            h.createUpdate(
                """
                INSERT INTO dag_run (run_id, dag_id, status, global_context, trigger_type,
                    trigger_metadata, parent_run_id, deadline_at, last_epoch, created_at, updated_at)
                VALUES (:runId, :dagId, 'PENDING', :globalContext, :triggerType,
                    :triggerMetadata, :parentRunId, :deadlineAt, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
            )
                .bind("runId", runId)
                .bind("dagId", dagId)
                .bind("globalContext", globalContext)
                .bind("triggerType", triggerType.name)
                .bind("triggerMetadata", triggerMetadata)
                .bind("parentRunId", parentRunId)
                .bind("deadlineAt", deadlineAt)
                .execute()

            val batch = h.prepareBatch(
                """
                INSERT INTO dag_task_instance (instance_id, run_id, task_key, node_type, task_type,
                    dependencies, status, trigger_rule, attempt, max_attempts,
                    last_epoch, created_at, updated_at)
                VALUES (:instanceId, :runId, :taskKey, :nodeType, :taskType,
                    :dependencies, :status, :triggerRule, 1, :maxAttempts,
                    0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
            )

            nodes.forEach { node ->
                val isRoot = node.dependencies.isEmpty() && node.triggerRule != com.mapreduce.dag.model.TriggerRule.ON_FAILURE
                val status = if (isRoot) "READY" else "BLOCKED"
                val maxAttempts = node.maxAttempts ?: defaultMaxAttempts
                batch
                    .bind("instanceId", UUID.randomUUID().toString())
                    .bind("runId", runId)
                    .bind("taskKey", node.taskKey)
                    .bind("nodeType", node.nodeType)
                    .bind("taskType", node.taskType)
                    .bind("dependencies", objectMapper.writeValueAsString(node.dependencies))
                    .bind("status", status)
                    .bind("triggerRule", node.triggerRule.name)
                    .bind("maxAttempts", maxAttempts)
                    .add()
            }
            batch.execute()
        }
    }

    fun findRunById(runId: String): DagRun? =
        jdbi.withHandle<DagRun?, Exception> { h ->
            h.createQuery("SELECT * FROM dag_run WHERE run_id = :runId")
                .bind("runId", runId)
                .mapTo(DagRun::class.java)
                .findOne()
                .orElse(null)
        }

    fun findRunsByStatus(status: DagRunStatus): List<DagRun> =
        jdbi.withHandle<List<DagRun>, Exception> { h ->
            h.createQuery("SELECT * FROM dag_run WHERE status = :status")
                .bind("status", status.name)
                .mapTo(DagRun::class.java)
                .list()
        }

    fun findAllRuns(limit: Int = 100): List<DagRun> =
        jdbi.withHandle<List<DagRun>, Exception> { h ->
            h.createQuery("SELECT * FROM dag_run ORDER BY created_at DESC FETCH FIRST :limit ROWS ONLY")
                .bind("limit", limit)
                .mapTo(DagRun::class.java)
                .list()
        }

    /** Count active RUNNING runs for a given dag_id (for concurrency control). */
    fun countRunningRunsByDagId(dagId: String): Int =
        jdbi.withHandle<Int, Exception> { h ->
            h.createQuery(
                "SELECT COUNT(*) FROM dag_run WHERE dag_id = :dagId AND status = 'RUNNING'",
            )
                .bind("dagId", dagId)
                .mapTo(Int::class.java)
                .one()
        }

    fun findInstancesByRunId(runId: String): List<DagTaskInstance> =
        jdbi.withHandle<List<DagTaskInstance>, Exception> { h ->
            h.createQuery("SELECT * FROM dag_task_instance WHERE run_id = :runId")
                .bind("runId", runId)
                .mapTo(DagTaskInstance::class.java)
                .list()
        }

    fun findInstancesByRunAndStatus(runId: String, status: TaskInstanceStatus): List<DagTaskInstance> =
        jdbi.withHandle<List<DagTaskInstance>, Exception> { h ->
            h.createQuery(
                "SELECT * FROM dag_task_instance WHERE run_id = :runId AND status = :status",
            )
                .bind("runId", runId)
                .bind("status", status.name)
                .mapTo(DagTaskInstance::class.java)
                .list()
        }

    /** Find instances that have exceeded their timeout deadline. */
    fun findTimedOutInstances(runId: String): List<DagTaskInstance> =
        jdbi.withHandle<List<DagTaskInstance>, Exception> { h ->
            h.createQuery(
                """
                SELECT * FROM dag_task_instance
                WHERE run_id = :runId
                  AND status IN ('QUEUED', 'RUNNING')
                  AND timeout_at IS NOT NULL
                  AND timeout_at < CURRENT_TIMESTAMP
                """,
            )
                .bind("runId", runId)
                .mapTo(DagTaskInstance::class.java)
                .list()
        }

    /**
     * Persist node handler output on the instance row (XCom).
     * Fenced by execution_generation via the backing task to prevent zombie writes.
     *
     * Not a leader-only write — called from the DagTaskHandler (worker).
     */
    fun saveInstanceOutput(instanceId: String, output: String?, executionGeneration: String? = null) {
        if (executionGeneration != null) {
            jdbi.useHandle<Exception> { h ->
                h.createUpdate(
                    """
                    UPDATE dag_task_instance SET output_data = :output, updated_at = CURRENT_TIMESTAMP
                    WHERE instance_id = :instanceId
                      AND task_id IN (SELECT task_id FROM task WHERE task_id = dag_task_instance.task_id AND execution_generation = :gen)
                    """,
                )
                    .bind("instanceId", instanceId)
                    .bind("output", output)
                    .bind("gen", executionGeneration)
                    .execute()
            }
        } else {
            jdbi.useHandle<Exception> { h ->
                h.createUpdate(
                    """
                    UPDATE dag_task_instance SET output_data = :output, updated_at = CURRENT_TIMESTAMP
                    WHERE instance_id = :instanceId
                    """,
                )
                    .bind("instanceId", instanceId)
                    .bind("output", output)
                    .execute()
            }
        }
    }

    /**
     * Update instance status (leader-only, fenced).
     * Includes the epoch fence when running inside a fenced leader context.
     */
    fun updateInstanceStatus(instanceId: String, status: TaskInstanceStatus) {
        val epoch = optionalEpoch()
        val completedAt = if (status.isTerminal) "CURRENT_TIMESTAMP" else "NULL"
        jdbi.useHandle<Exception> { h ->
            if (epoch != null) {
                h.createUpdate(
                    """
                    UPDATE dag_task_instance
                    SET status = :status, last_epoch = :epoch, completed_at = $completedAt,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE instance_id = :instanceId AND last_epoch <= :epoch
                    """,
                )
                    .bind("instanceId", instanceId)
                    .bind("status", status.name)
                    .bind("epoch", epoch)
                    .execute()
            } else {
                h.createUpdate(
                    """
                    UPDATE dag_task_instance SET status = :status, completed_at = $completedAt,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE instance_id = :instanceId
                    """,
                )
                    .bind("instanceId", instanceId)
                    .bind("status", status.name)
                    .execute()
            }
        }
    }

    /** Update instance status and persist error payload (leader-only, fenced). */
    fun updateInstanceStatusWithError(instanceId: String, status: TaskInstanceStatus, error: String?) {
        val epoch = optionalEpoch()
        jdbi.useHandle<Exception> { h ->
            if (epoch != null) {
                h.createUpdate(
                    """
                    UPDATE dag_task_instance
                    SET status = :status, error = :error, last_epoch = :epoch,
                        completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                    WHERE instance_id = :instanceId AND last_epoch <= :epoch
                    """,
                )
                    .bind("instanceId", instanceId)
                    .bind("status", status.name)
                    .bind("error", error)
                    .bind("epoch", epoch)
                    .execute()
            } else {
                h.createUpdate(
                    """
                    UPDATE dag_task_instance SET status = :status, error = :error,
                        completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                    WHERE instance_id = :instanceId
                    """,
                )
                    .bind("instanceId", instanceId)
                    .bind("status", status.name)
                    .bind("error", error)
                    .execute()
            }
        }
    }

    /**
     * Prepare an instance for retry: increment attempt, reset status to READY,
     * clear the old task_id, and set a future dispatched_at for backoff.
     */
    fun prepareInstanceForRetry(
        instanceId: String,
        nextAttempt: Int,
        dispatchAfter: Instant?,
    ) {
        val epoch = optionalEpoch()
        jdbi.useHandle<Exception> { h ->
            if (epoch != null) {
                h.createUpdate(
                    """
                    UPDATE dag_task_instance
                    SET status = 'READY', attempt = :nextAttempt, task_id = NULL,
                        timeout_at = NULL, dispatched_at = :dispatchAfter,
                        error = NULL, last_epoch = :epoch, updated_at = CURRENT_TIMESTAMP
                    WHERE instance_id = :instanceId AND last_epoch <= :epoch
                    """,
                )
                    .bind("instanceId", instanceId)
                    .bind("nextAttempt", nextAttempt)
                    .bind("dispatchAfter", dispatchAfter)
                    .bind("epoch", epoch)
                    .execute()
            } else {
                h.createUpdate(
                    """
                    UPDATE dag_task_instance
                    SET status = 'READY', attempt = :nextAttempt, task_id = NULL,
                        timeout_at = NULL, dispatched_at = :dispatchAfter,
                        error = NULL, updated_at = CURRENT_TIMESTAMP
                    WHERE instance_id = :instanceId
                    """,
                )
                    .bind("instanceId", instanceId)
                    .bind("nextAttempt", nextAttempt)
                    .bind("dispatchAfter", dispatchAfter)
                    .execute()
            }
        }
    }

    /**
     * Transition instance to RUNNING and record the Layer 1 task_id (leader-only, fenced).
     */
    fun updateInstanceStatusAndTaskId(
        instanceId: String,
        status: TaskInstanceStatus,
        taskId: String,
        timeoutAt: Instant? = null,
    ) {
        val epoch = optionalEpoch()
        jdbi.useHandle<Exception> { h ->
            if (epoch != null) {
                h.createUpdate(
                    """
                    UPDATE dag_task_instance
                    SET status = :status, task_id = :taskId, timeout_at = :timeoutAt,
                        dispatched_at = CURRENT_TIMESTAMP, last_epoch = :epoch,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE instance_id = :instanceId AND last_epoch <= :epoch
                    """,
                )
                    .bind("instanceId", instanceId)
                    .bind("status", status.name)
                    .bind("taskId", taskId)
                    .bind("timeoutAt", timeoutAt)
                    .bind("epoch", epoch)
                    .execute()
            } else {
                h.createUpdate(
                    """
                    UPDATE dag_task_instance SET status = :status, task_id = :taskId,
                        timeout_at = :timeoutAt, dispatched_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE instance_id = :instanceId
                    """,
                )
                    .bind("instanceId", instanceId)
                    .bind("status", status.name)
                    .bind("taskId", taskId)
                    .bind("timeoutAt", timeoutAt)
                    .execute()
            }
        }
    }

    /**
     * Compare-and-swap for run status transitions (leader-only, fenced).
     * Returns true if the transition succeeded.
     */
    fun updateRunStatus(runId: String, expectedStatus: DagRunStatus, newStatus: DagRunStatus): Boolean {
        val epoch = optionalEpoch()
        val completedField = if (newStatus.isTerminal()) ", completed_at = CURRENT_TIMESTAMP" else ""
        val startedField = if (newStatus == DagRunStatus.RUNNING) ", started_at = CURRENT_TIMESTAMP" else ""
        val updated = jdbi.withHandle<Int, Exception> { h ->
            if (epoch != null) {
                h.createUpdate(
                    """
                    UPDATE dag_run
                    SET status = :newStatus, last_epoch = :epoch$startedField$completedField,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE run_id = :runId AND status = :expectedStatus AND last_epoch <= :epoch
                    """,
                )
                    .bind("runId", runId)
                    .bind("expectedStatus", expectedStatus.name)
                    .bind("newStatus", newStatus.name)
                    .bind("epoch", epoch)
                    .execute()
            } else {
                h.createUpdate(
                    """
                    UPDATE dag_run SET status = :newStatus$startedField$completedField,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE run_id = :runId AND status = :expectedStatus
                    """,
                )
                    .bind("runId", runId)
                    .bind("expectedStatus", expectedStatus.name)
                    .bind("newStatus", newStatus.name)
                    .execute()
            }
        }
        return updated > 0
    }

    /** Cancel a Run and mark all non-terminal instances as SKIPPED. */
    fun cancelRun(runId: String): Boolean {
        val updated = updateRunStatus(runId, DagRunStatus.RUNNING, DagRunStatus.CANCELLED)
        if (!updated) {
            // Also try cancelling PENDING runs
            val pendingCancelled = updateRunStatus(runId, DagRunStatus.PENDING, DagRunStatus.CANCELLED)
            if (!pendingCancelled) return false
        }
        val epoch = optionalEpoch()
        jdbi.useHandle<Exception> { h ->
            if (epoch != null) {
                h.createUpdate(
                    """
                    UPDATE dag_task_instance
                    SET status = 'SKIPPED', last_epoch = :epoch, completed_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE run_id = :runId AND status NOT IN ('COMPLETED', 'SKIPPED', 'FAILED', 'TIMED_OUT')
                      AND last_epoch <= :epoch
                    """,
                )
                    .bind("runId", runId)
                    .bind("epoch", epoch)
                    .execute()
            } else {
                h.createUpdate(
                    """
                    UPDATE dag_task_instance
                    SET status = 'SKIPPED', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                    WHERE run_id = :runId AND status NOT IN ('COMPLETED', 'SKIPPED', 'FAILED', 'TIMED_OUT')
                    """,
                )
                    .bind("runId", runId)
                    .execute()
            }
        }
        return true
    }

    /** Reset a FAILED instance to READY for manual retry. */
    fun manualRetryInstance(runId: String, taskKey: String): Boolean {
        val instance = findInstancesByRunId(runId).find { it.taskKey == taskKey } ?: return false
        if (instance.status != TaskInstanceStatus.FAILED && instance.status != TaskInstanceStatus.TIMED_OUT) return false
        prepareInstanceForRetry(instance.instanceId, instance.attempt + 1, null)
        // Ensure the run is in RUNNING state
        updateRunStatus(runId, DagRunStatus.FAILED, DagRunStatus.RUNNING)
        return true
    }

    /** Skip a BLOCKED or FAILED instance manually. */
    fun manualSkipInstance(runId: String, taskKey: String): Boolean {
        val instance = findInstancesByRunId(runId).find { it.taskKey == taskKey } ?: return false
        if (instance.status != TaskInstanceStatus.BLOCKED && instance.status != TaskInstanceStatus.FAILED) return false
        updateInstanceStatus(instance.instanceId, TaskInstanceStatus.SKIPPED)
        return true
    }
}

private fun DagRunStatus.isTerminal(): Boolean =
    this in setOf(DagRunStatus.COMPLETED, DagRunStatus.FAILED, DagRunStatus.CANCELLED)
