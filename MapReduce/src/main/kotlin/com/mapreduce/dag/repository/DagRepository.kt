package com.mapreduce.dag.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.dag.model.DagRun
import com.mapreduce.dag.model.DagRunStatus
import com.mapreduce.dag.model.DagTaskInstance
import com.mapreduce.dag.model.TaskInstanceStatus
import com.mapreduce.dag.spi.DagNodeDef
import com.mapreduce.leader.FencedRepository
import com.mapreduce.leader.FencingTokenHolder
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Jdbi
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
    ) {
        jdbi.useTransaction<Exception> { h ->
            h.createUpdate(
                """
                INSERT INTO dag_run (run_id, dag_id, status, global_context, last_epoch, created_at, updated_at)
                VALUES (:runId, :dagId, 'RUNNING', :globalContext, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
            )
                .bind("runId", runId)
                .bind("dagId", dagId)
                .bind("globalContext", globalContext)
                .execute()

            val batch = h.prepareBatch(
                """
                INSERT INTO dag_task_instance (instance_id, run_id, task_key, node_type, dependencies,
                    status, trigger_rule, last_epoch, created_at, updated_at)
                VALUES (:instanceId, :runId, :taskKey, :nodeType, :dependencies,
                    :status, :triggerRule, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
            )

            nodes.forEach { node ->
                val status = if (node.dependencies.isEmpty()) "READY" else "BLOCKED"
                batch
                    .bind("instanceId", UUID.randomUUID().toString())
                    .bind("runId", runId)
                    .bind("taskKey", node.taskKey)
                    .bind("nodeType", node.nodeType)
                    .bind("dependencies", objectMapper.writeValueAsString(node.dependencies))
                    .bind("status", status)
                    .bind("triggerRule", node.triggerRule.name)
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
        jdbi.useHandle<Exception> { h ->
            if (epoch != null) {
                h.createUpdate(
                    """
                    UPDATE dag_task_instance
                    SET status = :status, last_epoch = :epoch, updated_at = CURRENT_TIMESTAMP
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
                    UPDATE dag_task_instance SET status = :status, updated_at = CURRENT_TIMESTAMP
                    WHERE instance_id = :instanceId
                    """,
                )
                    .bind("instanceId", instanceId)
                    .bind("status", status.name)
                    .execute()
            }
        }
    }

    /**
     * Transition instance to RUNNING and record the Layer 1 task_id (leader-only, fenced).
     */
    fun updateInstanceStatusAndTaskId(instanceId: String, status: TaskInstanceStatus, taskId: String) {
        val epoch = optionalEpoch()
        jdbi.useHandle<Exception> { h ->
            if (epoch != null) {
                h.createUpdate(
                    """
                    UPDATE dag_task_instance
                    SET status = :status, task_id = :taskId, last_epoch = :epoch,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE instance_id = :instanceId AND last_epoch <= :epoch
                    """,
                )
                    .bind("instanceId", instanceId)
                    .bind("status", status.name)
                    .bind("taskId", taskId)
                    .bind("epoch", epoch)
                    .execute()
            } else {
                h.createUpdate(
                    """
                    UPDATE dag_task_instance SET status = :status, task_id = :taskId,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE instance_id = :instanceId
                    """,
                )
                    .bind("instanceId", instanceId)
                    .bind("status", status.name)
                    .bind("taskId", taskId)
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
        val updated = jdbi.withHandle<Int, Exception> { h ->
            if (epoch != null) {
                h.createUpdate(
                    """
                    UPDATE dag_run
                    SET status = :newStatus, last_epoch = :epoch, updated_at = CURRENT_TIMESTAMP
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
                    UPDATE dag_run SET status = :newStatus, updated_at = CURRENT_TIMESTAMP
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
}
