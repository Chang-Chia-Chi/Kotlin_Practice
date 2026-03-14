package com.mapreduce.dag.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.dag.model.DagRun
import com.mapreduce.dag.model.DagRunStatus
import com.mapreduce.dag.model.DagTaskInstance
import com.mapreduce.dag.model.TaskInstanceStatus
import com.mapreduce.dag.spi.DagNodeDef
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Jdbi
import java.util.UUID

/**
 * Layer 2 persistence — DAG orchestration specific.
 *
 * Handles dag_run, dag_task_instance, and the atomic submission
 * (run + all node instances in one txn).
 */
@ApplicationScoped
class DagRepository(
    private val jdbi: Jdbi,
    private val objectMapper: ObjectMapper,
) {

    /**
     * Atomic submission: create dag_run + all dag_task_instances in one transaction.
     * Root nodes (no dependencies) start as READY, others as BLOCKED.
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
                INSERT INTO dag_run (run_id, dag_id, status, global_context, created_at, updated_at)
                VALUES (:runId, :dagId, 'RUNNING', :globalContext, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
            )
                .bind("runId", runId)
                .bind("dagId", dagId)
                .bind("globalContext", globalContext)
                .execute()

            val batch = h.prepareBatch(
                """
                INSERT INTO dag_task_instance (instance_id, run_id, task_key, node_type, dependencies,
                    status, trigger_rule, created_at, updated_at)
                VALUES (:instanceId, :runId, :taskKey, :nodeType, :dependencies,
                    :status, :triggerRule, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
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

    /** Persist node handler output on the instance row (XCom). */
    fun saveInstanceOutput(instanceId: String, output: String?) {
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

    fun updateInstanceStatus(instanceId: String, status: TaskInstanceStatus) {
        jdbi.useHandle<Exception> { h ->
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

    /** Transition instance to RUNNING and record the Layer 1 task_id. */
    fun updateInstanceStatusAndTaskId(instanceId: String, status: TaskInstanceStatus, taskId: String) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                UPDATE dag_task_instance SET status = :status, task_id = :taskId, updated_at = CURRENT_TIMESTAMP
                WHERE instance_id = :instanceId
                """,
            )
                .bind("instanceId", instanceId)
                .bind("status", status.name)
                .bind("taskId", taskId)
                .execute()
        }
    }

    /**
     * Compare-and-swap for run status transitions.
     * Returns true if the transition succeeded.
     */
    fun updateRunStatus(runId: String, expectedStatus: DagRunStatus, newStatus: DagRunStatus): Boolean {
        val updated = jdbi.withHandle<Int, Exception> { h ->
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
        return updated > 0
    }
}
