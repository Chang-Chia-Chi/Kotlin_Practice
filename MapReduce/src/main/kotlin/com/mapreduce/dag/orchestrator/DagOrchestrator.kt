package com.mapreduce.dag.orchestrator

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.config.FrameworkConfig
import com.mapreduce.dag.model.DagRun
import com.mapreduce.dag.model.DagRunStatus
import com.mapreduce.dag.model.DagTaskInstance
import com.mapreduce.dag.model.TaskInstanceStatus
import com.mapreduce.dag.model.TriggerRule
import com.mapreduce.dag.repository.DagRepository
import com.mapreduce.leader.FencingTokenHolder
import com.mapreduce.leader.LeaderManager
import com.mapreduce.observability.AutoscalingMetrics
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.TaskStatus
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.jboss.logging.Logger

/**
 * Leader-only orchestration loop for DAG runs.
 *
 * The state machine:
 * 1. **Reconcile** — poll Layer 1 tasks tied to a dag_run for terminal states,
 *    update the corresponding dag_task_instance status and output.
 * 2. **Identify Dependents** — find BLOCKED nodes listing the resolved node.
 * 3. **Evaluate Trigger Rules** — decide READY, SKIPPED, or WAIT.
 *    Cascade Protocol: SKIPPED nodes immediately trigger recursive evaluation.
 * 4. **Dispatch** — merge global_context + upstream output_data, transition
 *    READY → RUNNING, enqueue into the Layer 1 task table.
 *
 * All leader writes propagate the fencing epoch via [FencingTokenHolder]
 * so repository SQL includes the `WHERE last_epoch <= :epoch` guard.
 */
@ApplicationScoped
class DagOrchestrator(
    private val config: FrameworkConfig,
    private val dagRepository: DagRepository,
    private val taskRepository: TaskRepository,
    private val leaderManager: LeaderManager,
    private val objectMapper: ObjectMapper,
    private val shutdownCoordinator: ShutdownCoordinator,
    private val autoscalingMetrics: AutoscalingMetrics,
) {

    private val log = Logger.getLogger(DagOrchestrator::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    fun onStart(@Observes ev: StartupEvent) {
        // Register scope cancellation with shutdown coordinator for Phase 1
        shutdownCoordinator.registerLeaderScopeCallback { scope.cancel() }

        val interval = config.leader().monitorInterval().toMillis()
        scope.launch {
            delay(interval) // initial delay
            while (isActive) {
                if (leaderManager.isActive) {
                    val epoch = leaderManager.token
                    try {
                        withContext(Dispatchers.IO) {
                            FencingTokenHolder.withToken(epoch) {
                                monitorRuns()
                            }
                        }
                    } catch (e: Exception) {
                        log.errorf(e, "Error in DAG orchestrator loop")
                    }
                }
                delay(interval)
            }
        }
    }

    private fun monitorRuns() {
        val runningRuns = dagRepository.findRunsByStatus(DagRunStatus.RUNNING)
        for (run in runningRuns) {
            processRun(run)
        }
    }

    /**
     * Full orchestration cycle for a single DAG run:
     * reconcile → evaluate → dispatch → check completion.
     */
    private fun processRun(run: DagRun) {
        val reconciled = reconcile(run)
        if (reconciled) {
            evaluate(run)
        }
        dispatch(run)
        checkCompletion(run)
    }

    // ──────────────────────────────────────────────────────────────
    //  Phase 1: Reconcile — sync Layer 1 task states to DAG instances
    // ──────────────────────────────────────────────────────────────

    /**
     * Poll RUNNING instances, check their backing Layer 1 task,
     * and update the instance to COMPLETED or FAILED.
     *
     * @return true if any instance state changed.
     */
    private fun reconcile(run: DagRun): Boolean {
        val runningInstances = dagRepository.findInstancesByRunAndStatus(run.runId, TaskInstanceStatus.RUNNING)
        var changed = false

        for (instance in runningInstances) {
            val taskId = instance.taskId ?: continue
            val task = taskRepository.findById(taskId) ?: continue

            when (task.status) {
                TaskStatus.COMPLETED -> {
                    dagRepository.updateInstanceStatus(instance.instanceId, TaskInstanceStatus.COMPLETED)
                    handleDynamicRouting(run.runId, instance)
                    changed = true
                    log.debugf("Reconciled %s → COMPLETED (run=%s)", instance.taskKey, run.runId)
                }
                TaskStatus.DEAD_LETTER -> {
                    dagRepository.updateInstanceStatus(instance.instanceId, TaskInstanceStatus.FAILED)
                    changed = true
                    log.debugf("Reconciled %s → FAILED (run=%s)", instance.taskKey, run.runId)
                }
                else -> { /* still in progress */ }
            }
        }

        return changed
    }

    // ──────────────────────────────────────────────────────────────
    //  Phase 1b: Dynamic Branch Routing
    // ──────────────────────────────────────────────────────────────

    /**
     * If a completed task's output contains `__dag_route__`, forcefully
     * SKIP any immediate downstream nodes not in the routing array.
     */
    private fun handleDynamicRouting(runId: String, completedInstance: DagTaskInstance) {
        val output = completedInstance.outputData ?: return

        val routeNode = try {
            val tree = objectMapper.readTree(output)
            tree.get("__dag_route__") ?: return
        } catch (_: Exception) {
            return
        }

        if (!routeNode.isArray) return
        val routedKeys = routeNode.map { it.asText() }.toSet()

        val instances = dagRepository.findInstancesByRunId(runId)
        for (downstream in instances) {
            if (downstream.status != TaskInstanceStatus.BLOCKED) continue
            val deps = parseDependencies(downstream.dependencies)
            if (completedInstance.taskKey !in deps) continue

            if (downstream.taskKey !in routedKeys) {
                dagRepository.updateInstanceStatus(downstream.instanceId, TaskInstanceStatus.SKIPPED)
                log.infof("Dynamic routing: skipped %s (not in route from %s, run=%s)",
                    downstream.taskKey, completedInstance.taskKey, runId)
            }
        }
    }

    // ──────────────────────────────────────────────────────────────
    //  Phase 2: Evaluate trigger rules (with cascade)
    // ──────────────────────────────────────────────────────────────

    /**
     * Iterate BLOCKED nodes and evaluate trigger rules against upstream parents.
     * Re-loops until no more state changes (handles cascading SKIPs).
     */
    private fun evaluate(run: DagRun) {
        var changed = true
        while (changed) {
            changed = false
            val instances = dagRepository.findInstancesByRunId(run.runId)
            val instanceMap = instances.associateBy { it.taskKey }

            for (instance in instances) {
                if (instance.status != TaskInstanceStatus.BLOCKED) continue

                val deps = parseDependencies(instance.dependencies)
                val allUpstream = deps.mapNotNull { instanceMap[it] }
                if (allUpstream.size != deps.size) {
                    log.warnf("Instance %s has unresolvable dependencies (run=%s)", instance.taskKey, run.runId)
                    continue
                }

                when (evaluateTriggerRule(instance.triggerRule, allUpstream)) {
                    EvalResult.READY -> {
                        dagRepository.updateInstanceStatus(instance.instanceId, TaskInstanceStatus.READY)
                        changed = true
                    }
                    EvalResult.SKIP -> {
                        dagRepository.updateInstanceStatus(instance.instanceId, TaskInstanceStatus.SKIPPED)
                        changed = true
                        log.debugf("Cascade skip: %s (run=%s)", instance.taskKey, run.runId)
                    }
                    EvalResult.WAIT -> { /* not all deps resolved yet */ }
                }
            }
        }
    }

    private enum class EvalResult { READY, SKIP, WAIT }

    private fun evaluateTriggerRule(rule: TriggerRule, upstream: List<DagTaskInstance>): EvalResult {
        val terminal = setOf(TaskInstanceStatus.COMPLETED, TaskInstanceStatus.FAILED, TaskInstanceStatus.SKIPPED)
        val allTerminal = upstream.all { it.status in terminal }

        return when (rule) {
            TriggerRule.ALL_SUCCESS -> {
                if (upstream.any { it.status in setOf(TaskInstanceStatus.FAILED, TaskInstanceStatus.SKIPPED) }) {
                    EvalResult.SKIP
                } else if (upstream.all { it.status == TaskInstanceStatus.COMPLETED }) {
                    EvalResult.READY
                } else {
                    EvalResult.WAIT
                }
            }
            TriggerRule.ONE_SUCCESS -> {
                if (upstream.any { it.status == TaskInstanceStatus.COMPLETED }) {
                    EvalResult.READY
                } else if (allTerminal) {
                    EvalResult.SKIP // all done but none succeeded
                } else {
                    EvalResult.WAIT
                }
            }
            TriggerRule.ALL_DONE -> {
                if (allTerminal) EvalResult.READY else EvalResult.WAIT
            }
        }
    }

    // ──────────────────────────────────────────────────────────────
    //  Phase 3: Dispatch READY nodes as Layer 1 tasks
    // ──────────────────────────────────────────────────────────────

    private fun dispatch(run: DagRun) {
        val readyInstances = dagRepository.findInstancesByRunAndStatus(run.runId, TaskInstanceStatus.READY)

        for (instance in readyInstances) {
            val payload = buildPayload(run, instance)

            val taskId = taskRepository.enqueue(
                EnqueueRequest(
                    handler = "dag.${instance.nodeType}",
                    payload = payload,
                    queue = "dag",
                    groupId = run.runId,
                    metadata = objectMapper.writeValueAsString(
                        mapOf(
                            "instance_id" to instance.instanceId,
                            "task_key" to instance.taskKey,
                            "phase" to "DAG",
                        ),
                    ),
                ),
            )

            dagRepository.updateInstanceStatusAndTaskId(instance.instanceId, TaskInstanceStatus.RUNNING, taskId)
            log.debugf("Dispatched %s → task %s (run=%s)", instance.taskKey, taskId, run.runId)
        }
    }

    /**
     * Merge global_context and upstream output_data into a single JSON payload.
     */
    private fun buildPayload(run: DagRun, instance: DagTaskInstance): String {
        val payloadMap = mutableMapOf<String, Any?>()

        if (run.globalContext != null) {
            try {
                payloadMap["global_context"] = objectMapper.readTree(run.globalContext)
            } catch (_: Exception) {
                payloadMap["global_context"] = run.globalContext
            }
        }

        val deps = parseDependencies(instance.dependencies)
        if (deps.isNotEmpty()) {
            val allInstances = dagRepository.findInstancesByRunId(run.runId)
            val upstreamOutputs = mutableMapOf<String, Any>()
            for (upstream in allInstances) {
                if (upstream.taskKey in deps && upstream.outputData != null) {
                    try {
                        upstreamOutputs[upstream.taskKey] = objectMapper.readTree(upstream.outputData)
                    } catch (_: Exception) {
                        upstreamOutputs[upstream.taskKey] = upstream.outputData
                    }
                }
            }
            if (upstreamOutputs.isNotEmpty()) {
                payloadMap["upstream"] = upstreamOutputs
            }
        }

        return objectMapper.writeValueAsString(payloadMap)
    }

    // ──────────────────────────────────────────────────────────────
    //  Phase 4: Check run completion
    // ──────────────────────────────────────────────────────────────

    private fun checkCompletion(run: DagRun) {
        val instances = dagRepository.findInstancesByRunId(run.runId)
        val terminal = setOf(TaskInstanceStatus.COMPLETED, TaskInstanceStatus.SKIPPED, TaskInstanceStatus.FAILED)
        val allTerminal = instances.all { it.status in terminal }

        if (allTerminal && instances.isNotEmpty()) {
            val hasFailed = instances.any { it.status == TaskInstanceStatus.FAILED }
            val newStatus = if (hasFailed) DagRunStatus.FAILED else DagRunStatus.COMPLETED
            if (dagRepository.updateRunStatus(run.runId, DagRunStatus.RUNNING, newStatus)) {
                log.infof("DAG run %s → %s (dag=%s)", run.runId, newStatus, run.dagId)
                if (run.createdAt != null) {
                    autoscalingMetrics.recordOrchestrationDuration("DAG", run.dagId, run.createdAt)
                }
            }
        }
    }

    // ──────────────────────────────────────────────────────────────
    //  Helpers
    // ──────────────────────────────────────────────────────────────

    private fun parseDependencies(deps: String?): List<String> {
        if (deps.isNullOrBlank()) return emptyList()
        return try {
            val node = objectMapper.readTree(deps)
            if (node.isArray) node.map { it.asText() } else emptyList()
        } catch (_: Exception) {
            emptyList()
        }
    }
}
