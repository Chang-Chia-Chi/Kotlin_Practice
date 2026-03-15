package com.mapreduce.dag.orchestrator

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.config.FrameworkConfig
import com.mapreduce.dag.model.DagRun
import com.mapreduce.dag.model.DagRunStatus
import com.mapreduce.dag.model.DagTaskInstance
import com.mapreduce.dag.model.ErrorClass
import com.mapreduce.dag.model.TaskInstanceStatus
import com.mapreduce.dag.model.TriggerRule
import com.mapreduce.dag.observability.DagEventLog
import com.mapreduce.dag.observability.DagMetrics
import com.mapreduce.dag.registry.DagRegistrar
import com.mapreduce.dag.repository.DagRepository
import com.mapreduce.dag.template.ConditionEvaluator
import com.mapreduce.dag.template.TemplateEngine
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
import java.time.Duration
import java.time.Instant

/**
 * Leader-only orchestration loop for DAG runs.
 *
 * Implements the six-phase state machine from the DAG Orchestration spec:
 *
 * 1. **Reconcile** — sync Layer 1 terminal states to dag_task_instances,
 *    handle error classification and dynamic routing.
 * 2. **Timeout Reaping** — detect instances exceeding their timeout_at deadline,
 *    transition to TIMED_OUT, and schedule retries if eligible.
 * 3. **Identify Dependents & Evaluate Trigger Rules** — find BLOCKED nodes whose
 *    dependencies resolved, evaluate trigger rules with cascade protocol.
 * 4. **Dispatch** — resolve templates, enforce concurrency limits, enqueue READY
 *    nodes as Layer 1 tasks.
 * 5. **Run Completion Check** — determine terminal Run status, dispatch ON_FAILURE
 *    handlers when appropriate.
 * 6. **Promote PENDING Runs** — transition PENDING → RUNNING respecting max_parallel_runs.
 *
 * All leader writes propagate the fencing epoch via [FencingTokenHolder]
 * so repository SQL includes the `WHERE last_epoch <= :epoch` guard.
 */
@ApplicationScoped
class DagOrchestrator(
    private val config: FrameworkConfig,
    private val dagRepository: DagRepository,
    private val taskRepository: TaskRepository,
    private val dagRegistrar: DagRegistrar,
    private val leaderManager: LeaderManager,
    private val objectMapper: ObjectMapper,
    private val shutdownCoordinator: ShutdownCoordinator,
    private val autoscalingMetrics: AutoscalingMetrics,
    private val dagMetrics: DagMetrics,
    private val dagEventLog: DagEventLog,
) {

    private val log = Logger.getLogger(DagOrchestrator::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val templateEngine = TemplateEngine(objectMapper)
    private val conditionEvaluator = ConditionEvaluator()

    fun onStart(@Observes ev: StartupEvent) {
        shutdownCoordinator.registerLeaderScopeCallback { scope.cancel() }

        val interval = config.leader().monitorInterval().toMillis()
        scope.launch {
            delay(interval) // initial delay
            while (isActive) {
                if (leaderManager.isActive) {
                    val epoch = leaderManager.token
                    val loopStart = System.currentTimeMillis()
                    try {
                        withContext(Dispatchers.IO) {
                            FencingTokenHolder.withToken(epoch) {
                                monitorRuns()
                                promotePendingRuns()
                            }
                        }
                    } catch (e: Exception) {
                        log.errorf(e, "Error in DAG orchestrator loop")
                    }
                    dagMetrics.recordLeaderLoopDuration(System.currentTimeMillis() - loopStart)
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
     * Phase 1 (reconcile) → Phase 2 (timeout) → Phase 3/4 (evaluate) →
     * Phase 5 (dispatch) → Phase 6 (completion check).
     */
    private fun processRun(run: DagRun) {
        val reconciled = reconcile(run)
        val reaped = reapTimeouts(run)
        if (reconciled || reaped) {
            evaluate(run)
        }
        dispatch(run)
        checkCompletion(run)
        checkSlaDeadline(run)
    }

    // ──────────────────────────────────────────────────────────────
    //  Phase 1: Reconcile — sync Layer 1 task states to DAG instances
    // ──────────────────────────────────────────────────────────────

    /**
     * Poll RUNNING instances, check their backing Layer 1 task,
     * and update the instance to COMPLETED or FAILED.
     * Handles error classification for retry decisions.
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
                    dagEventLog.nodeStateChange(
                        run.runId, run.dagId, instance.taskKey,
                        TaskInstanceStatus.RUNNING, TaskInstanceStatus.COMPLETED,
                        instance.attempt,
                        instance.dispatchedAt?.let { Duration.between(it, Instant.now()).toMillis() },
                    )
                    dagMetrics.recordNodeDuration(
                        run.dagId, instance.taskKey, instance.taskType ?: instance.nodeType,
                        instance.dispatchedAt?.let { Duration.between(it, Instant.now()).toMillis() } ?: 0,
                    )
                    handleDynamicRouting(run, instance)
                    changed = true
                    log.debugf("Reconciled %s → COMPLETED (run=%s)", instance.taskKey, run.runId)
                }
                TaskStatus.DEAD_LETTER -> {
                    val errorClass = extractErrorClass(instance)
                    if (errorClass == ErrorClass.TRANSIENT && instance.attempt < instance.maxAttempts) {
                        scheduleRetry(run, instance)
                    } else {
                        val errorJson = objectMapper.writeValueAsString(
                            mapOf("class" to (errorClass?.name ?: "UNKNOWN"), "task_id" to taskId),
                        )
                        dagRepository.updateInstanceStatusWithError(
                            instance.instanceId, TaskInstanceStatus.FAILED, errorJson,
                        )
                        dagEventLog.nodeStateChange(
                            run.runId, run.dagId, instance.taskKey,
                            TaskInstanceStatus.RUNNING, TaskInstanceStatus.FAILED, instance.attempt,
                        )
                    }
                    changed = true
                    log.debugf("Reconciled %s → FAILED/RETRY (run=%s)", instance.taskKey, run.runId)
                }
                else -> { /* still in progress */ }
            }
        }

        return changed
    }

    /** Extract error classification from the instance's output data. */
    private fun extractErrorClass(instance: DagTaskInstance): ErrorClass? {
        val output = instance.outputData ?: return null
        return try {
            val tree = objectMapper.readTree(output)
            val classField = tree.get("__error_class__")?.asText() ?: return null
            ErrorClass.valueOf(classField.uppercase())
        } catch (_: Exception) {
            null
        }
    }

    /** Schedule a DAG-level retry with backoff. */
    private fun scheduleRetry(run: DagRun, instance: DagTaskInstance) {
        val blueprint = dagRegistrar.getBlueprint(run.dagId)
        val defaults = blueprint?.defaults()
        val backoff = defaults?.retryBackoff ?: com.mapreduce.dag.spi.RetryBackoff()

        val delayMs = computeBackoffDelay(instance.attempt, backoff)
        val dispatchAfter = Instant.now().plusMillis(delayMs)
        val nextAttempt = instance.attempt + 1

        dagRepository.prepareInstanceForRetry(instance.instanceId, nextAttempt, dispatchAfter)
        dagMetrics.incrementNodeRetry(run.dagId, instance.taskKey)
        dagEventLog.retryScheduled(
            run.runId, run.dagId, instance.taskKey, instance.attempt, nextAttempt, delayMs,
        )
        log.infof(
            "Retry scheduled: %s attempt %d → %d, delay=%dms (run=%s)",
            instance.taskKey, instance.attempt, nextAttempt, delayMs, run.runId,
        )
    }

    private fun computeBackoffDelay(attempt: Int, backoff: com.mapreduce.dag.spi.RetryBackoff): Long {
        val initialMs = backoff.initialDelay.toMillis()
        val maxMs = backoff.maxDelay.toMillis()
        val raw = when (backoff.strategy) {
            com.mapreduce.dag.spi.BackoffStrategy.FIXED -> initialMs
            com.mapreduce.dag.spi.BackoffStrategy.LINEAR -> initialMs * attempt
            com.mapreduce.dag.spi.BackoffStrategy.EXPONENTIAL -> initialMs * (1L shl (attempt - 1))
        }
        return raw.coerceAtMost(maxMs)
    }

    // ──────────────────────────────────────────────────────────────
    //  Phase 1b: Dynamic Branch Routing
    // ──────────────────────────────────────────────────────────────

    /**
     * If a completed task's output contains `__dag_route__`, forcefully
     * SKIP any immediate downstream nodes not in the routing array.
     */
    private fun handleDynamicRouting(run: DagRun, completedInstance: DagTaskInstance) {
        val output = completedInstance.outputData ?: return

        val routeNode = try {
            val tree = objectMapper.readTree(output)
            tree.get("__dag_route__") ?: return
        } catch (_: Exception) {
            return
        }

        if (!routeNode.isArray) return
        val routedKeys = routeNode.map { it.asText() }.toSet()

        val instances = dagRepository.findInstancesByRunId(run.runId)
        val skippedKeys = mutableListOf<String>()
        for (downstream in instances) {
            if (downstream.status != TaskInstanceStatus.BLOCKED) continue
            val deps = parseDependencies(downstream.dependencies)
            if (completedInstance.taskKey !in deps) continue

            if (downstream.taskKey !in routedKeys) {
                dagRepository.updateInstanceStatus(downstream.instanceId, TaskInstanceStatus.SKIPPED)
                dagEventLog.nodeStateChange(
                    run.runId, run.dagId, downstream.taskKey,
                    TaskInstanceStatus.BLOCKED, TaskInstanceStatus.SKIPPED,
                )
                skippedKeys.add(downstream.taskKey)
            }
        }

        if (skippedKeys.isNotEmpty()) {
            dagEventLog.dynamicRoute(run.runId, run.dagId, completedInstance.taskKey, routedKeys, skippedKeys)
            log.infof(
                "Dynamic routing from %s: routed=%s, skipped=%s (run=%s)",
                completedInstance.taskKey, routedKeys, skippedKeys, run.runId,
            )
        }
    }

    // ──────────────────────────────────────────────────────────────
    //  Phase 2: Timeout Reaping
    // ──────────────────────────────────────────────────────────────

    /**
     * Detect instances that have exceeded their timeout_at deadline.
     * Transition to TIMED_OUT and schedule retry if eligible.
     */
    private fun reapTimeouts(run: DagRun): Boolean {
        val timedOut = dagRepository.findTimedOutInstances(run.runId)
        if (timedOut.isEmpty()) return false

        for (instance in timedOut) {
            dagMetrics.incrementNodeTimeout(run.dagId, instance.taskKey)
            dagEventLog.timeoutReaped(run.runId, run.dagId, instance.taskKey, instance.attempt)

            if (instance.attempt < instance.maxAttempts) {
                scheduleRetry(run, instance)
                log.infof("Timeout reaped (retryable): %s attempt %d (run=%s)",
                    instance.taskKey, instance.attempt, run.runId)
            } else {
                dagRepository.updateInstanceStatusWithError(
                    instance.instanceId, TaskInstanceStatus.TIMED_OUT,
                    """{"class":"TIMED_OUT","attempt":${instance.attempt}}""",
                )
                dagEventLog.nodeStateChange(
                    run.runId, run.dagId, instance.taskKey,
                    instance.status, TaskInstanceStatus.TIMED_OUT, instance.attempt,
                )
                log.warnf("Timeout reaped (exhausted): %s (run=%s)", instance.taskKey, run.runId)
            }
        }
        return true
    }

    // ──────────────────────────────────────────────────────────────
    //  Phase 3/4: Evaluate trigger rules (with cascade + conditions)
    // ──────────────────────────────────────────────────────────────

    /**
     * Iterate BLOCKED nodes and evaluate trigger rules against upstream parents.
     * Also evaluates inline condition expressions.
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
                // ON_FAILURE nodes are only dispatched during completion check
                if (instance.triggerRule == TriggerRule.ON_FAILURE) continue

                val deps = parseDependencies(instance.dependencies)
                val allUpstream = deps.mapNotNull { instanceMap[it] }
                if (allUpstream.size != deps.size) {
                    log.warnf("Instance %s has unresolvable dependencies (run=%s)", instance.taskKey, run.runId)
                    continue
                }

                when (evaluateTriggerRule(instance.triggerRule, allUpstream)) {
                    EvalResult.READY -> {
                        // Evaluate condition expression if present
                        if (shouldSkipByCondition(run, instance, instanceMap)) {
                            dagRepository.updateInstanceStatus(instance.instanceId, TaskInstanceStatus.SKIPPED)
                            dagEventLog.nodeStateChange(
                                run.runId, run.dagId, instance.taskKey,
                                TaskInstanceStatus.BLOCKED, TaskInstanceStatus.SKIPPED,
                            )
                            changed = true
                        } else {
                            dagRepository.updateInstanceStatus(instance.instanceId, TaskInstanceStatus.READY)
                            changed = true
                        }
                    }
                    EvalResult.SKIP -> {
                        dagRepository.updateInstanceStatus(instance.instanceId, TaskInstanceStatus.SKIPPED)
                        dagEventLog.nodeStateChange(
                            run.runId, run.dagId, instance.taskKey,
                            TaskInstanceStatus.BLOCKED, TaskInstanceStatus.SKIPPED,
                        )
                        changed = true
                        log.debugf("Cascade skip: %s (run=%s)", instance.taskKey, run.runId)
                    }
                    EvalResult.WAIT -> { /* not all deps resolved yet */ }
                }
            }
        }
    }

    /**
     * Evaluate a node's inline condition expression.
     * Returns true if the condition evaluates to false (node should be SKIPPED).
     */
    private fun shouldSkipByCondition(
        run: DagRun,
        instance: DagTaskInstance,
        instanceMap: Map<String, DagTaskInstance>,
    ): Boolean {
        val blueprint = dagRegistrar.getBlueprint(run.dagId) ?: return false
        val nodeDef = blueprint.nodes().find { it.taskKey == instance.taskKey } ?: return false
        val condition = nodeDef.condition ?: return false

        return try {
            val xcom = buildXcomContext(instance, instanceMap)
            val inputs = run.globalContext?.let { objectMapper.readTree(it) }
            val ctx = TemplateEngine.ResolutionContext(
                runId = run.runId, dagId = run.dagId, inputs = inputs, xcom = xcom,
            )
            val resolved = templateEngine.resolve(condition, ctx)
            val result = conditionEvaluator.evaluate(resolved)
            dagEventLog.conditionEvaluated(run.runId, run.dagId, instance.taskKey, condition, result)
            !result // return true if should SKIP (condition is false)
        } catch (e: Exception) {
            log.warnf(e, "Condition evaluation failed for %s (run=%s), treating as SKIP",
                instance.taskKey, run.runId)
            true
        }
    }

    private enum class EvalResult { READY, SKIP, WAIT }

    private fun evaluateTriggerRule(rule: TriggerRule, upstream: List<DagTaskInstance>): EvalResult {
        val allTerminal = upstream.all { it.status.isTerminal }

        return when (rule) {
            TriggerRule.ALL_SUCCESS -> {
                if (upstream.any { it.status in setOf(TaskInstanceStatus.FAILED, TaskInstanceStatus.SKIPPED, TaskInstanceStatus.TIMED_OUT) }) {
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
                    EvalResult.SKIP
                } else {
                    EvalResult.WAIT
                }
            }
            TriggerRule.ALL_DONE -> {
                if (allTerminal) EvalResult.READY else EvalResult.WAIT
            }
            TriggerRule.NONE_FAILED -> {
                val hasFailed = upstream.any {
                    it.status == TaskInstanceStatus.FAILED || it.status == TaskInstanceStatus.TIMED_OUT
                }
                if (hasFailed) {
                    EvalResult.SKIP
                } else if (allTerminal) {
                    EvalResult.READY
                } else {
                    EvalResult.WAIT
                }
            }
            TriggerRule.ON_FAILURE -> {
                // Handled separately in checkCompletion
                EvalResult.WAIT
            }
        }
    }

    // ──────────────────────────────────────────────────────────────
    //  Phase 5: Dispatch READY nodes as Layer 1 tasks
    // ──────────────────────────────────────────────────────────────

    private fun dispatch(run: DagRun) {
        val readyInstances = dagRepository.findInstancesByRunAndStatus(run.runId, TaskInstanceStatus.READY)

        // Filter out instances with future dispatched_at (backoff not yet elapsed)
        val dispatchable = readyInstances.filter { instance ->
            instance.dispatchedAt == null || !instance.dispatchedAt.isAfter(Instant.now())
        }

        // Enforce max_parallel_nodes concurrency limit
        val blueprint = dagRegistrar.getBlueprint(run.dagId)
        val maxParallel = blueprint?.concurrency()?.maxParallelNodes ?: Int.MAX_VALUE
        val activeCount = dagRepository.findInstancesByRunId(run.runId).count {
            it.status == TaskInstanceStatus.QUEUED || it.status == TaskInstanceStatus.RUNNING
        }
        val slotsAvailable = (maxParallel - activeCount).coerceAtLeast(0)

        val toDispatch = if (slotsAvailable < dispatchable.size) {
            log.debugf("Concurrency limit: dispatching %d of %d ready nodes (max=%d, active=%d, run=%s)",
                slotsAvailable, dispatchable.size, maxParallel, activeCount, run.runId)
            dispatchable.take(slotsAvailable)
        } else {
            dispatchable
        }

        for (instance in toDispatch) {
            val payload = buildPayload(run, instance)

            // Compute timeout_at from node or blueprint defaults
            val nodeDef = blueprint?.nodes()?.find { it.taskKey == instance.taskKey }
            val timeout = nodeDef?.timeout ?: blueprint?.defaults()?.timeout
            val timeoutAt = timeout?.let { Instant.now().plus(it) }

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

            dagRepository.updateInstanceStatusAndTaskId(
                instance.instanceId, TaskInstanceStatus.RUNNING, taskId, timeoutAt,
            )

            // Track dispatch lag (time from READY to actual dispatch)
            if (instance.createdAt != null) {
                dagMetrics.recordDispatchLag(
                    run.dagId, Duration.between(instance.createdAt, Instant.now()).toMillis(),
                )
            }

            dagEventLog.nodeStateChange(
                run.runId, run.dagId, instance.taskKey,
                TaskInstanceStatus.READY, TaskInstanceStatus.RUNNING, instance.attempt,
            )
            log.debugf("Dispatched %s → task %s (run=%s, attempt=%d)",
                instance.taskKey, taskId, run.runId, instance.attempt)
        }
    }

    /**
     * Merge global_context and upstream output_data into a single JSON payload.
     * Resolves template expressions in node config if present.
     */
    private fun buildPayload(run: DagRun, instance: DagTaskInstance): String {
        val payloadMap = mutableMapOf<String, Any?>()

        val inputsNode = if (run.globalContext != null) {
            try {
                objectMapper.readTree(run.globalContext).also {
                    payloadMap["global_context"] = it
                }
            } catch (_: Exception) {
                payloadMap["global_context"] = run.globalContext
                null
            }
        } else {
            null
        }

        val deps = parseDependencies(instance.dependencies)
        val allInstances = if (deps.isNotEmpty()) dagRepository.findInstancesByRunId(run.runId) else emptyList()
        val instanceMap = allInstances.associateBy { it.taskKey }
        val xcom = buildXcomContext(instance, instanceMap)

        if (xcom.isNotEmpty()) {
            payloadMap["upstream"] = xcom.mapValues { (_, v) -> v }
        }

        // Resolve template expressions in node config
        val blueprint = dagRegistrar.getBlueprint(run.dagId)
        val nodeDef = blueprint?.nodes()?.find { it.taskKey == instance.taskKey }
        if (nodeDef != null && nodeDef.config.isNotEmpty()) {
            val ctx = TemplateEngine.ResolutionContext(
                runId = run.runId, dagId = run.dagId, inputs = inputsNode, xcom = xcom,
            )
            try {
                val resolvedConfig = templateEngine.resolveConfig(nodeDef.config, ctx)
                payloadMap["config"] = resolvedConfig
            } catch (e: Exception) {
                log.warnf(e, "Template resolution failed for %s (run=%s)", instance.taskKey, run.runId)
                payloadMap["config"] = nodeDef.config
            }
        }

        return objectMapper.writeValueAsString(payloadMap)
    }

    /** Build XCom context from upstream completed instances. */
    private fun buildXcomContext(
        instance: DagTaskInstance,
        instanceMap: Map<String, DagTaskInstance>,
    ): Map<String, JsonNode> {
        val deps = parseDependencies(instance.dependencies)
        val xcom = mutableMapOf<String, JsonNode>()
        for (depKey in deps) {
            val upstream = instanceMap[depKey]
            if (upstream?.outputData != null) {
                try {
                    xcom[depKey] = objectMapper.readTree(upstream.outputData)
                } catch (_: Exception) {
                    // skip malformed output
                }
            }
        }
        return xcom
    }

    // ──────────────────────────────────────────────────────────────
    //  Phase 6: Check run completion
    // ──────────────────────────────────────────────────────────────

    private fun checkCompletion(run: DagRun) {
        val instances = dagRepository.findInstancesByRunId(run.runId)

        // Exclude ON_FAILURE nodes from normal completion check
        val normalInstances = instances.filter { it.triggerRule != TriggerRule.ON_FAILURE }
        val allTerminal = normalInstances.all { it.status.isTerminal }

        if (!allTerminal || normalInstances.isEmpty()) return

        val hasFailed = normalInstances.any {
            it.status == TaskInstanceStatus.FAILED || it.status == TaskInstanceStatus.TIMED_OUT
        }
        val newStatus = if (hasFailed) DagRunStatus.FAILED else DagRunStatus.COMPLETED

        // Before finalizing FAILED, dispatch ON_FAILURE nodes
        if (hasFailed) {
            val onFailureNodes = instances.filter {
                it.triggerRule == TriggerRule.ON_FAILURE && it.status == TaskInstanceStatus.BLOCKED
            }
            if (onFailureNodes.isNotEmpty()) {
                for (node in onFailureNodes) {
                    dagRepository.updateInstanceStatus(node.instanceId, TaskInstanceStatus.READY)
                    dagEventLog.nodeStateChange(
                        run.runId, run.dagId, node.taskKey,
                        TaskInstanceStatus.BLOCKED, TaskInstanceStatus.READY,
                    )
                    log.infof("Dispatching ON_FAILURE handler: %s (run=%s)", node.taskKey, run.runId)
                }
                // Don't finalize yet — let ON_FAILURE nodes execute first
                return
            }

            // Check if ON_FAILURE nodes are still running
            val pendingOnFailure = instances.any {
                it.triggerRule == TriggerRule.ON_FAILURE && !it.status.isTerminal
            }
            if (pendingOnFailure) return
        }

        if (dagRepository.updateRunStatus(run.runId, DagRunStatus.RUNNING, newStatus)) {
            val durationMs = run.startedAt?.let { Duration.between(it, Instant.now()).toMillis() }
            dagEventLog.runStateChange(run.runId, run.dagId, DagRunStatus.RUNNING, newStatus, durationMs)
            dagMetrics.recordRunDuration(run.dagId, newStatus.name, durationMs ?: 0)
            if (run.createdAt != null) {
                autoscalingMetrics.recordOrchestrationDuration("DAG", run.dagId, run.createdAt)
            }
            log.infof("DAG run %s → %s (dag=%s)", run.runId, newStatus, run.dagId)
        }
    }

    /** Emit SLA breach alert if a run exceeds its deadline. */
    private fun checkSlaDeadline(run: DagRun) {
        val deadline = run.deadlineAt ?: return
        if (Instant.now().isAfter(deadline)) {
            dagEventLog.slaBreached(run.runId, run.dagId, deadline)
        }
    }

    // ──────────────────────────────────────────────────────────────
    //  Promote PENDING runs (respecting max_parallel_runs)
    // ──────────────────────────────────────────────────────────────

    private fun promotePendingRuns() {
        val pendingRuns = dagRepository.findRunsByStatus(DagRunStatus.PENDING)
        for (run in pendingRuns) {
            val blueprint = dagRegistrar.getBlueprint(run.dagId)
            val maxParallelRuns = blueprint?.concurrency()?.maxParallelRuns ?: Int.MAX_VALUE
            val activeRuns = dagRepository.countRunningRunsByDagId(run.dagId)

            if (activeRuns < maxParallelRuns) {
                if (dagRepository.updateRunStatus(run.runId, DagRunStatus.PENDING, DagRunStatus.RUNNING)) {
                    dagEventLog.runStateChange(run.runId, run.dagId, DagRunStatus.PENDING, DagRunStatus.RUNNING)
                    log.infof("Promoted PENDING → RUNNING: %s (dag=%s, active=%d/%d)",
                        run.runId, run.dagId, activeRuns + 1, maxParallelRuns)
                }
            } else {
                log.debugf("Run %s stays PENDING: max_parallel_runs reached (%d/%d)",
                    run.runId, activeRuns, maxParallelRuns)
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
