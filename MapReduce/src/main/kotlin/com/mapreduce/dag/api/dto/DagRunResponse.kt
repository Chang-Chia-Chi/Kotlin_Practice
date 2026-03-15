package com.mapreduce.dag.api.dto

import com.mapreduce.dag.model.DagRun
import com.mapreduce.dag.model.DagTaskInstance

data class DagRunResponse(
    val runId: String,
    val dagId: String,
    val status: String,
    val triggerType: String,
    val parentRunId: String?,
    val nodes: List<NodeInstanceResponse>,
    val startedAt: String?,
    val completedAt: String?,
    val deadlineAt: String?,
    val createdAt: String?,
    val updatedAt: String?,
) {
    companion object {
        fun from(run: DagRun, instances: List<DagTaskInstance>) = DagRunResponse(
            runId = run.runId,
            dagId = run.dagId,
            status = run.status.name,
            triggerType = run.triggerType.name,
            parentRunId = run.parentRunId,
            nodes = instances.map { NodeInstanceResponse.from(it) },
            startedAt = run.startedAt?.toString(),
            completedAt = run.completedAt?.toString(),
            deadlineAt = run.deadlineAt?.toString(),
            createdAt = run.createdAt?.toString(),
            updatedAt = run.updatedAt?.toString(),
        )
    }
}

data class NodeInstanceResponse(
    val instanceId: String,
    val taskKey: String,
    val nodeType: String,
    val taskType: String?,
    val status: String,
    val dependencies: String?,
    val triggerRule: String,
    val attempt: Int,
    val maxAttempts: Int,
    val outputData: String?,
    val error: String?,
    val dispatchedAt: String?,
    val completedAt: String?,
) {
    companion object {
        fun from(instance: DagTaskInstance) = NodeInstanceResponse(
            instanceId = instance.instanceId,
            taskKey = instance.taskKey,
            nodeType = instance.nodeType,
            taskType = instance.taskType,
            status = instance.status.name,
            dependencies = instance.dependencies,
            triggerRule = instance.triggerRule.name,
            attempt = instance.attempt,
            maxAttempts = instance.maxAttempts,
            outputData = instance.outputData,
            error = instance.error,
            dispatchedAt = instance.dispatchedAt?.toString(),
            completedAt = instance.completedAt?.toString(),
        )
    }
}
