package com.mapreduce.dag.api.dto

import com.mapreduce.dag.model.DagRun
import com.mapreduce.dag.model.DagTaskInstance

data class DagRunResponse(
    val runId: String,
    val dagId: String,
    val status: String,
    val nodes: List<NodeInstanceResponse>,
    val createdAt: String?,
    val updatedAt: String?,
) {
    companion object {
        fun from(run: DagRun, instances: List<DagTaskInstance>) = DagRunResponse(
            runId = run.runId,
            dagId = run.dagId,
            status = run.status.name,
            nodes = instances.map { NodeInstanceResponse.from(it) },
            createdAt = run.createdAt?.toString(),
            updatedAt = run.updatedAt?.toString(),
        )
    }
}

data class NodeInstanceResponse(
    val instanceId: String,
    val taskKey: String,
    val nodeType: String,
    val status: String,
    val dependencies: String?,
    val triggerRule: String,
    val outputData: String?,
) {
    companion object {
        fun from(instance: DagTaskInstance) = NodeInstanceResponse(
            instanceId = instance.instanceId,
            taskKey = instance.taskKey,
            nodeType = instance.nodeType,
            status = instance.status.name,
            dependencies = instance.dependencies,
            triggerRule = instance.triggerRule.name,
            outputData = instance.outputData,
        )
    }
}
