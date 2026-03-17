package com.mapreduce.mr.service

import com.mapreduce.mr.registry.MapReduceRegistrar
import com.mapreduce.mr.spi.unsafeCast
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.GroupStatus
import com.mapreduce.queue.model.TaskGroup
import com.mapreduce.queue.repository.TaskGroupRepository
import jakarta.enterprise.context.ApplicationScoped
import java.util.UUID

data class SubmitResult(
    val jobId: String,
    val totalTasks: Int,
)

@ApplicationScoped
class MapReduceService(
    private val taskGroupRepository: TaskGroupRepository,
    private val registrar: MapReduceRegistrar,
) {
    fun submitJob(jobType: String, params: String): SubmitResult {
        val definition = registrar.getDefinition(jobType)?.unsafeCast()
            ?: throw IllegalArgumentException("Unknown job type: $jobType")

        val parsedParams = definition.deserializeParams(params)
        val taskInputs = definition.split(parsedParams)
        if (taskInputs.isEmpty()) throw IllegalArgumentException("Split produced zero tasks")

        val jobId = UUID.randomUUID().toString()
        val group = TaskGroup(
            groupId = jobId,
            groupType = jobType,
            status = GroupStatus.ACTIVE,
            params = params,
            queue = definition.queue,
            phase = "map",
            phaseTotal = taskInputs.size,
            onCompleteHandler = "$jobType.__phase_complete",
            failurePolicy = definition.failurePolicy,
            failureThreshold = definition.failureThreshold,
        )
        val tasks = taskInputs.mapIndexed { i, input ->
            EnqueueRequest(
                handler = "$jobType.map",
                payload = definition.serializeInput(input),
                queue = definition.queue,
                groupId = jobId,
                metadata = """{"task_index":$i,"phase":"MAP"}""",
                maxRetries = definition.maxRetries,
            )
        }
        taskGroupRepository.submitGroup(group, tasks)
        return SubmitResult(jobId, taskInputs.size)
    }
}
