package com.mapreduce.mr.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.mr.shuffle.BlobStore
import com.mapreduce.mr.spi.MapReduceDefinition
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.repository.TaskGroupRepository
import com.mapreduce.queue.spi.TaskHandler
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.map
import org.jboss.logging.Logger

/**
 * Auto-generated handler for the reduce phase of a [MapReduceDefinition].
 *
 * Reads blob URIs from completed map tasks in the `task` table (via
 * [TaskGroupRepository.streamTaskOutputs]), streams the actual intermediate
 * data from the external [BlobStore]. Returns the result via [TaskResult.Success].
 */
class ReduceTaskHandler(
    private val definition: MapReduceDefinition<Any, Any, Any, Any>,
    private val taskGroupRepository: TaskGroupRepository,
    private val blobStore: BlobStore,
    private val objectMapper: ObjectMapper,
) : TaskHandler {

    private val log = Logger.getLogger(ReduceTaskHandler::class.java)

    override val handlerName: String = "${definition.jobType}.reduce"

    @kotlinx.coroutines.ExperimentalCoroutinesApi
    override suspend fun handle(ctx: TaskContext): TaskResult {
        val jobId = ctx.groupId
            ?: return TaskResult.Failure("Reduce task ${ctx.taskId} has no groupId (jobId)")

        val mapHandler = "${definition.jobType}.map"
        val outputFlow = taskGroupRepository.streamTaskOutputs(jobId, mapHandler)
            .flatMapConcat { output -> blobStore.read(output.uri) }
            .map { definition.deserializeOutput(it) }

        val result = definition.reduce(outputFlow)
        definition.onCompleted(result)

        val resultMetadata = definition.serializeResult(result)
        log.infof("REDUCE %s completed (job=%s)", ctx.taskId, jobId)
        return TaskResult.Success(outputMetadata = resultMetadata)
    }
}
