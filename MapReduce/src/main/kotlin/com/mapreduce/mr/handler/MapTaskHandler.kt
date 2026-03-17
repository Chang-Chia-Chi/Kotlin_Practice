package com.mapreduce.mr.handler

import com.mapreduce.mr.shuffle.BlobStore
import com.mapreduce.mr.spi.MapReduceDefinition
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.spi.TaskHandler
import kotlinx.coroutines.flow.map
import org.jboss.logging.Logger

/**
 * Auto-generated handler for the map phase of a [MapReduceDefinition].
 *
 * Intermediate outputs are streamed to the external [BlobStore].
 * Returns the blob URI via [TaskResult.Success] — the framework handles
 * task completion and group counter increment atomically.
 */
class MapTaskHandler(
    private val definition: MapReduceDefinition<Any, Any, Any, Any>,
    private val blobStore: BlobStore,
) : TaskHandler {

    private val log = Logger.getLogger(MapTaskHandler::class.java)

    override val handlerName: String = "${definition.jobType}.map"

    override suspend fun handle(ctx: TaskContext): TaskResult {
        val jobId = ctx.groupId
            ?: return TaskResult.Failure("Map task ${ctx.taskId} has no groupId (jobId)")

        val input = definition.deserializeInput(ctx.payload)
        val outputFlow = definition.map(input)
            .map { definition.serializeOutput(it) }

        val blobUri = blobStore.write(jobId, ctx.taskId, 0, outputFlow)

        log.debugf("MAP %s completed (job=%s, blob=%s)", ctx.taskId, jobId, blobUri)
        return TaskResult.Success(
            outputUri = blobUri,
            outputMetadata = """{"partition_hash":0}""",
        )
    }
}
