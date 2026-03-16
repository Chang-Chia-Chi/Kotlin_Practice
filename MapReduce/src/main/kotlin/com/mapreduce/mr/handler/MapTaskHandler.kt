package com.mapreduce.mr.handler

import com.mapreduce.mr.repository.JobRepository
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
 * Intermediate outputs are streamed to the external [BlobStore] — the database
 * `mr_output` table stores only the blob URI, never the data.
 * Task completion + counter increment happen atomically in one Oracle transaction.
 */
class MapTaskHandler(
    private val definition: MapReduceDefinition<Any, Any, Any, Any>,
    private val jobRepository: JobRepository,
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

        jobRepository.completeMapTask(ctx.taskId, jobId, blobUri, ctx.executionGeneration, 0)

        log.debugf("MAP %s completed (job=%s, blob=%s)", ctx.taskId, jobId, blobUri)
        return TaskResult.Success()
    }
}
