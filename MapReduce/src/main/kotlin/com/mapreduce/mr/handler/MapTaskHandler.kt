package com.mapreduce.mr.handler

import com.mapreduce.mr.repository.JobRepository
import com.mapreduce.mr.shuffle.BlobStore
import com.mapreduce.mr.spi.MapReduceDefinition
import com.mapreduce.mr.spi.PartitionedMapReduceDefinition
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.spi.TaskHandler
import kotlinx.coroutines.flow.map
import org.jboss.logging.Logger

/**
 * Auto-generated handler for the map phase of a [MapReduceDefinition].
 *
 * Not a CDI bean — instantiated by [com.mapreduce.mr.registry.MapReduceRegistrar]
 * and registered programmatically with the [com.mapreduce.queue.registry.HandlerRegistry].
 *
 * Intermediate outputs are streamed to the external [BlobStore] — the database
 * `mr_output` table stores only the blob URI and partition hash, never the data.
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

        val partitionHash = if (definition is PartitionedMapReduceDefinition<*, *, *, *>) {
            @Suppress("UNCHECKED_CAST")
            val partitioned = definition as PartitionedMapReduceDefinition<Any, Any, Any, Any>
            val rawInput = definition.deserializeInput(ctx.payload)
            partitioned.partitionFor(rawInput)
        } else {
            0
        }

        // Phase 1: Stream intermediate outputs to external blob store
        val blobUri = blobStore.write(jobId, ctx.taskId, partitionHash, outputFlow)

        // Phase 2: Atomic — persist blob URI + mark task COMPLETED + increment completed_tasks
        // Fenced by execution_generation to prevent zombie commits
        jobRepository.completeMapTask(ctx.taskId, jobId, blobUri, ctx.executionGeneration, partitionHash)

        log.debugf("MAP %s completed (job=%s, blob=%s)", ctx.taskId, jobId, blobUri)
        return TaskResult.Success
    }
}
