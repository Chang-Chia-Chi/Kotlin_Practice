package com.mapreduce.mr.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.mr.repository.JobRepository
import com.mapreduce.mr.shuffle.BlobStore
import com.mapreduce.mr.spi.MapReduceDefinition
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.spi.TaskHandler
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.map
import org.jboss.logging.Logger

/**
 * Auto-generated handler for the reduce phase of a [MapReduceDefinition].
 *
 * Reads blob URIs from the database, streams the actual intermediate data
 * directly from the external [BlobStore], bypassing the database for data
 * movement entirely. Calls reduce, then onCompleted.
 *
 * Atomic: task completion + result metadata happen in one transaction.
 */
class ReduceTaskHandler(
    private val definition: MapReduceDefinition<Any, Any, Any, Any>,
    private val jobRepository: JobRepository,
    private val blobStore: BlobStore,
    private val objectMapper: ObjectMapper,
) : TaskHandler {

    private val log = Logger.getLogger(ReduceTaskHandler::class.java)

    override val handlerName: String = "${definition.jobType}.reduce"

    @kotlinx.coroutines.ExperimentalCoroutinesApi
    override suspend fun handle(ctx: TaskContext): TaskResult {
        val jobId = ctx.groupId
            ?: return TaskResult.Failure("Reduce task ${ctx.taskId} has no groupId (jobId)")

        val partitionHash = extractPartitionHash(ctx.metadata)

        // Read blob URIs from DB, then stream data from external blob store
        val outputFlow = jobRepository.streamBlobUris(jobId, partitionHash)
            .flatMapConcat { uri -> blobStore.read(uri) }
            .map { definition.deserializeOutput(it) }

        val result = definition.reduce(outputFlow)

        val resultMetadata = definition.serializeResult(result)
        // Fenced by execution_generation to prevent zombie commits
        jobRepository.completeReduceTask(ctx.taskId, jobId, resultMetadata, ctx.executionGeneration)

        definition.onCompleted(result)

        log.infof("REDUCE %s completed (job=%s, partition=%s)", ctx.taskId, jobId, partitionHash?.toString() ?: "all")
        return TaskResult.Success()
    }

    private fun extractPartitionHash(metadata: String?): Int? {
        if (metadata == null) return null
        return try {
            objectMapper.readTree(metadata).get("partition_hash")?.asInt()
        } catch (_: Exception) {
            null
        }
    }
}
