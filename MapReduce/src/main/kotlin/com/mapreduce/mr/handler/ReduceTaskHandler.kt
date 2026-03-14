package com.mapreduce.mr.handler

import com.mapreduce.mr.repository.JobRepository
import com.mapreduce.mr.spi.MapReduceDefinition
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.spi.TaskHandler
import kotlinx.coroutines.flow.map
import org.jboss.logging.Logger

/**
 * Auto-generated handler for the reduce phase of a [MapReduceDefinition].
 *
 * Streams all output records for the job, calls reduce, then onCompleted.
 * Atomic: task completion + result metadata happen in one transaction.
 */
class ReduceTaskHandler(
    private val definition: MapReduceDefinition<Any, Any, Any, Any>,
    private val jobRepository: JobRepository,
) : TaskHandler {

    private val log = Logger.getLogger(ReduceTaskHandler::class.java)

    override val handlerName: String = "${definition.jobType}.reduce"

    override suspend fun handle(ctx: TaskContext): TaskResult {
        val jobId = ctx.groupId
            ?: return TaskResult.Failure("Reduce task ${ctx.taskId} has no groupId (jobId)")

        val outputFlow = jobRepository.streamOutputs(jobId)
            .map { definition.deserializeOutput(it) }

        val result = definition.reduce(outputFlow)

        val resultMetadata = definition.serializeResult(result)
        jobRepository.completeReduceTask(ctx.taskId, jobId, resultMetadata)

        definition.onCompleted(result)

        log.infof("REDUCE %s completed (job=%s)", ctx.taskId, jobId)
        return TaskResult.Success
    }
}
