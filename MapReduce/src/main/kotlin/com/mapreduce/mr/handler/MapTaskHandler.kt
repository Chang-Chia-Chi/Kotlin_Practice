package com.mapreduce.mr.handler

import com.mapreduce.mr.repository.JobRepository
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
 * Execution is atomic: outputs + task completion + counter increment happen
 * in one Oracle transaction via [JobRepository.completeMapTask].
 */
class MapTaskHandler(
    private val definition: MapReduceDefinition<Any, Any, Any, Any>,
    private val jobRepository: JobRepository,
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

        // Atomic: persist outputs in chunks + mark task COMPLETED + increment completed_tasks
        // Fenced by execution_generation to prevent zombie commits
        jobRepository.completeMapTask(ctx.taskId, jobId, outputFlow, ctx.executionGeneration, partitionHash)

        log.debugf("MAP %s completed (job=%s)", ctx.taskId, jobId)
        return TaskResult.Success
    }
}
