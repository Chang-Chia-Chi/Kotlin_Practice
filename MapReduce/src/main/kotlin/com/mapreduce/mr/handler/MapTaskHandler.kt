package com.mapreduce.mr.handler

import com.mapreduce.mr.repository.JobRepository
import com.mapreduce.mr.spi.MapReduceDefinition
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.spi.TaskHandler
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
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
        val serialized = definition.map(input)
            .map { definition.serializeOutput(it) }
            .toList()

        // Atomic: persist outputs + mark task COMPLETED + increment completed_tasks
        jobRepository.completeMapTask(ctx.taskId, jobId, serialized)

        log.debugf("MAP %s completed: %d outputs (job=%s)", ctx.taskId, serialized.size, jobId)
        return TaskResult.Success
    }
}
