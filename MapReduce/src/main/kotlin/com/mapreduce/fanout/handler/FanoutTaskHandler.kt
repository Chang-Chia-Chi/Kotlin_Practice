package com.mapreduce.fanout.handler

import com.mapreduce.fanout.repository.FanoutJobRepository
import com.mapreduce.fanout.spi.FanoutDefinition
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.spi.TaskHandler
import org.jboss.logging.Logger

/**
 * Auto-generated handler for the execute phase of a [FanoutDefinition].
 *
 * Not a CDI bean — instantiated by [com.mapreduce.fanout.registry.FanoutRegistrar]
 * and registered programmatically with the [com.mapreduce.queue.registry.HandlerRegistry].
 *
 * Unlike [com.mapreduce.mr.handler.MapTaskHandler], there are no intermediate outputs
 * and no blob store interaction. Task completion + counter increment happen atomically
 * in one Oracle transaction.
 */
class FanoutTaskHandler(
    private val definition: FanoutDefinition<Any, Any>,
    private val fanoutJobRepository: FanoutJobRepository,
) : TaskHandler {

    private val log = Logger.getLogger(FanoutTaskHandler::class.java)

    override val handlerName: String = "${definition.jobType}.execute"

    override suspend fun handle(ctx: TaskContext): TaskResult {
        val jobId = ctx.groupId
            ?: return TaskResult.Failure("Fanout task ${ctx.taskId} has no groupId (jobId)")

        val input = definition.deserializeInput(ctx.payload)

        definition.execute(input)

        // Atomic: mark task COMPLETED + increment completed_tasks
        // Fenced by execution_generation to prevent zombie commits
        fanoutJobRepository.completeFanoutTask(ctx.taskId, jobId, ctx.executionGeneration)

        log.debugf("EXECUTE %s completed (job=%s)", ctx.taskId, jobId)
        return TaskResult.Success()
    }
}
