package com.mapreduce.queue.worker

import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.pipeline.TaskExecutionContext
import com.mapreduce.queue.pipeline.TaskPipeline
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.TaskGroupRepository
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger

/**
 * Resolves the handler for a claimed task, executes it through the
 * [TaskPipeline], and routes the result to the appropriate repository.
 */
@ApplicationScoped
class TaskDispatcher(
    private val taskRepository: TaskRepository,
    private val taskGroupRepository: TaskGroupRepository,
    private val handlerRegistry: HandlerRegistry,
    private val pipeline: TaskPipeline,
    private val shutdownCoordinator: ShutdownCoordinator,
) {

    private val log = Logger.getLogger(TaskDispatcher::class.java)

    /** Execute a claimed task through the pipeline and process the result. */
    suspend fun execute(task: Task) {
        val handler = handlerRegistry.resolve(task.handler)
        if (handler == null) {
            log.errorf("No handler for '%s' — dead-lettering task %s", task.handler, task.taskId)
            val reason = "No handler registered for '${task.handler}'"
            if (task.groupId != null) {
                taskGroupRepository.deadLetterGroupTask(task.taskId, task.groupId, reason, task.claimToken)
            } else {
                taskRepository.deadLetter(task.taskId, reason, task.claimToken)
            }
            return
        }

        val ctx = buildExecutionContext(task)
        val result = try {
            pipeline.execute(ctx, handler)
        } catch (e: Exception) {
            log.errorf(e, "Pipeline escaped with exception for handler '%s' task %s", task.handler, task.taskId)
            TaskResult.Failure("Pipeline error: ${e.javaClass.simpleName}: ${e.message}")
        }

        processResult(task, result, task.claimToken)
    }

    // ── Result processing ──────────────────────────────────────────

    private fun processResult(task: Task, result: TaskResult, gen: String?) {
        when (result) {
            is TaskResult.Success -> {
                if (task.groupId != null) {
                    taskGroupRepository.resolveGroupTask(
                        taskId = task.taskId, groupId = task.groupId,
                        claimToken = gen,
                        outputUri = result.outputUri, outputMetadata = result.outputMetadata,
                    )
                } else {
                    taskRepository.complete(task.taskId, gen)
                }
            }
            is TaskResult.Retry -> {
                if (result.consumeRetry) {
                    if (task.groupId != null) {
                        taskGroupRepository.failGroupTask(
                            task.taskId, task.groupId, result.reason, result.delay, gen,
                        )
                    } else {
                        taskRepository.fail(task.taskId, result.reason, result.delay, gen)
                    }
                } else {
                    taskRepository.requeue(task.taskId, result.delay, gen)
                }
            }
            is TaskResult.Failure -> {
                if (task.groupId != null) {
                    taskGroupRepository.failGroupTask(
                        task.taskId, task.groupId, result.message, claimToken = gen,
                    )
                } else {
                    taskRepository.fail(task.taskId, result.message, claimToken = gen)
                }
            }
            is TaskResult.DeadLetter -> {
                if (task.groupId != null) {
                    taskGroupRepository.deadLetterGroupTask(
                        task.taskId, task.groupId, result.reason, gen,
                    )
                } else {
                    taskRepository.deadLetter(task.taskId, result.reason, gen)
                }
            }
        }
    }

    // ── Context building ───────────────────────────────────────────

    private fun buildExecutionContext(task: Task) = TaskExecutionContext(
        taskId = task.taskId,
        handler = task.handler,
        queue = task.queue,
        groupId = task.groupId,
        payload = task.payload,
        metadata = task.metadata,
        retryCount = task.retryCount,
        maxRetries = task.maxRetries,
        claimedAt = task.claimedAt,
        claimToken = task.claimToken,
        taskContext = TaskContext(
            task.taskId, task.payload, task.groupId, task.metadata, task.claimToken,
            retryCount = task.retryCount, maxRetries = task.maxRetries,
            shuttingDownSupplier = { shutdownCoordinator.isShuttingDown },
        ),
    )
}
