package com.mapreduce.queue.worker

import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.pipeline.TaskPipeline
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.WorkflowStepRepository
import com.mapreduce.queue.repository.TaskRepository
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger

/**
 * Resolves the handler for a claimed task, executes it through the
 * [TaskPipeline], and routes the result to the appropriate repository.
 */
@ApplicationScoped
class TaskDispatcher(
    private val taskRepository: TaskRepository,
    private val workflowStepRepository: WorkflowStepRepository,
    private val handlerRegistry: HandlerRegistry,
    private val pipeline: TaskPipeline,
) {

    private val log = Logger.getLogger(TaskDispatcher::class.java)

    /** Execute a claimed task through the pipeline and process the result. */
    suspend fun execute(task: Task) {
        val handler = handlerRegistry.resolve(task.handler)
        if (handler == null) {
            log.errorf("No handler for '%s' — dead-lettering task %s", task.handler, task.taskId)
            val reason = "No handler registered for '${task.handler}'"
            if (task.stepId != null) {
                workflowStepRepository.deadLetterStepTask(task.taskId, task.stepId, reason, task.claimToken)
            } else {
                taskRepository.deadLetter(task.taskId, reason, task.claimToken)
            }
            return
        }

        val ctx = buildContext(task)
        val result = try {
            pipeline.execute(ctx, handler)
        } catch (e: Exception) {
            log.errorf(e, "Pipeline escaped with exception for handler '%s' task %s", task.handler, task.taskId)
            TaskResult.Failure("Pipeline error: ${e.javaClass.simpleName}: ${e.message}")
        }

        processResult(task, result, task.claimToken)
    }

    // ── Result processing ──────────────────────────────────────────

    private suspend fun processResult(task: Task, result: TaskResult, gen: String?) {
        when (result) {
            is TaskResult.Success -> {
                if (task.stepId != null) {
                    workflowStepRepository.resolveStepTask(
                        taskId = task.taskId, stepId = task.stepId,
                        claimToken = gen,
                        outputUri = result.outputUri, outputMetadata = result.outputMetadata,
                    )
                } else {
                    taskRepository.complete(task.taskId, gen)
                }
            }
            is TaskResult.Retry -> {
                if (result.consumeRetry) {
                    if (task.stepId != null) {
                        workflowStepRepository.failStepTask(
                            task.taskId, task.stepId, result.reason, result.delay, gen,
                        )
                    } else {
                        taskRepository.fail(task.taskId, result.reason, result.delay, gen)
                    }
                } else {
                    taskRepository.requeue(task.taskId, result.delay, gen)
                }
            }
            is TaskResult.Failure -> {
                if (task.stepId != null) {
                    workflowStepRepository.failStepTask(
                        task.taskId, task.stepId, result.message, claimToken = gen,
                    )
                } else {
                    taskRepository.fail(task.taskId, result.message, claimToken = gen)
                }
            }
            is TaskResult.DeadLetter -> {
                if (task.stepId != null) {
                    workflowStepRepository.deadLetterStepTask(
                        task.taskId, task.stepId, result.reason, gen,
                    )
                } else {
                    taskRepository.deadLetter(task.taskId, result.reason, gen)
                }
            }
        }
    }

    // ── Context building ───────────────────────────────────────────

    private fun buildContext(task: Task) = TaskContext(
        taskId = task.taskId,
        handler = task.handler,
        queue = task.queue,
        payload = task.payload,
        stepId = task.stepId,
        metadata = task.metadata,
        claimToken = task.claimToken,
        retryCount = task.retryCount,
        maxRetries = task.maxRetries,
    )
}
