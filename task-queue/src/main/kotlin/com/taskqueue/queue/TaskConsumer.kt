package com.taskqueue.queue

import io.quarkus.scheduler.Scheduled
import jakarta.inject.Singleton
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jboss.logging.Logger

/**
 * Polls TASK_QUEUE on a fixed interval and dispatches claimed tasks to handlers.
 *
 * Runs on **every** pod (including the leader). Coordination is lock-free:
 * SELECT FOR UPDATE SKIP LOCKED ensures each task is processed by exactly one pod.
 *
 * ### Two-Phase Design
 *
 * **Phase 1 — Claim (inside TX, milliseconds):**
 *   MERGE + flip to PROCESSING + read payload → commit → release all row locks.
 *
 * **Phase 2 — Process (outside TX, no DB locks held):**
 *   For each claimed task: run handler concurrently (bounded by semaphore) →
 *   insert children → mark terminal status. Slow handlers never block other pods' claim queries.
 *
 * ### Failure Semantics
 *
 * - Handler throws + retries remaining → task moves to RETRYABLE (with exponential backoff).
 * - Handler throws + retries exhausted → task moves to DISCARDED.
 * - Handler returns [TaskResult.Cancel] → task moves to CANCELLED.
 * - Handler returns [TaskResult.Snooze] → task moves to SCHEDULED.
 * - Pod crashes mid-process → leader's stale reclaimer resets the task after timeout.
 * - Unknown taskType → task moves to DISCARDED immediately (no retry).
 *
 * ### Graceful Shutdown
 *
 * Handled by [@GracefulShutdown] interceptor: stops claiming new tasks, waits for
 * in-flight tasks to drain (up to `timeoutSeconds`), then returns. K8s
 * `terminationGracePeriodSeconds` should be set to timeoutSeconds + 5.
 *
 * ### Concurrency
 *
 * Claimed batch is processed concurrently using a coroutine [Semaphore] to bound
 * parallelism. Each handler gets its own [TaskEmitter] — no shared mutable state.
 */
@Singleton
class TaskConsumer(
    private val dao: TaskQueueDao,
    private val registry: TaskHandlerRegistry,
    @ConfigProperty(name = "task.consumer.batch-size", defaultValue = "50")
    private val batchSize: Int,
    @ConfigProperty(name = "task.consumer.concurrency", defaultValue = "10")
    private val concurrency: Int,
) {

    private val log = Logger.getLogger(TaskConsumer::class.java)

    private val semaphore by lazy { Semaphore(concurrency) }

    @GracefulShutdown(timeoutSeconds = 25)
    @Scheduled(every = "{task.consumer.poll-interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    fun poll() {
        // Phase 1: Claim — short TX, row locks held for milliseconds
        val claimed = try {
            dao.claimBatch(batchSize)
        } catch (e: Exception) {
            log.errorf(e, "Failed to claim tasks — will retry next cycle")
            return
        }

        if (claimed.isEmpty()) return
        log.debugf("Claimed %d task(s)", claimed.size)

        // Phase 2: Process — outside TX, concurrently bounded by semaphore
        runBlocking {
            processClaimedBatch(claimed)
        }
    }

    private suspend fun processClaimedBatch(claimed: List<TaskContext>) {
        coroutineScope {
            claimed.map { task ->
                launch {
                    semaphore.withPermit {
                        processSafely(task)
                    }
                }
            }.joinAll()
        }
    }

    /**
     * Process a single claimed task. Every code path leads to a terminal status update
     * (DONE, CANCELLED, DISCARDED, EXPIRED, or RETRYABLE/SCHEDULED for deferral), so no
     * task is left in PROCESSING indefinitely — unless the pod crashes, which the stale
     * reclaimer handles.
     */
    private fun processSafely(task: TaskContext) {
        // Pre-handler deadline check: the task may have expired between claim and execution
        if (task.isExpired()) {
            dao.markExpired(task.taskId)
            log.debugf("Task %d expired before processing", task.taskId)
            return
        }

        val handler = registry.getHandler(task.taskType)
        if (handler == null) {
            dao.markDiscarded(task.taskId, "No handler registered for taskType='${task.taskType}'", task.retryCount)
            log.warnf("No handler for taskType='%s' — task %d marked DISCARDED", task.taskType, task.taskId)
            return
        }

        val emitter = TaskEmitter(task.taskId)

        val result: TaskResult
        try {
            result = handler.handle(task, emitter)
        } catch (e: Exception) {
            handleFailure(task, e)
            return
        }

        // Dispatch on handler's return signal
        when (result) {
            is TaskResult.Success -> {
                try {
                    val children = emitter.drain()
                    if (children.isNotEmpty()) {
                        dao.insertChildren(task.taskId, children)
                        log.debugf("Task %d emitted %d children", task.taskId, children.size)
                    }
                    dao.markDone(task.taskId)
                } catch (e: Exception) {
                    log.errorf(e, "Post-handler persistence failed for task %d — scheduling retry", task.taskId)
                    handleFailure(task, e)
                }
            }

            is TaskResult.Snooze -> {
                dao.markSnoozed(task.taskId, result.duration.seconds)
                log.debugf("Task %d snoozed for %s", task.taskId, result.duration)
            }

            is TaskResult.Cancel -> {
                dao.markCancelled(task.taskId, result.reason)
                log.infof("Task %d cancelled: %s", task.taskId, result.reason)
            }
        }
    }

    /**
     * Decide retry (RETRYABLE with backoff) vs. discard based on remaining retry budget.
     */
    private fun handleFailure(task: TaskContext, error: Exception) {
        val message = "${error::class.simpleName}: ${error.message}"

        if (task.hasRetriesLeft()) {
            dao.markRetryable(task.taskId, message, task.retryCount)
            log.infof(
                "Task %d (type=%s) failed, retry %d/%d scheduled with backoff: %s",
                task.taskId, task.taskType, task.retryCount + 1, task.maxRetries, message,
            )
        } else {
            dao.markDiscarded(task.taskId, message, task.retryCount)
            log.warnf(
                "Task %d (type=%s) exhausted retries (%d/%d), discarded: %s",
                task.taskId, task.taskType, task.retryCount + 1, task.maxRetries, message,
            )
        }
    }
}
