package com.taskqueue.queue

/**
 * Contract for processing a specific [taskType].
 *
 * Implementations must be CDI beans (typically `@Singleton`). The registry
 * discovers them automatically at startup via `Instance<TaskHandler>`.
 *
 * ### Idempotency contract
 * Handlers *should* be idempotent. The framework guarantees at-least-once delivery:
 * a pod crash mid-processing causes the stale reclaimer to re-queue the task.
 * Use an idempotency key in the payload if your side-effects are not naturally idempotent.
 *
 * ### Error handling
 * - Throw any exception → the consumer will retry (up to [TaskContext.maxRetries]), then discard.
 * - Return [TaskResult.Success] → the consumer marks the task DONE and inserts emitted children.
 * - Return [TaskResult.Snooze] → the task is deferred for re-execution after the given duration.
 * - Return [TaskResult.Cancel] → the task is cancelled immediately with a reason (no retry).
 */
interface TaskHandler {

    /** Unique discriminator. Must match the TASK_TYPE column value exactly. */
    val taskType: String

    /**
     * Execute the task's business logic.
     *
     * @param ctx     immutable snapshot of the claimed task row
     * @param emitter accumulator for child tasks; call [TaskEmitter.emit] / [TaskEmitter.emitAll]
     * @return        signal for the consumer: [TaskResult.Success], [TaskResult.Snooze], or [TaskResult.Cancel]
     */
    fun handle(ctx: TaskContext, emitter: TaskEmitter): TaskResult
}
