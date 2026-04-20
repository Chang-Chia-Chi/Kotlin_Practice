package com.taskqueue.handlers

import com.taskqueue.queue.TaskContext
import com.taskqueue.queue.TaskEmitter
import com.taskqueue.queue.TaskHandler
import com.taskqueue.queue.TaskResult
import jakarta.inject.Singleton
import org.jboss.logging.Logger

/**
 * Leaf handler: processes a single user status update.
 *
 * No children emitted — this is a terminal task in the fan-out tree.
 *
 * ### Idempotency
 * The external API call should be idempotent (PUT semantics), or the handler
 * should check for a previously completed update before calling the API.
 */
@Singleton
class UpdateUserStatusHandler : TaskHandler {

    private val log = Logger.getLogger(UpdateUserStatusHandler::class.java)

    override val taskType = "UPDATE_USER_STATUS"

    override fun handle(ctx: TaskContext, emitter: TaskEmitter): TaskResult {
        val userId = parseUserId(ctx.payload)

        // TODO: inject your actual API client here
        // apiClient.refreshStatus(userId)
        log.debugf("Updated status for user %s (task %d)", userId, ctx.taskId)

        return TaskResult.Success
    }

    private fun parseUserId(payload: String?): String {
        requireNotNull(payload) { "UPDATE_USER_STATUS requires a payload with userId" }
        // Minimal JSON parse — replace with kotlinx.serialization for production payloads
        return payload.substringAfter("\"userId\":\"").substringBefore("\"")
    }
}
