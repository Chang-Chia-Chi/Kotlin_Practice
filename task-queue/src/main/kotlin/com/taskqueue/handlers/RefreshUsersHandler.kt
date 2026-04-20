package com.taskqueue.handlers

import com.taskqueue.queue.TaskContext
import com.taskqueue.queue.TaskEmitter
import com.taskqueue.queue.TaskHandler
import com.taskqueue.queue.TaskResult
import jakarta.inject.Singleton
import org.jdbi.v3.core.Jdbi
import org.jboss.logging.Logger
import java.time.Duration
import java.time.Instant

/**
 * Fan-out handler: queries premium users and emits one child task per user.
 *
 * Root task (no payload needed) → N children of type "UPDATE_USER_STATUS".
 * The children are distributed across all pods via SKIP LOCKED automatically.
 */
@Singleton
class RefreshUsersHandler(private val jdbi: Jdbi) : TaskHandler {

    private val log = Logger.getLogger(RefreshUsersHandler::class.java)

    override val taskType = "REFRESH_PREMIUM_USERS"

    override fun handle(ctx: TaskContext, emitter: TaskEmitter): TaskResult {
        val users = jdbi.withHandle<List<String>, Exception> { handle ->
            handle.createQuery("SELECT USER_ID FROM USERS WHERE TIER = 'PREMIUM'")
                .mapTo(String::class.java)
                .list()
        }

        log.infof("Refreshing %d premium users (task %d)", users.size, ctx.taskId)

        val deadline = Instant.now().plus(Duration.ofHours(4))

        emitter.emitAll(
            taskType = "UPDATE_USER_STATUS",
            payloads = users.map { """{"userId":"$it"}""" },
            deadlineAt = deadline,
        )

        return TaskResult.Success
    }
}
