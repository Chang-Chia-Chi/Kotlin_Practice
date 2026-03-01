package com.taskqueue.handlers

import com.taskqueue.housekeeping.RootTaskRequest
import com.taskqueue.housekeeping.TaskProducerJob
import com.taskqueue.queue.TaskQueueDao
import jakarta.inject.Singleton
import java.time.Duration
import java.time.Instant

/**
 * Example producer: emits a single REFRESH_PREMIUM_USERS root task on each cron tick.
 *
 * The root task is a lightweight "trigger" — the actual fan-out happens inside
 * [RefreshUsersHandler], which emits one child per premium user.
 *
 * Uses deduplication via [uniqueKey] to prevent duplicate active refresh tasks
 * if the previous one hasn't completed yet.
 */
@Singleton
class RefreshUsersProducerJob : TaskProducerJob {

    override val name = "refresh-premium-users"

    override fun produce(): List<RootTaskRequest> {
        return listOf(
            RootTaskRequest(
                taskType = "REFRESH_PREMIUM_USERS",
                priority = 3,
                deadlineAt = Instant.now().plus(Duration.ofHours(6)),
                uniqueKey = TaskQueueDao.generateUniqueKey("REFRESH_PREMIUM_USERS", null),
            )
        )
    }
}
