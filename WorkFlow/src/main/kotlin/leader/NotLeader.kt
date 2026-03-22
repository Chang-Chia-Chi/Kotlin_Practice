package com.workflow.leader

import io.quarkus.scheduler.Scheduled
import io.quarkus.scheduler.ScheduledExecution
import jakarta.inject.Singleton

/**
 * Quarkus [Scheduled.SkipPredicate] that silently skips execution
 * when this pod is not the leader.
 *
 * Usage: `@Scheduled(skipExecutionIf = NotLeader::class)`
 */
@Singleton
class NotLeader(private val leaderElection: LeaderElection) : Scheduled.SkipPredicate {
    override fun test(execution: ScheduledExecution) = !leaderElection.isActive
}
