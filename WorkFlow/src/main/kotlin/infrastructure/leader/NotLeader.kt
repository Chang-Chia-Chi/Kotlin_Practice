package com.workflow.infrastructure.leader

import io.quarkus.scheduler.Scheduled
import io.quarkus.scheduler.ScheduledExecution
import jakarta.inject.Singleton

@Singleton
class NotLeader(private val leaderElection: LeaderElection) : Scheduled.SkipPredicate {
    override fun test(execution: ScheduledExecution) = !leaderElection.isActive
}
