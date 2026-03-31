package com.workflow.infrastructure.queryexporter.adapter

import com.workflow.infrastructure.leader.LeaderManager
import com.workflow.infrastructure.queryexporter.spi.LeaderGuard
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.flow.StateFlow

@ApplicationScoped
class LeaderManagerGuardAdapter(
    private val leaderManager: LeaderManager,
) : LeaderGuard {
    override val leaderState: StateFlow<Boolean> get() = leaderManager.leaderState
}
