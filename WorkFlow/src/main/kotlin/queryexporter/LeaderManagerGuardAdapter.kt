package com.workflow.queryexporter

import com.workflow.leader.LeaderManager
import com.workflow.queryexporter.spi.LeaderGuard
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.flow.StateFlow

@ApplicationScoped
class LeaderManagerGuardAdapter(
    private val leaderManager: LeaderManager,
) : LeaderGuard {
    override val leaderState: StateFlow<Boolean> get() = leaderManager.leaderState
}
