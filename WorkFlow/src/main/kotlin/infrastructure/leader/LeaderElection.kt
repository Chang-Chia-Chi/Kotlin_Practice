package com.workflow.infrastructure.leader

import java.time.Instant

interface LeaderElection {
    val isActive: Boolean
    val token: Long
    val lastHeartbeat: Instant
}
