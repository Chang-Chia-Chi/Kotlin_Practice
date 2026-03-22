package com.workflow.leader

import java.time.Instant

/**
 * Read-only view of leader election state.
 *
 * All properties are safe to read from any thread without synchronization
 * (backed by lock-free [kotlinx.coroutines.flow.MutableStateFlow] in the production implementation).
 *
 * Consumers depend on this interface, never on the concrete manager.
 * The [io.quarkus.scheduler.Scheduled.SkipPredicate] pattern ([NotLeader]) uses [isActive].
 */
interface LeaderElection {
    /** Whether this instance currently holds the leader lease. */
    val isActive: Boolean

    /** Fencing epoch — sourced from K8s lease transitions counter. */
    val token: Long

    /** Timestamp of the last successful leader heartbeat. */
    val lastHeartbeat: Instant
}
