package com.workflow.shutdown

/**
 * State machine for the graceful shutdown protocol.
 *
 * ```
 * RUNNING ──► DRAINING ──► TERMINATED
 *                │
 *                │ (global timeout expired)
 *                ▼
 *            TERMINATED
 * ```
 *
 * Task release and leader lease release are handled by [ShutdownParticipant]
 * implementations during the DRAINING phase, so no separate RELEASING state
 * is needed.
 */
enum class ShutdownState {
    /** Normal operation. Workers claim tasks, leader orchestrates. */
    RUNNING,

    /** No new claims. In-flight tasks run to completion (or until timeout). */
    DRAINING,

    /** All cleanup done. Process may exit. */
    TERMINATED,
}
