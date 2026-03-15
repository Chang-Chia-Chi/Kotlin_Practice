package com.mapreduce.shutdown

/**
 * State machine for the graceful shutdown protocol.
 *
 * ```
 * RUNNING ──► DRAINING ──► RELEASING ──► TERMINATED
 *                │
 *                │ (drain timeout expired)
 *                ▼
 *            RELEASING ──► TERMINATED
 * ```
 */
enum class ShutdownState {
    /** Normal operation. Workers claim tasks, leader orchestrates. */
    RUNNING,

    /** No new claims. In-flight tasks run to completion (or until timeout). */
    DRAINING,

    /** Uncompleted tasks are flipped back to PENDING. Leader lease is released. */
    RELEASING,

    /** All cleanup done. Process may exit. */
    TERMINATED,
}
