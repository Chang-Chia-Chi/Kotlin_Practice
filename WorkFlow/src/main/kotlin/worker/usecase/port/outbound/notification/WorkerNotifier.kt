package com.workflow.worker.usecase.port.outbound.notification

import java.time.Duration

/**
 * Notification layer for event-driven task wake-up.
 *
 * Workers suspend on a per-queue flow and wake instantly
 * when signaled. Three signal sources:
 * - Local: [signal] emits to the in-process flow and broadcasts to peers via HTTP.
 * - Remote: [onRemoteSignal] emits to the in-process flow only (no re-broadcast).
 * - Fallback: [awaitWork] times out after [Duration], triggering a poll probe.
 *
 * Correctness invariant: notifications are performance hints, never
 * correctness requirements. Removing the entire notification layer
 * degrades performance to fallback-poll mode (5s) but never affects
 * task claiming via SELECT FOR UPDATE SKIP LOCKED.
 */
interface WorkerNotifier {
    companion object {
        const val DEFAULT_QUEUE = "default"
    }

    /**
     * Signal that new work is available on [queueName].
     * Wakes local workers immediately and enqueues an asynchronous
     * broadcast to all peer pods via HTTP POST. The broadcast is
     * fire-and-forget: this method returns as soon as the local flow
     * is signaled. Individual peer failures are logged at debug level.
     * Called AFTER transaction commit.
     */
    suspend fun signal(queueName: String)

    /**
     * Handle a remote signal received via the internal HTTP endpoint.
     * Wakes local workers only -- does NOT re-broadcast to avoid loops.
     */
    fun onRemoteSignal(queueName: String)

    /**
     * Suspend until work is signaled on [queueName] or [timeout] expires.
     * Returns true if woken by a signal, false on timeout.
     */
    suspend fun awaitWork(
        queueName: String,
        timeout: Duration,
    ): Boolean
}
