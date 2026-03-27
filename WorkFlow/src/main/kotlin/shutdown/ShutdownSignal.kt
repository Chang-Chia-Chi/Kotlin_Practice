package com.workflow.shutdown

import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

/**
 * Coroutine context element that propagates shutdown awareness.
 *
 * Installed by [WorkerLoop][com.workflow.worker.WorkerLoop] at the
 * scope level so all task execution coroutines inherit it. Middlewares and
 * handlers query shutdown state via the top-level [isShuttingDown] function.
 */
class ShutdownSignal(
    private val supplier: () -> Boolean,
) : AbstractCoroutineContextElement(ShutdownSignal) {

    companion object Key : CoroutineContext.Key<ShutdownSignal>

    /** Returns true if the pod is shutting down. */
    val isShuttingDown: Boolean get() = supplier()
}

/**
 * Returns true if the current coroutine is running during shutdown.
 *
 * Falls back to `false` when no [ShutdownSignal] is installed in the
 * coroutine context (e.g., in unit tests that don't install one).
 */
suspend fun isShuttingDown(): Boolean =
    coroutineContext[ShutdownSignal]?.isShuttingDown ?: false
