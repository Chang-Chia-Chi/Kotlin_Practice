package com.workflow.infrastructure.shutdown

import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

class ShutdownSignal(
    private val supplier: () -> Boolean,
) : AbstractCoroutineContextElement(ShutdownSignal) {

    companion object Key : CoroutineContext.Key<ShutdownSignal>

    val isShuttingDown: Boolean get() = supplier()
}

suspend fun isShuttingDown(): Boolean =
    coroutineContext[ShutdownSignal]?.isShuttingDown ?: false
