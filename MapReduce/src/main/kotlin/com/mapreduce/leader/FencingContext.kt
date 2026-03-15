package com.mapreduce.leader

import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

/**
 * CoroutineContext-based fencing token propagation for suspend functions.
 *
 * Unlike [FencingTokenHolder] (ThreadLocal), this element travels with the
 * coroutine across suspension points and dispatcher switches. Use it when
 * business logic uses `suspend` functions or launches child coroutines.
 *
 * Usage:
 * ```
 * // Set by the interceptor or orchestrator:
 * withContext(FencingContext(epoch)) {
 *     // Read anywhere in the coroutine tree:
 *     val epoch = FencingContext.current()
 * }
 * ```
 */
class FencingContext(val epoch: Long) : AbstractCoroutineContextElement(FencingContext) {

    companion object Key : CoroutineContext.Key<FencingContext> {

        /**
         * Reads the fencing epoch from the current coroutine context.
         * Throws if no [FencingContext] is present.
         */
        suspend fun current(): Long {
            val ctx = coroutineContext[FencingContext]
                ?: throw IllegalStateException(
                    "FencingContext not in coroutine scope — wrap the call in withContext(FencingContext(epoch))",
                )
            return ctx.epoch
        }

        /**
         * Reads the fencing epoch from the current coroutine context,
         * or returns null if not present.
         */
        suspend fun currentOrNull(): Long? =
            coroutineContext[FencingContext]?.epoch
    }
}
