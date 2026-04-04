package com.workflow.infrastructure.coroutine

import kotlinx.coroutines.CancellationException

/**
 * Coroutine-safe alternative to [runCatching] that rethrows [CancellationException].
 *
 * Standard [runCatching] swallows [CancellationException], breaking structured
 * concurrency. This variant preserves cancellation semantics while capturing
 * all other exceptions into a [Result].
 */
@Suppress("TooGenericExceptionCaught")
inline fun <T> suspendCatching(block: () -> T): Result<T> =
    try {
        Result.success(block())
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        Result.failure(e)
    }
