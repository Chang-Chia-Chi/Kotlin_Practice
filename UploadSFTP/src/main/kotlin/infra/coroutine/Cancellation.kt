package infra.coroutine

import kotlinx.coroutines.Job
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.cancellation.CancellationException

fun isCancellation(
    coroutineContext: CoroutineContext,
    error: Throwable? = null,
): Boolean {
    // If job is missing then there is no cancellation
    val job = coroutineContext[Job] ?: return false

    return job.isCancelled || (error != null && error is CancellationException)
}
