package dynacache.cluster

import java.util.concurrent.CompletableFuture
import kotlinx.coroutines.future.await as awaitFuture

/**
 * The bridge from the engine's futures to coroutines (plan 2.5): the cluster module awaits a
 * `submit` here and never blocks a thread on it. It is kotlinx's own bridge under the cluster's
 * name, so no caller imports kotlinx for it.
 */
suspend fun <T> CompletableFuture<T>.await(): T = awaitFuture()
