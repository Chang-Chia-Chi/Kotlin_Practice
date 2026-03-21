package com.workflow.extension

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import org.jboss.logging.Logger

/**
 * Emits [value] indefinitely. Useful as the "tick" source for a poll loop
 * — downstream operators control concurrency and cancellation.
 */
fun <T> indefinitelyRepeat(value: T): Flow<T> =
    flow {
        while (true) emit(value)
    }

/**
 * Maps each upstream element through [transform] with bounded concurrency.
 *
 * Up to [concurrency] transforms run simultaneously. Results are emitted
 * in completion order (not upstream order). Back-pressures upstream when
 * all slots are occupied.
 *
 * Implementation uses [channelFlow] + [Semaphore] to avoid the race
 * conditions present in some other implementations.
 */
fun <T, R> Flow<T>.unorderedMapAsync(
    concurrency: Int,
    transform: suspend (T) -> R,
): Flow<R> =
    channelFlow {
        val semaphore = Semaphore(concurrency)
        collect { value ->
            semaphore.acquire() // back-pressure: suspends when all slots full
            launch {
                try {
                    send(transform(value))
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    // Isolate transform failures so one bad element doesn't kill the flow.
                    // The transform is responsible for its own error handling; if it escapes,
                    // log and drop rather than cancelling all concurrent work.
                    Logger.getLogger("FlowOps").errorf(e, "unorderedMapAsync transform failed")
                } finally {
                    semaphore.release()
                }
            }
        }
    }

/**
 * Collects upstream until [signal] completes, then cancels collection.
 */
fun <T> Flow<T>.takeUntilSignal(signal: Channel<Unit>): Flow<T> =
    channelFlow {
        val collectJob =
            launch {
                collect { send(it) }
            }
        coroutineScope {
            launch {
                signal.receive()
                collectJob.cancelAndJoin()
            }
        }
    }
