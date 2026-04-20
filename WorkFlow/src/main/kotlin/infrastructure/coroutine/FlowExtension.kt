package com.workflow.infrastructure.coroutine

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore

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
        val supervisor = SupervisorJob(coroutineContext[Job])
        val handler = CoroutineExceptionHandler { _, e -> close(e) }
        collect { value ->
            semaphore.acquire() // back-pressure: suspends when all slots full
            launch(supervisor + handler) {
                try {
                    send(transform(value))
                } finally {
                    semaphore.release()
                }
            }
        }
        supervisor.complete()
    }

/**
 * Collects upstream until [signal] completes, then cancels collection.
 */
fun <T> Flow<T>.takeUntilSignal(signal: Channel<Unit>): Flow<T> =
    channelFlow {
        val signalJob = launch {
            signal.receive()
            close()
        }
        try {
            collect { send(it) }
        } catch (_: ClosedSendChannelException) {
            // Normal: signalJob closed the channel while upstream was emitting
        }
        signalJob.cancel()
    }
