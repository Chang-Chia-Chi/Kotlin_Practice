package infra.coroutine

import io.nats.client.JetStreamSubscription
import io.nats.client.Message
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore

fun <T> Flow<T>.takeUntilSignal(signal: CompletableDeferred<Unit>): Flow<T> =
    flow {
        try {
            coroutineScope {
                launch {
                    signal.await()
                    this@coroutineScope.cancel()
                }
                collect { emit(it) }
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

fun <T, R> Flow<T>.unorderedMapAsync(
    concurrency: Int = 1,
    transform: suspend (T) -> R,
) = channelFlow {
    val semaphore = Semaphore(concurrency)
    collect {
        launch {
            semaphore.acquire()
            try {
                val result = transform(it)
                send(result)
            } finally {
                semaphore.release()
            }
        }
    }
}

fun JetStreamSubscription.pullExpiresInAsFlow(
    batch: Int,
    durationMillis: Long,
): Flow<Message> =
    channelFlow {
        pullExpiresIn(batch, durationMillis)
        generateSequence { nextMessage(1) }
            .asFlow()
            .collect { send(it) }
    }
