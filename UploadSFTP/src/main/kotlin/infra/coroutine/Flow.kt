package infra.coroutine

import com.river.core.chunked
import com.river.core.mapAsync
import io.nats.client.JetStreamSubscription
import io.nats.client.Message
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flattenMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.flow.transform
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Semaphore
import kotlin.time.Duration.Companion.seconds

fun <T> Flow<T>.takeUntilSignal(signal: Deferred<Unit>): Flow<T> =
    channelFlow {
        val sendJob =
            launch {
                collect { value -> send(value) }
            }

        signal.await()
        sendJob.cancelAndJoin()
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

fun <T> Flow<T>.pauseWhile(paused: StateFlow<Boolean>): Flow<T> =
    transform { v ->
        if (paused.value) paused.filter { !it }.first()
        emit(v)
    }
