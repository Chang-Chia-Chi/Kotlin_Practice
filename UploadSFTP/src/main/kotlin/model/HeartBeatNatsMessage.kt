package model

import io.nats.client.Message
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.atomic.AtomicBoolean

class HeartBeatNatsMessage(
    val delegate: Message,
    private val heartBeatInterval: Long = 5000,
    private val heartBeatTimeout: Long = 120000,
) {
    private val start = AtomicBoolean(false)
    private val stop = AtomicBoolean(false)
    private val mutex = Mutex()

    suspend fun ack() {
        stop.compareAndSet(false, true)
        mutex.withLock { delegate.ack() }
    }

    suspend fun nak() {
        stop.compareAndSet(false, true)
        mutex.withLock { delegate.nak() }
    }

    suspend fun heartBeat() =
        withTimeoutOrNull(heartBeatTimeout) {
            if (!start.compareAndSet(true, false)) return@withTimeoutOrNull
            while (!stop.get()) {
                mutex.withLock {
                    if (stop.get()) return@withTimeoutOrNull
                    delegate.inProgress()
                }
                delay(heartBeatInterval)
            }
        }
}
