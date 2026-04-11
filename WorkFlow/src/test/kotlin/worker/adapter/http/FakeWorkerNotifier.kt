package com.workflow.worker.adapter.http

import com.workflow.worker.usecase.port.outbound.notification.WorkerNotifier
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeoutOrNull
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Test double for [WorkerNotifier] that provides deterministic control.
 *
 * - [awaitWork] suspends up to the requested [timeout], waking early if
 *   [wakeLocalWaiters] is called for the same queue. On timeout it returns
 *   [awaitReturns]; on wake it returns `true`.
 * - [signal], [onRemoteSignal], and [wakeLocalWaiters] count invocations
 *   for assertions.
 */
class FakeWorkerNotifier : WorkerNotifier {

    @Volatile
    var awaitReturns: Boolean = false

    private val _signalCount = AtomicInteger(0)
    val signalCount: Int get() = _signalCount.get()

    private val _signalledQueues = mutableListOf<String>()
    val signalledQueues: List<String> get() = synchronized(_signalledQueues) { _signalledQueues.toList() }

    private val _remoteSignalCount = AtomicInteger(0)
    val remoteSignalCount: Int get() = _remoteSignalCount.get()

    private val _wakeLocalCount = AtomicInteger(0)
    val wakeLocalCount: Int get() = _wakeLocalCount.get()

    private val _wakeLocalQueues = mutableListOf<String>()
    val wakeLocalQueues: List<String> get() = synchronized(_wakeLocalQueues) { _wakeLocalQueues.toList() }

    private val _awaitCallCount = AtomicInteger(0)
    val awaitCallCount: Int get() = _awaitCallCount.get()

    private val _awaitQueues = mutableListOf<String>()
    val awaitQueues: List<String> get() = synchronized(_awaitQueues) { _awaitQueues.toList() }

    private val _awaitTimeouts = mutableListOf<Duration>()
    val awaitTimeouts: List<Duration> get() = synchronized(_awaitTimeouts) { _awaitTimeouts.toList() }

    private val wakeFlows = ConcurrentHashMap<String, MutableSharedFlow<Unit>>()

    private fun wakeFlowFor(queue: String): MutableSharedFlow<Unit> =
        wakeFlows.computeIfAbsent(queue) {
            MutableSharedFlow(
                replay = 0,
                extraBufferCapacity = 1,
                onBufferOverflow = BufferOverflow.DROP_OLDEST,
            )
        }

    /**
     * Queues listed here will throw [RuntimeException] when signalled.
     * Callers use this to verify that signal failures are isolated and
     * do not prevent other queues from being notified.
     */
    @Volatile
    var failQueues: Set<String> = emptySet()

    override suspend fun signal(queueName: String) {
        _signalCount.incrementAndGet()
        synchronized(_signalledQueues) { _signalledQueues.add(queueName) }
        if (queueName in failQueues) throw RuntimeException("Simulated signal failure for queue: $queueName")
    }

    override fun onRemoteSignal(queueName: String) {
        _remoteSignalCount.incrementAndGet()
    }

    override fun wakeLocalWaiters(queueName: String) {
        _wakeLocalCount.incrementAndGet()
        synchronized(_wakeLocalQueues) { _wakeLocalQueues.add(queueName) }
        wakeFlowFor(queueName).tryEmit(Unit)
    }

    override suspend fun awaitWork(queueName: String, timeout: Duration): Boolean {
        _awaitCallCount.incrementAndGet()
        synchronized(_awaitQueues) { _awaitQueues.add(queueName) }
        synchronized(_awaitTimeouts) { _awaitTimeouts.add(timeout) }
        val woken = withTimeoutOrNull(timeout.toMillis()) {
            wakeFlowFor(queueName).first()
            true
        }
        return woken ?: awaitReturns
    }
}
