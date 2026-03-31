package com.workflow.worker.adapter.http

import com.workflow.worker.usecase.port.outbound.notification.DispatchNotifier
import kotlinx.coroutines.delay
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

/**
 * Test double for [DispatchNotifier] that provides deterministic control.
 *
 * - [awaitWork] suspends for the requested [timeout] duration (simulating the
 *   real notifier's fallback poll interval), then returns [awaitReturns].
 *   This ensures the poll loop yields to the test scheduler between iterations.
 * - [signal] and [onRemoteSignal] count invocations for assertions.
 */
class FakeDispatchNotifier : DispatchNotifier {

    @Volatile
    var awaitReturns: Boolean = false

    private val _signalCount = AtomicInteger(0)
    val signalCount: Int get() = _signalCount.get()

    private val _signalledQueues = mutableListOf<String>()
    val signalledQueues: List<String> get() = synchronized(_signalledQueues) { _signalledQueues.toList() }

    private val _remoteSignalCount = AtomicInteger(0)
    val remoteSignalCount: Int get() = _remoteSignalCount.get()

    private val _awaitCallCount = AtomicInteger(0)
    val awaitCallCount: Int get() = _awaitCallCount.get()

    private val _awaitQueues = mutableListOf<String>()
    val awaitQueues: List<String> get() = synchronized(_awaitQueues) { _awaitQueues.toList() }

    private val _awaitTimeouts = mutableListOf<Duration>()
    val awaitTimeouts: List<Duration> get() = synchronized(_awaitTimeouts) { _awaitTimeouts.toList() }

    override suspend fun signal(queueName: String) {
        _signalCount.incrementAndGet()
        synchronized(_signalledQueues) { _signalledQueues.add(queueName) }
    }

    override fun onRemoteSignal(queueName: String) {
        _remoteSignalCount.incrementAndGet()
    }

    override suspend fun awaitWork(queueName: String, timeout: Duration): Boolean {
        _awaitCallCount.incrementAndGet()
        synchronized(_awaitQueues) { _awaitQueues.add(queueName) }
        synchronized(_awaitTimeouts) { _awaitTimeouts.add(timeout) }
        delay(timeout.toMillis())
        return awaitReturns
    }
}
