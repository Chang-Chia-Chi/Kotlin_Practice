package jms

import jakarta.jms.ConnectionFactory
import jakarta.jms.ExceptionListener
import jakarta.jms.JMSException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.cancellation.CancellationException

class JmsListenerContextManager(
    private val connectionFactory: ConnectionFactory,
    private val config: JmsContextConfig,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob()),
) {
    private val isShutdown: AtomicBoolean = AtomicBoolean(false)
    private val isReconnecting: AtomicBoolean = AtomicBoolean(false)
    private var monitorJob: Job? = null

    private val _contextFlow = MutableStateFlow<JmsListenerContext?>(null)
    val contextFlow: StateFlow<JmsListenerContext?> = _contextFlow.asStateFlow()

    private val _contextStateFlow = MutableStateFlow(JmsContextState.NULL_CONNECTION)
    val contextStateFlow: StateFlow<JmsContextState> = _contextStateFlow.asStateFlow()

    init {
        monitorJob = scope.launch { monitorContextAndReconnect() }
    }

    suspend fun getContext(): JmsListenerContext = contextFlow.filterNotNull().first()

    private suspend fun awaitReconnectPermission() {}

    private suspend fun monitorContextAndReconnect() {
        contextStateFlow.collect { state ->
            when (state) {
                JmsContextState.NULL_CONNECTION,
                JmsContextState.CONNECTION_DIED,
                -> {
                    awaitReconnectPermission()
                    reconnect()
                }
                JmsContextState.DISCONNECTED,
                JmsContextState.CONNECTING,
                JmsContextState.CONNECTED,
                -> {}
            }
        }
    }

    suspend fun example() {
        val queueName = "example-queue"

        connectionFactory.consume(queueName).collect { message ->
            // Process the message here
            // Acknowledge the message after processing using coAcknowledge
            // Don't call acknowledge directly since it may perform blocking I/O
            message.coAcknowledge()
        }
    }

    private suspend fun reconnect() {
        if (isShutdown.get()) return
        if (!isReconnecting.compareAndSet(false, true)) return

        var listenerContext: JmsListenerContext? = null
        try {
            _contextStateFlow.value = JmsContextState.CONNECTING
            listenerContext =
                withContext(Dispatchers.IO) { createJmsListenerContext() }
                    .apply {
                        context.exceptionListener =
                            ExceptionListener { _ ->
                                clearConnection(this)
                                _contextStateFlow.value = JmsContextState.CONNECTION_DIED
                            }
                    }

            _contextFlow.value = listenerContext
            _contextStateFlow.value = JmsContextState.CONNECTED
        } catch (e: CancellationException) {
            clearConnection(listenerContext)
            _contextStateFlow.value = JmsContextState.DISCONNECTED
            throw e
        } catch (e: JMSException) {
            clearConnection(listenerContext)
            _contextStateFlow.value = JmsContextState.CONNECTION_DIED
        } finally {
            isReconnecting.set(false)
        }
    }

    private fun clearConnection(listenerContext: JmsListenerContext?) {
    }

    private suspend fun createJmsListenerContext(): JmsListenerContext {
        val context = withContext(Dispatchers.IO) { connectionFactory.createContextSafely() }
        val queue = withContext(Dispatchers.IO) { context.createQueue(config.queueName) }
        val consumer = withContext(Dispatchers.IO) { context.createConsumer(queue) }
    }
}
