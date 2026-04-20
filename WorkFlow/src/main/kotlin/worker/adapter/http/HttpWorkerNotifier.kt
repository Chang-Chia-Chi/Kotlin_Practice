package com.workflow.worker.adapter.http

import com.workflow.worker.usecase.port.outbound.notification.WorkerNotifier
import com.workflow.worker.usecase.port.outbound.peer.PeerDiscovery
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import jakarta.annotation.PreDestroy
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.net.URLEncoder
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

@ApplicationScoped
class HttpWorkerNotifier(
    private val peerDiscovery: PeerDiscovery,
    private val httpClient: HttpClient,
) : WorkerNotifier {
    private val log = LoggerFactory.getLogger(HttpWorkerNotifier::class.java)

    private val isShutdown = AtomicBoolean(false)
    private val broadcastScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    private val localFlows = ConcurrentHashMap<String, MutableSharedFlow<Unit>>()
    private val broadcastFlows = ConcurrentHashMap<String, MutableSharedFlow<Unit>>()

    private fun localFlowFor(queue: String): MutableSharedFlow<Unit> =
        localFlows.computeIfAbsent(queue) {
            MutableSharedFlow(
                replay = 0,
                extraBufferCapacity = 1,
                onBufferOverflow = BufferOverflow.DROP_OLDEST,
            )
        }

    private fun broadcastFlowFor(queue: String): MutableSharedFlow<Unit> =
        broadcastFlows.computeIfAbsent(queue) { q ->
            MutableSharedFlow<Unit>(
                replay = 1,
                extraBufferCapacity = 0,
                onBufferOverflow = BufferOverflow.DROP_OLDEST,
            ).also { flow -> launchBroadcastCollector(q, flow) }
        }

    private fun launchBroadcastCollector(queue: String, flow: MutableSharedFlow<Unit>) {
        try {
            broadcastScope.launch {
                val encodedQueue = URLEncoder.encode(queue, Charsets.UTF_8)
                flow.collect {
                    val peers = peerDiscovery.peers()
                    for (peer in peers) {
                        launch {
                            try {
                                httpClient.post("http://$peer:8080/internal/dispatch-notify?queue=$encodedQueue")
                            } catch (e: Exception) {
                                log.debug("Peer notify failed for {}: {}", peer, e.message)
                            }
                        }
                    }
                }
            }
        } catch (_: IllegalStateException) {
            // broadcastScope already cancelled during shutdown — benign race
        }
    }

    override suspend fun signal(queueName: String) {
        localFlowFor(queueName).tryEmit(Unit)
        if (!isShutdown.get()) {
            broadcastFlowFor(queueName).tryEmit(Unit)
        }
    }

    override fun onRemoteSignal(queueName: String) {
        localFlowFor(queueName).tryEmit(Unit)
    }

    override fun wakeLocalWaiters(queueName: String) {
        localFlowFor(queueName).tryEmit(Unit)
    }

    override suspend fun awaitWork(
        queueName: String,
        timeout: Duration,
    ): Boolean =
        withTimeoutOrNull(timeout.toMillis()) {
            localFlowFor(queueName).first()
        } != null

    @PreDestroy
    fun shutdown() {
        isShutdown.set(true)
        broadcastScope.cancel()
    }
}
