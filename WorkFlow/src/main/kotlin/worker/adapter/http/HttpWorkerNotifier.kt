package com.workflow.worker.adapter.http

import com.workflow.worker.usecase.port.outbound.notification.WorkerNotifier
import com.workflow.worker.usecase.port.outbound.peer.PeerDiscovery
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.net.URLEncoder
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class HttpWorkerNotifier(
    private val peerDiscovery: PeerDiscovery,
    private val httpClient: HttpClient,
) : WorkerNotifier {
    private val log = LoggerFactory.getLogger(HttpWorkerNotifier::class.java)

    private val flows = ConcurrentHashMap<String, MutableSharedFlow<Unit>>()

    private fun flowFor(queue: String) =
        flows.getOrPut(queue) {
            MutableSharedFlow(
                replay = 0,
                extraBufferCapacity = 1,
                onBufferOverflow = BufferOverflow.DROP_OLDEST,
            )
        }

    override suspend fun signal(queueName: String) {
        flowFor(queueName).tryEmit(Unit)
        val peers = peerDiscovery.peers()
        if (peers.isEmpty()) return
        val encodedQueue = URLEncoder.encode(queueName, Charsets.UTF_8)
        supervisorScope {
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

    override fun onRemoteSignal(queueName: String) {
        flowFor(queueName).tryEmit(Unit)
    }

    override suspend fun awaitWork(
        queueName: String,
        timeout: Duration,
    ): Boolean =
        withTimeoutOrNull(timeout.toMillis()) {
            flowFor(queueName).first()
        } != null
}
