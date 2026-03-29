package com.workflow.worker

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

/**
 * Notification layer for event-driven task dispatch.
 *
 * Workers suspend on a per-queue [MutableSharedFlow] and wake instantly
 * when signaled. Three signal sources:
 * - Local: [signal] emits to the in-process flow and broadcasts to peers via HTTP.
 * - Remote: [onRemoteSignal] emits to the in-process flow only (no re-broadcast).
 * - Fallback: [awaitWork] times out after [Duration], triggering a poll probe.
 *
 * Correctness invariant: notifications are performance hints, never
 * correctness requirements. Removing the entire notification layer
 * degrades performance to fallback-poll mode (5s) but never affects
 * task claiming via SELECT FOR UPDATE SKIP LOCKED.
 */
interface DispatchNotifier {
    /**
     * Signal that new work is available on [queueName].
     * Wakes local workers immediately and broadcasts to all peer pods
     * concurrently via HTTP POST within a [supervisorScope]. Awaits
     * all peer notifications; individual failures are logged and do
     * not cancel siblings. Called AFTER transaction commit.
     */
    suspend fun signal(queueName: String)

    /**
     * Handle a remote signal received via the internal HTTP endpoint.
     * Wakes local workers only -- does NOT re-broadcast to avoid loops.
     */
    fun onRemoteSignal(queueName: String)

    /**
     * Suspend until work is signaled on [queueName] or [timeout] expires.
     * Returns true if woken by a signal, false on timeout.
     */
    suspend fun awaitWork(
        queueName: String,
        timeout: Duration,
    ): Boolean
}

@ApplicationScoped
class DispatchNotifierImpl(
    private val peerRegistry: PeerRegistry,
    private val httpClient: HttpClient,
) : DispatchNotifier {
    private val log = LoggerFactory.getLogger(DispatchNotifierImpl::class.java)

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
        val peers = peerRegistry.peers()
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
