package com.workflow.worker

import io.vertx.core.Future
import io.vertx.core.buffer.Buffer
import io.vertx.ext.web.client.HttpRequest
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class DispatchNotifierTest {

    private lateinit var peerRegistry: PeerRegistry
    private lateinit var webClient: WebClient
    private lateinit var notifier: DispatchNotifierImpl

    @BeforeEach
    fun setup() {
        peerRegistry = mock()
        webClient = mock()
        whenever(peerRegistry.peers()).thenReturn(emptyList())
        notifier = DispatchNotifierImpl(peerRegistry, webClient)
    }

    // ── A. signal() wakes awaitWork() ────────────────────────────────────

    @Nested
    inner class SignalWakesSingleWaiter {

        @Test
        fun `signal wakes single suspended awaitWork`() = runTest(UnconfinedTestDispatcher()) {
            val result = async {
                notifier.awaitWork("default", Duration.ofSeconds(10))
            }
            yield() // let the collector suspend on flow.first()

            notifier.signal("default")

            assertTrue(result.await(), "awaitWork should return true when signaled")
        }

        @Test
        fun `signal wakes multiple concurrent awaitWork coroutines`() = runTest(UnconfinedTestDispatcher()) {
            val result1 = async {
                notifier.awaitWork("default", Duration.ofSeconds(10))
            }
            val result2 = async {
                notifier.awaitWork("default", Duration.ofSeconds(10))
            }
            // Both coroutines are already suspended at flow.first() due to
            // UnconfinedTestDispatcher's eager dispatch. Do NOT advanceUntilIdle()
            // here — that would advance the 10s timeout.

            notifier.signal("default")

            assertTrue(result1.await(), "First waiter should be woken")
            assertTrue(result2.await(), "Second waiter should be woken")
        }
    }

    // ── B. awaitWork() returns false on timeout ──────────────────────────

    @Nested
    inner class AwaitWorkTimeout {

        @Test
        fun `awaitWork returns false on timeout when no signal sent`() = runTest {
            val result = notifier.awaitWork("default", Duration.ofSeconds(5))

            assertFalse(result, "awaitWork should return false on timeout")
        }
    }

    // ── C. Multi-queue isolation ─────────────────────────────────────────

    @Nested
    inner class MultiQueueIsolation {

        @Test
        fun `signal on queue a does not wake waiter on queue b`() = runTest(UnconfinedTestDispatcher()) {
            val result = async {
                notifier.awaitWork("b", Duration.ofSeconds(1))
            }
            yield()

            notifier.signal("a")

            assertFalse(result.await(), "Signal on 'a' should not wake waiter on 'b'")
        }

        @Test
        fun `signal on correct queue wakes only that queue`() = runTest(UnconfinedTestDispatcher()) {
            val resultA = async {
                notifier.awaitWork("a", Duration.ofSeconds(10))
            }
            yield()

            notifier.signal("a")

            assertTrue(resultA.await(), "Signal on 'a' should wake waiter on 'a'")
        }
    }

    // ── D. onRemoteSignal wakes local only (no HTTP broadcast) ───────────

    @Nested
    inner class OnRemoteSignal {

        @Test
        fun `onRemoteSignal wakes local waiter`() = runTest(UnconfinedTestDispatcher()) {
            val result = async {
                notifier.awaitWork("default", Duration.ofSeconds(10))
            }
            yield()

            notifier.onRemoteSignal("default")

            assertTrue(result.await(), "onRemoteSignal should wake local waiter")
        }

        @Test
        fun `onRemoteSignal does not broadcast via HTTP`() = runTest {
            notifier.onRemoteSignal("default")
            advanceUntilIdle()

            verify(webClient, never()).post(any<Int>(), any<String>(), any<String>())
        }
    }

    // ── E. Signal coalescing ─────────────────────────────────────────────

    @Nested
    inner class SignalCoalescing {

        @Test
        fun `rapid signals coalesce into single wake-up`() = runTest(UnconfinedTestDispatcher()) {
            var wakeCount = 0

            val waiter = launch {
                if (notifier.awaitWork("default", Duration.ofSeconds(10))) {
                    wakeCount++
                }
            }
            yield()

            repeat(100) { notifier.signal("default") }

            waiter.join()
            assertTrue(wakeCount == 1, "100 rapid signals should result in 1 coroutine resumption, got $wakeCount")
        }
    }

    // ── F. signal() with peers triggers HTTP broadcast ───────────────────

    @Nested
    inner class SignalWithPeers {

        @Suppress("UNCHECKED_CAST")
        private fun mockWebClientPost(peer: String, queue: String): HttpRequest<Buffer> {
            val request = mock<HttpRequest<Buffer>>()
            val response = mock<HttpResponse<Buffer>>()
            whenever(request.send()).thenReturn(Future.succeededFuture(response))
            whenever(webClient.post(eq(8080), eq(peer), eq("/internal/dispatch-notify?queue=$queue")))
                .thenReturn(request)
            return request
        }

        @Test
        fun `signal with peers calls webClient post once per peer`() = runTest {
            whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.2", "10.0.0.3"))
            val req1 = mockWebClientPost("10.0.0.2", "default")
            val req2 = mockWebClientPost("10.0.0.3", "default")

            notifier.signal("default")

            verify(req1).send()
            verify(req2).send()
        }

        @Test
        fun `signal with no peers does not call webClient`() = runTest {
            whenever(peerRegistry.peers()).thenReturn(emptyList())

            notifier.signal("default")

            verify(webClient, never()).post(any<Int>(), any<String>(), any<String>())
        }

        @Test
        fun `signal propagates queue name in HTTP path`() = runTest {
            whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.5"))
            mockWebClientPost("10.0.0.5", "priority-queue")

            notifier.signal("priority-queue")

            verify(webClient).post(eq(8080), eq("10.0.0.5"), eq("/internal/dispatch-notify?queue=priority-queue"))
        }

        @Test
        fun `signal URL-encodes queue name with special characters`() = runTest {
            whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.7"))
            mockWebClientPost("10.0.0.7", "queue+with+spaces")

            notifier.signal("queue with spaces")

            verify(webClient).post(eq(8080), eq("10.0.0.7"), eq("/internal/dispatch-notify?queue=queue+with+spaces"))
        }

        @Test
        fun `signal with HTTP failure does not throw`() = runTest {
            whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.9"))
            val request = mock<HttpRequest<Buffer>>()
            whenever(request.send()).thenReturn(Future.failedFuture(RuntimeException("timeout")))
            whenever(webClient.post(eq(8080), eq("10.0.0.9"), eq("/internal/dispatch-notify?queue=default")))
                .thenReturn(request)

            notifier.signal("default")

            verify(request).send()
        }
    }
}
