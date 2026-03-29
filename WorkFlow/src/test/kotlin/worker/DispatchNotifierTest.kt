package com.workflow.worker

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class DispatchNotifierTest {

    private lateinit var peerRegistry: PeerRegistry
    private lateinit var notifier: DispatchNotifierImpl

    @BeforeEach
    fun setup() {
        peerRegistry = mock()
        whenever(peerRegistry.peers()).thenReturn(emptyList())
        notifier = DispatchNotifierImpl(peerRegistry, HttpClient(MockEngine { respond("") }))
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
            whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.2"))
            val engine = MockEngine { respond("") }
            val notifier = DispatchNotifierImpl(peerRegistry, HttpClient(engine))

            notifier.onRemoteSignal("default")

            assertTrue(engine.requestHistory.isEmpty(), "onRemoteSignal should not make HTTP calls")
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

        @Test
        fun `signal with peers calls HTTP post once per peer`() = runTest {
            whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.2", "10.0.0.3"))
            val engine = MockEngine { respond("") }
            val notifier = DispatchNotifierImpl(peerRegistry, HttpClient(engine))

            notifier.signal("default")

            assertEquals(2, engine.requestHistory.size)
            val hosts = engine.requestHistory.map { it.url.host }.toSet()
            assertEquals(setOf("10.0.0.2", "10.0.0.3"), hosts)
            engine.requestHistory.forEach { req ->
                assertEquals(HttpMethod.Post, req.method)
                assertEquals(8080, req.url.port)
                assertEquals("/internal/dispatch-notify", req.url.encodedPath)
                assertEquals("default", req.url.parameters["queue"])
            }
        }

        @Test
        fun `signal with no peers does not make HTTP calls`() = runTest {
            whenever(peerRegistry.peers()).thenReturn(emptyList())
            val engine = MockEngine { respond("") }
            val notifier = DispatchNotifierImpl(peerRegistry, HttpClient(engine))

            notifier.signal("default")

            assertTrue(engine.requestHistory.isEmpty())
        }

        @Test
        fun `signal propagates queue name in HTTP path`() = runTest {
            whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.5"))
            val engine = MockEngine { respond("") }
            val notifier = DispatchNotifierImpl(peerRegistry, HttpClient(engine))

            notifier.signal("priority-queue")

            assertEquals(1, engine.requestHistory.size)
            assertEquals("priority-queue", engine.requestHistory[0].url.parameters["queue"])
        }

        @Test
        fun `signal URL-encodes queue name with special characters`() = runTest {
            whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.7"))
            val engine = MockEngine { respond("") }
            val notifier = DispatchNotifierImpl(peerRegistry, HttpClient(engine))

            notifier.signal("queue with spaces")

            assertEquals(1, engine.requestHistory.size)
            assertEquals("queue with spaces", engine.requestHistory[0].url.parameters["queue"])
        }

        @Test
        fun `signal with HTTP failure does not throw`() = runTest {
            whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.9"))
            val engine = MockEngine { respond("", HttpStatusCode.InternalServerError) }
            val notifier = DispatchNotifierImpl(peerRegistry, HttpClient(engine))

            notifier.signal("default") // should not throw
        }
    }
}
