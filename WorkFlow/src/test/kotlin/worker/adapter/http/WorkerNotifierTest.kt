package com.workflow.worker.adapter.http

import com.workflow.worker.usecase.port.outbound.peer.PeerDiscovery
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
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
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
class WorkerNotifierTest {

    private lateinit var peerDiscovery: PeerDiscovery
    private lateinit var notifier: HttpWorkerNotifier

    @BeforeEach
    fun setup() {
        peerDiscovery = mock()
        whenever(peerDiscovery.peers()).thenReturn(emptyList())
        notifier = HttpWorkerNotifier(peerDiscovery, HttpClient(MockEngine { respond("") }))
    }

    @AfterEach
    fun teardown() {
        notifier.shutdown()
    }

    // ── A. signal() wakes awaitWork() ────────────────────────────────────

    @Nested
    inner class SignalWakesSingleWaiter {

        @Test
        fun `signal wakes single suspended awaitWork`() = runTest(UnconfinedTestDispatcher()) {
            val result = async {
                notifier.awaitWork("default", Duration.ofSeconds(10))
            }
            yield()

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
        fun `onRemoteSignal does not broadcast via HTTP`() = runTest(UnconfinedTestDispatcher()) {
            whenever(peerDiscovery.peers()).thenReturn(listOf("10.0.0.2"))
            val engine = MockEngine { respond("") }
            val localNotifier = HttpWorkerNotifier(peerDiscovery, HttpClient(engine))

            // First prove the broadcast collector IS active by calling signal()
            localNotifier.signal("default")
            await atMost Duration.ofSeconds(2) untilAsserted {
                assertTrue(engine.requestHistory.size > 0, "signal() should have triggered HTTP broadcast")
            }
            val countAfterSignal = engine.requestHistory.size

            // Now call onRemoteSignal and verify NO additional HTTP calls
            localNotifier.onRemoteSignal("default")

            await.during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(1)).untilAsserted {
                assertEquals(
                    countAfterSignal, engine.requestHistory.size,
                    "onRemoteSignal should not trigger additional HTTP calls"
                )
            }
            localNotifier.shutdown()
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
        fun `signal with peers calls HTTP post once per peer`() = runTest(UnconfinedTestDispatcher()) {
            whenever(peerDiscovery.peers()).thenReturn(listOf("10.0.0.2", "10.0.0.3"))
            val engine = MockEngine { respond("") }
            val localNotifier = HttpWorkerNotifier(peerDiscovery, HttpClient(engine))

            localNotifier.signal("default")

            await atMost Duration.ofSeconds(2) untilAsserted {
                assertEquals(2, engine.requestHistory.size)
            }
            val hosts = engine.requestHistory.map { it.url.host }.toSet()
            assertEquals(setOf("10.0.0.2", "10.0.0.3"), hosts)
            engine.requestHistory.forEach { req ->
                assertEquals(HttpMethod.Post, req.method)
                assertEquals(8080, req.url.port)
                assertEquals("/internal/dispatch-notify", req.url.encodedPath)
                assertEquals("default", req.url.parameters["queue"])
            }
            localNotifier.shutdown()
        }

        @Test
        fun `signal with no peers does not make HTTP calls`() = runTest(UnconfinedTestDispatcher()) {
            whenever(peerDiscovery.peers()).thenReturn(emptyList())
            val engine = MockEngine { respond("") }
            val localNotifier = HttpWorkerNotifier(peerDiscovery, HttpClient(engine))

            localNotifier.signal("default")

            await.during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(1)).untilAsserted {
                assertTrue(engine.requestHistory.isEmpty())
            }
            localNotifier.shutdown()
        }

        @Test
        fun `signal propagates queue name in HTTP path`() = runTest(UnconfinedTestDispatcher()) {
            whenever(peerDiscovery.peers()).thenReturn(listOf("10.0.0.5"))
            val engine = MockEngine { respond("") }
            val localNotifier = HttpWorkerNotifier(peerDiscovery, HttpClient(engine))

            localNotifier.signal("priority-queue")

            await atMost Duration.ofSeconds(2) untilAsserted {
                assertEquals(1, engine.requestHistory.size)
            }
            assertEquals("priority-queue", engine.requestHistory[0].url.parameters["queue"])
            localNotifier.shutdown()
        }

        @Test
        fun `signal URL-encodes queue name with special characters`() = runTest(UnconfinedTestDispatcher()) {
            whenever(peerDiscovery.peers()).thenReturn(listOf("10.0.0.7"))
            val engine = MockEngine { respond("") }
            val localNotifier = HttpWorkerNotifier(peerDiscovery, HttpClient(engine))

            localNotifier.signal("queue with spaces")

            await atMost Duration.ofSeconds(2) untilAsserted {
                assertEquals(1, engine.requestHistory.size)
            }
            assertEquals("queue with spaces", engine.requestHistory[0].url.parameters["queue"])
            localNotifier.shutdown()
        }

        @Test
        fun `signal with HTTP failure does not throw`() = runTest(UnconfinedTestDispatcher()) {
            whenever(peerDiscovery.peers()).thenReturn(listOf("10.0.0.9"))
            val engine = MockEngine { respond("", HttpStatusCode.InternalServerError) }
            val localNotifier = HttpWorkerNotifier(peerDiscovery, HttpClient(engine))

            localNotifier.signal("default")

            // Verify the broadcast executed — proving the 500 was handled gracefully
            await atMost Duration.ofSeconds(2) untilAsserted {
                assertEquals(1, engine.requestHistory.size, "Broadcast should have executed despite 500")
            }
            localNotifier.shutdown()
        }
    }

    // ── G. Fire-and-forget: signal returns before HTTP executes ─────────

    @Nested
    inner class FireAndForget {

        @Test
        fun `signal returns before HTTP executes`() = runTest {
            whenever(peerDiscovery.peers()).thenReturn(listOf("10.0.0.2"))
            val engine = MockEngine { respond("") }
            val localNotifier = HttpWorkerNotifier(peerDiscovery, HttpClient(engine))

            localNotifier.signal("default")

            assertEquals(0, engine.requestHistory.size, "signal() should return before HTTP executes")
            localNotifier.shutdown()
        }
    }

    // ── H. Broadcast coalescing: rapid signals collapse into fewer HTTP calls

    @Nested
    inner class BroadcastCoalescing {

        @Test
        fun `rapid signals coalesce into fewer HTTP broadcasts`() = runTest(UnconfinedTestDispatcher()) {
            whenever(peerDiscovery.peers()).thenReturn(listOf("10.0.0.2"))
            val engine = MockEngine { respond("") }
            val localNotifier = HttpWorkerNotifier(peerDiscovery, HttpClient(engine))

            repeat(10) { localNotifier.signal("default") }

            await atMost Duration.ofSeconds(2) untilAsserted {
                assertTrue(engine.requestHistory.size > 0, "At least one broadcast should have executed")
            }
            await.during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(1)).untilAsserted {
                assertTrue(
                    engine.requestHistory.size <= 2,
                    "10 rapid signals should coalesce, but got ${engine.requestHistory.size} HTTP requests"
                )
            }
            localNotifier.shutdown()
        }
    }

    // ── I. Shutdown behavior ────────────────────────────────────────────

    @Nested
    inner class ShutdownBehavior {

        @Test
        fun `shutdown cancels broadcast collectors`() = runTest(UnconfinedTestDispatcher()) {
            whenever(peerDiscovery.peers()).thenReturn(listOf("10.0.0.2"))
            val engine = MockEngine { respond("") }
            val localNotifier = HttpWorkerNotifier(peerDiscovery, HttpClient(engine))

            localNotifier.signal("default")
            await atMost Duration.ofSeconds(2) untilAsserted {
                assertTrue(engine.requestHistory.size > 0, "Broadcast should have executed before shutdown")
            }
            val countBefore = engine.requestHistory.size

            localNotifier.shutdown()

            localNotifier.signal("default")

            await.during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(1)).untilAsserted {
                assertEquals(countBefore, engine.requestHistory.size, "No HTTP calls after shutdown")
            }
        }

        @Test
        fun `post-shutdown signal on new queue does not throw`() = runTest(UnconfinedTestDispatcher()) {
            whenever(peerDiscovery.peers()).thenReturn(listOf("10.0.0.2"))
            val engine = MockEngine { respond("") }
            val localNotifier = HttpWorkerNotifier(peerDiscovery, HttpClient(engine))

            localNotifier.shutdown()

            localNotifier.signal("never-seen-queue")

            await.during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(1)).untilAsserted {
                assertEquals(0, engine.requestHistory.size, "No HTTP after shutdown on new queue")
            }
        }

        @Test
        fun `awaitWork still returns false on timeout after shutdown`() = runTest {
            val localNotifier = HttpWorkerNotifier(peerDiscovery, HttpClient(MockEngine { respond("") }))

            localNotifier.shutdown()

            val result = localNotifier.awaitWork("default", Duration.ofSeconds(1))

            assertFalse(result, "awaitWork should return false on timeout even after shutdown")
        }
    }
}
