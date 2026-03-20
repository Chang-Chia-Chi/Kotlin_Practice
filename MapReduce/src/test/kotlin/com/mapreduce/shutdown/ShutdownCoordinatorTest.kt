package com.mapreduce.shutdown

import com.mapreduce.config.FrameworkConfig
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.quarkus.runtime.ShutdownEvent
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

class ShutdownCoordinatorTest {

    private lateinit var meterRegistry: MeterRegistry
    private lateinit var config: FrameworkConfig
    private lateinit var coordinator: ShutdownCoordinator

    private fun participantInstance(vararg participants: ShutdownParticipant): Instance<ShutdownParticipant> {
        val instance = mock<Instance<ShutdownParticipant>>()
        whenever(instance.iterator()).thenAnswer { participants.toList().iterator() }
        return instance
    }

    private fun participant(
        order: Int = 0,
        timeout: Duration = Duration.ofMillis(500),
        action: suspend () -> Unit = {},
    ): ShutdownParticipant = object : ShutdownParticipant {
        override val shutdownOrder = order
        override val shutdownTimeout = timeout
        override suspend fun shutdown() = action()
    }

    @BeforeEach
    fun setUp() {
        meterRegistry = SimpleMeterRegistry()
        config = mock()
        val shutdownConfig = mock<FrameworkConfig.ShutdownConfig>()
        whenever(config.shutdown()).thenReturn(shutdownConfig)
        whenever(shutdownConfig.globalTimeout()).thenReturn(Duration.ofSeconds(90))

        coordinator = ShutdownCoordinator(
            participantInstance(), meterRegistry, config,
        )
    }

    // ── Initial state ─────────────────────────────────────────────

    @Nested
    inner class InitialState {

        @Test
        fun `initial state is RUNNING`() {
            assertEquals(ShutdownState.RUNNING, coordinator.state)
        }

        @Test
        fun `isShuttingDown is false initially`() {
            assertFalse(coordinator.isShuttingDown)
        }

    }

    // ── State transitions ─────────────────────────────────────────

    @Nested
    inner class StateTransitions {

        @Test
        fun `onShutdown transitions through DRAINING to TERMINATED`() {
            coordinator.onShutdown(ShutdownEvent())

            assertEquals(ShutdownState.TERMINATED, coordinator.state)
            assertTrue(coordinator.isShuttingDown)
        }
    }

    // ── Participant orchestration ──────────────────────────────────

    @Nested
    inner class ParticipantOrchestration {

        @Test
        fun `calls all participants during shutdown`() {
            val called1 = AtomicBoolean(false)
            val called2 = AtomicBoolean(false)
            val p1 = participant { called1.set(true) }
            val p2 = participant { called2.set(true) }

            coordinator = ShutdownCoordinator(
                participantInstance(p1, p2), meterRegistry, config,
            )
            coordinator.onShutdown(ShutdownEvent())

            assertTrue(called1.get())
            assertTrue(called2.get())
        }

        @Test
        fun `calls participants in ascending order`() {
            val callOrder = mutableListOf<Int>()
            val p0 = participant(order = 0) { callOrder.add(0) }
            val p1 = participant(order = 1) { callOrder.add(1) }
            val p2 = participant(order = 2) { callOrder.add(2) }

            // Add out of order to verify sorting
            coordinator = ShutdownCoordinator(
                participantInstance(p2, p0, p1), meterRegistry, config,
            )
            coordinator.onShutdown(ShutdownEvent())

            assertEquals(listOf(0, 1, 2), callOrder)
        }

        @Test
        fun `runs same-order participants concurrently`() {
            // Uses CompletableDeferred (suspending) instead of CountDownLatch (blocking)
            // to avoid starving the single-threaded runBlocking dispatcher.
            // Deadlocks if participants are run sequentially to completion.
            val p1Started = CompletableDeferred<Unit>()
            val p2Started = CompletableDeferred<Unit>()
            val p1 = participant(order = 0) {
                p1Started.complete(Unit)
                p2Started.await()
            }
            val p2 = participant(order = 0) {
                p2Started.complete(Unit)
                p1Started.await()
            }

            coordinator = ShutdownCoordinator(
                participantInstance(p1, p2), meterRegistry, config,
            )
            coordinator.onShutdown(ShutdownEvent())
        }

        @Test
        fun `completes group before starting next`() {
            val order0Done = AtomicBoolean(false)
            val p0 = participant(order = 0) {
                delay(50)
                order0Done.set(true)
            }
            val p1 = participant(order = 1) {
                assertTrue(order0Done.get(), "Order 0 should have completed before order 1 starts")
            }

            coordinator = ShutdownCoordinator(
                participantInstance(p1, p0), meterRegistry, config,
            )
            coordinator.onShutdown(ShutdownEvent())
        }
    }

    // ── Timeout & error handling ───────────────────────────────────

    @Nested
    inner class TimeoutAndErrorHandling {

        @Test
        fun `enforces participant timeout without blocking shutdown`() {
            val p = participant(order = 0, timeout = Duration.ofMillis(50)) {
                delay(10_000)
            }

            coordinator = ShutdownCoordinator(
                participantInstance(p), meterRegistry, config,
            )
            val start = System.currentTimeMillis()
            coordinator.onShutdown(ShutdownEvent())
            val elapsed = System.currentTimeMillis() - start

            assertTrue(elapsed < 5000, "Shutdown should not wait for slow participant (took ${elapsed}ms)")
            assertEquals(ShutdownState.TERMINATED, coordinator.state)
        }

        @Test
        fun `participant exception does not prevent other participants in same group`() {
            val completed = AtomicBoolean(false)
            val p1 = participant(order = 0) { throw RuntimeException("boom") }
            val p2 = participant(order = 0) { completed.set(true) }

            coordinator = ShutdownCoordinator(
                participantInstance(p1, p2), meterRegistry, config,
            )
            coordinator.onShutdown(ShutdownEvent())

            assertTrue(completed.get())
            assertEquals(ShutdownState.TERMINATED, coordinator.state)
        }

        @Test
        fun `participant exception does not prevent next group`() {
            val completed = AtomicBoolean(false)
            val p0 = participant(order = 0) { throw RuntimeException("boom") }
            val p1 = participant(order = 1) { completed.set(true) }

            coordinator = ShutdownCoordinator(
                participantInstance(p0, p1), meterRegistry, config,
            )
            coordinator.onShutdown(ShutdownEvent())

            assertTrue(completed.get())
        }

        @Test
        fun `timed out participant does not prevent next group`() {
            val completed = AtomicBoolean(false)
            val p0 = participant(order = 0, timeout = Duration.ofMillis(50)) {
                delay(10_000)
            }
            val p1 = participant(order = 1) { completed.set(true) }

            coordinator = ShutdownCoordinator(
                participantInstance(p0, p1), meterRegistry, config,
            )
            coordinator.onShutdown(ShutdownEvent())

            assertTrue(completed.get())
            assertEquals(ShutdownState.TERMINATED, coordinator.state)
        }
    }

    // ── Metrics ────────────────────────────────────────────────────

    @Nested
    inner class Metrics {

        @Test
        fun `records shutdown duration metric`() {
            coordinator.onShutdown(ShutdownEvent())

            val timer = meterRegistry.find("taskqueue_shutdown_duration_seconds").timer()
            assertNotNull(timer)
            assertTrue(timer!!.count() > 0)
        }

        @Test
        fun `exposes shutdown state gauge`() {
            val gauge = meterRegistry.find("taskqueue_shutdown_state").gauge()
            assertNotNull(gauge)
            assertEquals(ShutdownState.RUNNING.ordinal.toDouble(), gauge!!.value())

            coordinator.onShutdown(ShutdownEvent())

            assertEquals(ShutdownState.TERMINATED.ordinal.toDouble(), gauge.value())
        }
    }

    // ── Edge cases ────────────────────────────────────────────────

    @Test
    fun `onShutdown succeeds with no participants`() {
        coordinator.onShutdown(ShutdownEvent())

        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }
}
