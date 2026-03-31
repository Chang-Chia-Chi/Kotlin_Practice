package com.workflow.infrastructure.shutdown

import com.workflow.infrastructure.shutdown.ShutdownConfig
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.quarkus.runtime.ShutdownEvent
import jakarta.enterprise.inject.Instance
import java.time.Duration
import kotlinx.coroutines.CompletableDeferred
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class ShutdownCoordinatorTest {

    private val shutdownConfig = mock<ShutdownConfig>().also {
        whenever(it.globalTimeout()).thenReturn(Duration.ofSeconds(10))
    }
    private val meterRegistry = SimpleMeterRegistry()
    private val shutdownEvent = mock<ShutdownEvent>()

    private fun createCoordinator(
        participantList: List<ShutdownParticipant> = emptyList(),
    ): ShutdownCoordinator = ShutdownCoordinator(
        participants = fakeInstance(participantList),
        meterRegistry = meterRegistry,
        shutdownConfig = shutdownConfig,
    )

    // -- Test Helpers ---------------------------------------------------------

    private class TestParticipant(
        override val shutdownOrder: Int = 0,
        override val shutdownTimeout: Duration = Duration.ofSeconds(5),
        private val action: suspend () -> Unit = {},
    ) : ShutdownParticipant {
        val shutdownCalled = AtomicBoolean(false)

        override suspend fun shutdown() {
            shutdownCalled.set(true)
            action()
        }
    }

    private fun fakeInstance(list: List<ShutdownParticipant>): Instance<ShutdownParticipant> {
        val instance = mock<Instance<ShutdownParticipant>>()
        whenever(instance.iterator()).thenReturn(list.toMutableList().iterator())
        whenever(instance.stream()).thenReturn(list.stream())
        return instance
    }

    // -- A. Initial State -----------------------------------------------------

    @Test
    fun `initial state is RUNNING`() {
        val coordinator = createCoordinator()

        assertEquals(ShutdownState.RUNNING, coordinator.state)
    }

    @Test
    fun `initial isShuttingDown is false`() {
        val coordinator = createCoordinator()

        assertFalse(coordinator.isShuttingDown)
    }

    // -- B. State Transitions -------------------------------------------------

    @Test
    fun `onShutdown transitions state to TERMINATED`() {
        val coordinator = createCoordinator()

        coordinator.onShutdown(shutdownEvent)

        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    @Test
    fun `onShutdown passes through DRAINING state`() {
        var drainingObserved: ShutdownState? = null
        val observingParticipant = object : ShutdownParticipant {
            override val shutdownOrder = 0
            override val shutdownTimeout: Duration = Duration.ofSeconds(5)
            lateinit var coordinatorRef: ShutdownCoordinator
            override suspend fun shutdown() {
                drainingObserved = coordinatorRef.state
            }
        }
        val coord = ShutdownCoordinator(
            participants = fakeInstance(listOf(observingParticipant)),
            meterRegistry = SimpleMeterRegistry(),
            shutdownConfig = shutdownConfig,
        )
        observingParticipant.coordinatorRef = coord

        coord.onShutdown(shutdownEvent)

        assertEquals(ShutdownState.DRAINING, drainingObserved)
        assertEquals(ShutdownState.TERMINATED, coord.state)
    }

    @Test
    fun `isShuttingDown is true during DRAINING phase`() {
        var isShuttingDownDuringDrain: Boolean? = null
        val observingParticipant = object : ShutdownParticipant {
            override val shutdownOrder = 0
            override val shutdownTimeout: Duration = Duration.ofSeconds(5)
            lateinit var coordinatorRef: ShutdownCoordinator
            override suspend fun shutdown() {
                isShuttingDownDuringDrain = coordinatorRef.isShuttingDown
            }
        }
        val coord = ShutdownCoordinator(
            participants = fakeInstance(listOf(observingParticipant)),
            meterRegistry = SimpleMeterRegistry(),
            shutdownConfig = shutdownConfig,
        )
        observingParticipant.coordinatorRef = coord

        coord.onShutdown(shutdownEvent)

        assertTrue(isShuttingDownDuringDrain!!)
        assertTrue(coord.isShuttingDown) // still true at TERMINATED
    }

    // -- C. Participant Invocation --------------------------------------------

    @Test
    fun `single participant is invoked during shutdown`() {
        val participant = TestParticipant()
        val coordinator = createCoordinator(listOf(participant))

        coordinator.onShutdown(shutdownEvent)

        assertTrue(participant.shutdownCalled.get())
    }

    @Test
    fun `shutdown with no participants completes successfully`() {
        val coordinator = createCoordinator(emptyList())

        coordinator.onShutdown(shutdownEvent)

        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    @Test
    fun `multiple participants with same order run concurrently`() {
        val barrier = CompletableDeferred<Unit>()
        val concurrentEntries = AtomicInteger(0)
        val maxConcurrent = AtomicInteger(0)

        fun makeConcurrentParticipant() = TestParticipant(shutdownOrder = 1, action = {
            val current = concurrentEntries.incrementAndGet()
            maxConcurrent.updateAndGet { max -> maxOf(max, current) }
            if (current >= 2) barrier.complete(Unit)
            barrier.await() // suspends (not blocks) until both are running
            concurrentEntries.decrementAndGet()
        })

        val p1 = makeConcurrentParticipant()
        val p2 = makeConcurrentParticipant()
        val coordinator = createCoordinator(listOf(p1, p2))

        coordinator.onShutdown(shutdownEvent)

        assertTrue(p1.shutdownCalled.get())
        assertTrue(p2.shutdownCalled.get())
        assertEquals(2, maxConcurrent.get())
    }

    @Test
    fun `participants with different orders run sequentially lower first`() {
        val executionOrder = CopyOnWriteArrayList<Int>()

        val p1 = TestParticipant(shutdownOrder = 10, action = { executionOrder.add(10) })
        val p2 = TestParticipant(shutdownOrder = 1, action = { executionOrder.add(1) })
        val p3 = TestParticipant(shutdownOrder = 5, action = { executionOrder.add(5) })
        val coordinator = createCoordinator(listOf(p1, p2, p3))

        coordinator.onShutdown(shutdownEvent)

        assertEquals(listOf(1, 5, 10), executionOrder)
    }

    @Test
    fun `later group does not start until earlier group finishes`() {
        val groupOneFinished = AtomicBoolean(false)
        var groupTwoSawGroupOneFinished = false

        val earlyParticipant = TestParticipant(shutdownOrder = 1, action = {
            kotlinx.coroutines.delay(50)
            groupOneFinished.set(true)
        })
        val lateParticipant = TestParticipant(shutdownOrder = 2, action = {
            groupTwoSawGroupOneFinished = groupOneFinished.get()
        })
        val coordinator = createCoordinator(listOf(lateParticipant, earlyParticipant))

        coordinator.onShutdown(shutdownEvent)

        assertTrue(groupTwoSawGroupOneFinished)
    }

    // -- D. Error Handling ----------------------------------------------------

    @Test
    fun `participant that throws does not prevent same-order participants from running`() {
        val failingParticipant = TestParticipant(shutdownOrder = 1, action = {
            throw RuntimeException("boom")
        })
        val healthyParticipant = TestParticipant(shutdownOrder = 1)
        val coordinator = createCoordinator(listOf(failingParticipant, healthyParticipant))

        coordinator.onShutdown(shutdownEvent)

        assertTrue(healthyParticipant.shutdownCalled.get())
        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    @Test
    fun `participant that throws does not prevent later-order participants from running`() {
        val failingParticipant = TestParticipant(shutdownOrder = 1, action = {
            throw RuntimeException("boom")
        })
        val laterParticipant = TestParticipant(shutdownOrder = 2)
        val coordinator = createCoordinator(listOf(failingParticipant, laterParticipant))

        coordinator.onShutdown(shutdownEvent)

        assertTrue(laterParticipant.shutdownCalled.get())
        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    @Test
    fun `participant that exceeds per-participant timeout is cancelled`() {
        val timedOut = AtomicBoolean(false)
        val participant = TestParticipant(
            shutdownTimeout = Duration.ofMillis(50),
            action = {
                try {
                    kotlinx.coroutines.delay(Long.MAX_VALUE) // hangs forever
                } catch (_: kotlinx.coroutines.CancellationException) {
                    timedOut.set(true)
                    throw kotlinx.coroutines.CancellationException("timeout")
                }
            },
        )
        val coordinator = createCoordinator(listOf(participant))

        coordinator.onShutdown(shutdownEvent)

        assertTrue(participant.shutdownCalled.get())
        assertTrue(timedOut.get())
        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    @Test
    fun `timed-out participant does not block other same-order participants`() {
        val healthyParticipant = TestParticipant(shutdownOrder = 1)
        val hangingParticipant = TestParticipant(
            shutdownOrder = 1,
            shutdownTimeout = Duration.ofMillis(50),
            action = { kotlinx.coroutines.delay(Long.MAX_VALUE) },
        )
        val coordinator = createCoordinator(listOf(hangingParticipant, healthyParticipant))

        coordinator.onShutdown(shutdownEvent)

        assertTrue(healthyParticipant.shutdownCalled.get())
        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    // -- E. Global Timeout ----------------------------------------------------

    @Test
    fun `global timeout expiration still reaches TERMINATED`() {
        whenever(shutdownConfig.globalTimeout()).thenReturn(Duration.ofMillis(100))

        val participant = TestParticipant(
            shutdownTimeout = Duration.ofSeconds(60),
            action = {
                kotlinx.coroutines.delay(Long.MAX_VALUE)
            },
        )
        val coordinator = createCoordinator(listOf(participant))

        coordinator.onShutdown(shutdownEvent)

        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    @Test
    fun `global timeout cancels all remaining participants`() {
        whenever(shutdownConfig.globalTimeout()).thenReturn(Duration.ofMillis(100))

        val laterParticipantCalled = AtomicBoolean(false)
        val hangingParticipant = TestParticipant(
            shutdownOrder = 1,
            shutdownTimeout = Duration.ofSeconds(60),
            action = { kotlinx.coroutines.delay(Long.MAX_VALUE) },
        )
        val laterParticipant = TestParticipant(
            shutdownOrder = 2,
            action = { laterParticipantCalled.set(true) },
        )
        val coordinator = createCoordinator(listOf(hangingParticipant, laterParticipant))

        coordinator.onShutdown(shutdownEvent)

        // Global timeout fires during order-1 group, so order-2 never executes
        assertFalse(laterParticipantCalled.get())
        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    // -- F. Idempotency -------------------------------------------------------

    @Test
    fun `calling onShutdown twice invokes participants only once`() {
        val callCount = AtomicInteger(0)
        val participant = TestParticipant(action = { callCount.incrementAndGet() })
        val coordinator = createCoordinator(listOf(participant))

        coordinator.onShutdown(shutdownEvent)
        coordinator.onShutdown(shutdownEvent)

        assertEquals(1, callCount.get())
        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    @Test
    fun `second onShutdown call returns immediately without state change`() {
        val coordinator = createCoordinator()

        coordinator.onShutdown(shutdownEvent)
        assertEquals(ShutdownState.TERMINATED, coordinator.state)

        // Second call -- state remains TERMINATED, no error
        coordinator.onShutdown(shutdownEvent)
        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    // -- G. Metrics -----------------------------------------------------------

    @Test
    fun `shutdown duration timer is recorded after shutdown`() {
        val coordinator = createCoordinator()

        coordinator.onShutdown(shutdownEvent)

        val timer = meterRegistry.find("taskqueue_shutdown_duration_seconds").timer()
        assertNotNull(timer)
        assertTrue(timer.count() > 0)
    }

    @Test
    fun `shutdown duration timer has pod tag`() {
        val coordinator = createCoordinator()

        coordinator.onShutdown(shutdownEvent)

        val timer = meterRegistry.find("taskqueue_shutdown_duration_seconds").timer()
        assertNotNull(timer)
        val podTag = timer.id.getTag("pod")
        assertNotNull(podTag)
    }

    @Test
    fun `shutdown state gauge reflects RUNNING ordinal before shutdown`() {
        val coordinator = createCoordinator()

        val gauge = meterRegistry.find("taskqueue_shutdown_state").gauge()
        assertNotNull(gauge)
        assertEquals(ShutdownState.RUNNING.ordinal.toDouble(), gauge.value())
    }

    @Test
    fun `shutdown state gauge reflects TERMINATED ordinal after shutdown`() {
        val coordinator = createCoordinator()

        coordinator.onShutdown(shutdownEvent)

        val gauge = meterRegistry.find("taskqueue_shutdown_state").gauge()
        assertNotNull(gauge)
        assertEquals(ShutdownState.TERMINATED.ordinal.toDouble(), gauge.value())
    }
}
