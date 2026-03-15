package com.mapreduce.queue.worker

import com.mapreduce.event.CircuitBreakerStateChanged
import com.mapreduce.testinfra.TestConfig
import jakarta.enterprise.event.Event
import jakarta.enterprise.event.NotificationOptions
import jakarta.enterprise.util.TypeLiteral
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

class PodCircuitBreakerTest {

    private val firedEvents = CopyOnWriteArrayList<CircuitBreakerStateChanged>()

    private val mockEvent = object : Event<CircuitBreakerStateChanged> {
        override fun fire(event: CircuitBreakerStateChanged) {
            firedEvents.add(event)
        }

        override fun fireAsync(event: CircuitBreakerStateChanged): CompletionStage<CircuitBreakerStateChanged> {
            firedEvents.add(event)
            return CompletableFuture.completedFuture(event)
        }

        override fun fireAsync(
            event: CircuitBreakerStateChanged,
            options: NotificationOptions,
        ): CompletionStage<CircuitBreakerStateChanged> {
            firedEvents.add(event)
            return CompletableFuture.completedFuture(event)
        }

        override fun select(vararg qualifiers: Annotation): Event<CircuitBreakerStateChanged> =
            throw UnsupportedOperationException()

        override fun <U : CircuitBreakerStateChanged> select(
            subtype: Class<U>,
            vararg qualifiers: Annotation,
        ): Event<U> = throw UnsupportedOperationException()

        override fun <U : CircuitBreakerStateChanged> select(
            subtype: TypeLiteral<U>,
            vararg qualifiers: Annotation,
        ): Event<U> = throw UnsupportedOperationException()
    }

    private lateinit var breaker: PodCircuitBreaker

    @BeforeEach
    fun setup() {
        firedEvents.clear()
        breaker = PodCircuitBreaker(TestConfig.create(circuitBreakerThreshold = 3), mockEvent)
    }

    @Test
    fun `not tripped initially`() {
        assertFalse(breaker.isTripped)
    }

    @Test
    fun `below threshold does not trip`() {
        breaker.recordFailure()
        breaker.recordFailure()
        assertFalse(breaker.isTripped)
    }

    @Test
    fun `at threshold trips`() {
        repeat(3) { breaker.recordFailure() }
        assertTrue(breaker.isTripped)
    }

    @Test
    fun `fires event when tripped`() {
        repeat(3) { breaker.recordFailure() }
        assertEquals(1, firedEvents.size)
        assertEquals("pod", firedEvents[0].name)
    }

    @Test
    fun `above threshold stays tripped without duplicate event`() {
        repeat(5) { breaker.recordFailure() }
        assertTrue(breaker.isTripped)
        // compareAndSet prevents double-fire
        assertEquals(1, firedEvents.size)
    }

    @Test
    fun `recordSuccess resets consecutive counter`() {
        breaker.recordFailure()
        breaker.recordFailure()
        breaker.recordSuccess()
        // After reset, need 3 more to trip
        breaker.recordFailure()
        breaker.recordFailure()
        assertFalse(breaker.isTripped)
    }

    @Test
    fun `recordSuccess does not un-trip a tripped breaker`() {
        repeat(3) { breaker.recordFailure() }
        assertTrue(breaker.isTripped)
        breaker.recordSuccess()
        // Stays tripped — only reset() clears tripped state
        assertTrue(breaker.isTripped)
    }

    @Test
    fun `success between failures resets count`() {
        breaker.recordFailure()
        breaker.recordFailure()
        breaker.recordSuccess()
        breaker.recordFailure()
        breaker.recordFailure()
        assertFalse(breaker.isTripped)
        // Third consecutive failure after success trips it
        breaker.recordFailure()
        assertTrue(breaker.isTripped)
    }

    @Test
    fun `reset clears tripped state`() {
        repeat(3) { breaker.recordFailure() }
        assertTrue(breaker.isTripped)
        breaker.reset()
        assertFalse(breaker.isTripped)
    }

    @Test
    fun `reset fires close event`() {
        repeat(3) { breaker.recordFailure() }
        firedEvents.clear()
        breaker.reset()
        assertEquals(1, firedEvents.size)
        assertEquals(com.mapreduce.event.CBState.CLOSED, firedEvents[0].newState)
    }

    @Test
    fun `reset when not tripped does not fire event`() {
        firedEvents.clear()
        breaker.reset()
        assertEquals(0, firedEvents.size)
    }

    @Test
    fun `after reset can trip again`() {
        repeat(3) { breaker.recordFailure() }
        breaker.reset()
        assertFalse(breaker.isTripped)
        repeat(3) { breaker.recordFailure() }
        assertTrue(breaker.isTripped)
    }

    @Test
    fun `concurrent failures eventually trip`() {
        val executor = Executors.newFixedThreadPool(4)
        val latch = CountDownLatch(1)
        val tasks = (1..20).map {
            executor.submit {
                latch.await()
                breaker.recordFailure()
            }
        }
        latch.countDown()
        tasks.forEach { it.get() }
        executor.shutdown()

        assertTrue(breaker.isTripped)
        // Only one open event despite concurrent access
        assertEquals(1, firedEvents.count { it.newState == com.mapreduce.event.CBState.OPEN })
    }
}
