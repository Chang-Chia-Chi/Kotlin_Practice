package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.`when`
import org.mockito.Mockito.mock
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

class PodCircuitBreakerTest {

    private lateinit var config: FrameworkConfig
    private lateinit var workerConfig: FrameworkConfig.WorkerConfig
    private lateinit var cb: PodCircuitBreaker

    private val threshold = 5

    @BeforeEach
    fun setUp() {
        config = mock(FrameworkConfig::class.java)
        workerConfig = mock(FrameworkConfig.WorkerConfig::class.java)
        `when`(config.worker()).thenReturn(workerConfig)
        `when`(workerConfig.circuitBreakerThreshold()).thenReturn(threshold)

        cb = PodCircuitBreaker(config)
    }

    @Test
    fun `starts in closed state`() {
        assertFalse(cb.isTripped)
    }

    @Test
    fun `recordSuccess resets failure counter`() {
        repeat(threshold - 1) { cb.recordFailure() }
        cb.recordSuccess()
        repeat(threshold - 1) { cb.recordFailure() }

        assertFalse(cb.isTripped)
    }

    @Test
    fun `failures below threshold do not trip`() {
        repeat(threshold - 1) { cb.recordFailure() }

        assertFalse(cb.isTripped)
    }

    @Test
    fun `reaching threshold trips the breaker`() {
        repeat(threshold) { cb.recordFailure() }

        assertTrue(cb.isTripped)
    }

    @Test
    fun `once tripped stays tripped on further failures`() {
        repeat(threshold) { cb.recordFailure() }
        assertTrue(cb.isTripped)

        repeat(10) { cb.recordFailure() }
        assertTrue(cb.isTripped)
    }

    @Test
    fun `reset clears tripped state`() {
        repeat(threshold) { cb.recordFailure() }
        assertTrue(cb.isTripped)

        cb.reset()

        assertFalse(cb.isTripped)
    }

    @Test
    fun `reset allows retripping after new failures`() {
        repeat(threshold) { cb.recordFailure() }
        cb.reset()
        assertFalse(cb.isTripped)

        repeat(threshold) { cb.recordFailure() }
        assertTrue(cb.isTripped)
    }

    @Test
    fun `interleaved success resets counter preventing trip`() {
        repeat(threshold - 1) { cb.recordFailure() }
        cb.recordSuccess()
        repeat(threshold - 1) { cb.recordFailure() }

        assertFalse(cb.isTripped)
    }

    @Test
    fun `concurrent failures from multiple threads reach threshold correctly`() {
        val threadCount = threshold
        val latch = CountDownLatch(threadCount)
        val executor = Executors.newFixedThreadPool(threadCount)

        repeat(threadCount) {
            executor.submit {
                latch.countDown()
                latch.await()
                cb.recordFailure()
            }
        }

        executor.shutdown()
        executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)

        assertTrue(cb.isTripped)
    }
}
