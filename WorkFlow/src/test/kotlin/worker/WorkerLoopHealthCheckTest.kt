package com.workflow.worker

import com.workflow.config.FrameworkConfig
import org.eclipse.microprofile.health.HealthCheckResponse
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class WorkerLoopHealthCheckTest {

    private val workerConfig = mock<FrameworkConfig.WorkerConfig>().also {
        whenever(it.pollInterval()).thenReturn(Duration.ofSeconds(1))
    }
    private val config = mock<FrameworkConfig>().also {
        whenever(it.worker()).thenReturn(workerConfig)
    }

    @Test
    fun `returns UP when last activity is recent`() {
        val workerLoop = mock<WorkerLoop>().also {
            whenever(it.lastActivityTimestamp).thenReturn(Instant.now())
        }
        val check = WorkerLoopHealthCheck(workerLoop, config)

        val response = check.call()

        assertEquals(HealthCheckResponse.Status.UP, response.status)
        assertEquals("worker-loop", response.name)
    }

    @Test
    fun `returns DOWN when last activity exceeds threshold`() {
        val staleTime = Instant.now().minus(Duration.ofSeconds(10))
        val workerLoop = mock<WorkerLoop>().also {
            whenever(it.lastActivityTimestamp).thenReturn(staleTime)
        }
        val check = WorkerLoopHealthCheck(workerLoop, config)

        val response = check.call()

        assertEquals(HealthCheckResponse.Status.DOWN, response.status)
        assertEquals("worker-loop", response.name)
        assertTrue(response.data.isPresent, "DOWN response should include diagnostic data")
    }

    @Test
    fun `threshold is 5x poll interval`() {
        // pollInterval = 1s, threshold = 5s, age = 4s -> UP
        val justUnder = Instant.now().minus(Duration.ofSeconds(4))
        val workerLoop = mock<WorkerLoop>().also {
            whenever(it.lastActivityTimestamp).thenReturn(justUnder)
        }
        val check = WorkerLoopHealthCheck(workerLoop, config)

        val response = check.call()

        assertEquals(HealthCheckResponse.Status.UP, response.status)
    }
}
