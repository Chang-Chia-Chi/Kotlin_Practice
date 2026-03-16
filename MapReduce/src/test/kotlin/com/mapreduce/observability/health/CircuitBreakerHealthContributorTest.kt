package com.mapreduce.observability.health

import com.mapreduce.queue.worker.PodCircuitBreaker
import org.eclipse.microprofile.health.HealthCheckResponse
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`

class CircuitBreakerHealthContributorTest {

    private lateinit var circuitBreaker: PodCircuitBreaker
    private lateinit var contributor: CircuitBreakerHealthContributor

    @BeforeEach
    fun setUp() {
        circuitBreaker = mock(PodCircuitBreaker::class.java)
        contributor = CircuitBreakerHealthContributor(circuitBreaker)
    }

    @Test
    fun `call returns UP when circuit breaker is not tripped`() {
        `when`(circuitBreaker.isTripped).thenReturn(false)

        val response = contributor.call()

        assertEquals("circuit-breaker", response.name)
        assertEquals(HealthCheckResponse.Status.UP, response.status)
        assertTrue(response.data.isPresent)
        assertEquals("CLOSED", response.data.get()["podBreaker"])
    }

    @Test
    fun `call returns DOWN when circuit breaker is tripped`() {
        `when`(circuitBreaker.isTripped).thenReturn(true)

        val response = contributor.call()

        assertEquals("circuit-breaker", response.name)
        assertEquals(HealthCheckResponse.Status.DOWN, response.status)
        assertTrue(response.data.isPresent)
        assertEquals("TRIPPED", response.data.get()["podBreaker"])
        assertEquals(
            "Consecutive failure threshold exceeded — pod quarantined",
            response.data.get()["reason"],
        )
    }

    @Test
    fun `UP response does not contain reason key`() {
        `when`(circuitBreaker.isTripped).thenReturn(false)

        val response = contributor.call()
        val data = response.data.get()

        assertEquals(1, data.size)
        assertEquals("CLOSED", data["podBreaker"])
    }

    @Test
    fun `DOWN response contains exactly two entries`() {
        `when`(circuitBreaker.isTripped).thenReturn(true)

        val response = contributor.call()

        assertEquals(2, response.data.get().size)
    }
}
