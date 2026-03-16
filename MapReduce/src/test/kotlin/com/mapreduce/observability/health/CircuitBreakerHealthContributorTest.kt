package com.mapreduce.observability.health

import com.mapreduce.queue.worker.PodCircuitBreaker
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
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
    fun `name is circuit-breaker`() {
        assertEquals("circuit-breaker", contributor.name)
    }

    @Test
    fun `liveness returns null - no opinion`() {
        assertNull(contributor.liveness())
    }

    @Test
    fun `readiness returns UP when circuit breaker is not tripped`() {
        `when`(circuitBreaker.isTripped).thenReturn(false)

        val result = contributor.readiness()

        assertEquals(HealthStatus.UP, result.status)
        assertEquals("CLOSED", result.details["podBreaker"])
    }

    @Test
    fun `readiness returns DOWN when circuit breaker is tripped`() {
        `when`(circuitBreaker.isTripped).thenReturn(true)

        val result = contributor.readiness()

        assertEquals(HealthStatus.DOWN, result.status)
        assertEquals("TRIPPED", result.details["podBreaker"])
        assertEquals(
            "Consecutive failure threshold exceeded — pod quarantined",
            result.details["reason"],
        )
    }

    @Test
    fun `readiness UP details do not contain reason key`() {
        `when`(circuitBreaker.isTripped).thenReturn(false)

        val result = contributor.readiness()

        assertEquals(1, result.details.size)
        assertEquals("CLOSED", result.details["podBreaker"])
    }

    @Test
    fun `readiness DOWN details contain exactly two entries`() {
        `when`(circuitBreaker.isTripped).thenReturn(true)

        val result = contributor.readiness()

        assertEquals(2, result.details.size)
    }
}
