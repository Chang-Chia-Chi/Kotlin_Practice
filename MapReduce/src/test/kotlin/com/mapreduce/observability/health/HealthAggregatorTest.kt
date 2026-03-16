package com.mapreduce.observability.health

import jakarta.enterprise.inject.Instance
import org.eclipse.microprofile.health.HealthCheckResponse
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`

class HealthAggregatorTest {

    private fun mockInstance(contributors: List<HealthContributor>): Instance<HealthContributor> {
        @Suppress("UNCHECKED_CAST")
        val instance = mock(Instance::class.java) as Instance<HealthContributor>
        `when`(instance.iterator()).thenAnswer {
            contributors.toMutableList().iterator()
        }
        return instance
    }

    private fun contributor(
        name: String,
        liveness: ProbeResult? = null,
        readiness: ProbeResult? = null,
    ): HealthContributor = object : HealthContributor {
        override val name = name
        override fun liveness() = liveness
        override fun readiness() = readiness
    }

    // ── LivenessAggregator ────────────────────────────────────────

    @Nested
    inner class LivenessAggregatorTests {

        @Test
        fun `all contributors UP - response is UP`() {
            val instance = mockInstance(listOf(
                contributor("a", liveness = ProbeResult(HealthStatus.UP)),
                contributor("b", liveness = ProbeResult(HealthStatus.UP)),
            ))
            val aggregator = LivenessAggregator(instance)

            val response = aggregator.call()

            assertEquals(HealthCheckResponse.Status.UP, response.status)
        }

        @Test
        fun `one contributor DOWN - response is DOWN`() {
            val instance = mockInstance(listOf(
                contributor("a", liveness = ProbeResult(HealthStatus.UP)),
                contributor("b", liveness = ProbeResult(HealthStatus.DOWN)),
            ))
            val aggregator = LivenessAggregator(instance)

            val response = aggregator.call()

            assertEquals(HealthCheckResponse.Status.DOWN, response.status)
        }

        @Test
        fun `DEGRADED treated as UP - does not cause DOWN`() {
            val instance = mockInstance(listOf(
                contributor("a", liveness = ProbeResult(HealthStatus.UP)),
                contributor("b", liveness = ProbeResult(HealthStatus.DEGRADED)),
            ))
            val aggregator = LivenessAggregator(instance)

            val response = aggregator.call()

            assertEquals(HealthCheckResponse.Status.UP, response.status)
        }

        @Test
        fun `null liveness result is skipped`() {
            val instance = mockInstance(listOf(
                contributor("a", liveness = null),
                contributor("b", liveness = ProbeResult(HealthStatus.UP)),
            ))
            val aggregator = LivenessAggregator(instance)

            val response = aggregator.call()

            assertEquals(HealthCheckResponse.Status.UP, response.status)
            // "a" should not appear in data since it returned null
            val data = response.data.orElse(emptyMap())
            assertTrue(data.keys.none { it.startsWith("a.") })
        }

        @Test
        fun `no contributors - response is UP`() {
            val instance = mockInstance(emptyList())
            val aggregator = LivenessAggregator(instance)

            val response = aggregator.call()

            assertEquals(HealthCheckResponse.Status.UP, response.status)
        }

        @Test
        fun `details from contributors included in response`() {
            val details = mapOf("latency" to "42ms", "pool" to "healthy")
            val instance = mockInstance(listOf(
                contributor("db", liveness = ProbeResult(HealthStatus.UP, details)),
            ))
            val aggregator = LivenessAggregator(instance)

            val response = aggregator.call()

            val data = response.data.orElse(emptyMap())
            assertEquals("UP", data["db.status"])
            assertEquals("42ms", data["db.latency"])
            assertEquals("healthy", data["db.pool"])
        }

        @Test
        fun `response name is mapreduce-liveness`() {
            val instance = mockInstance(emptyList())
            val aggregator = LivenessAggregator(instance)

            val response = aggregator.call()

            assertEquals("mapreduce-liveness", response.name)
        }

        @Test
        fun `all null liveness results - response is UP`() {
            val instance = mockInstance(listOf(
                contributor("a", liveness = null),
                contributor("b", liveness = null),
            ))
            val aggregator = LivenessAggregator(instance)

            val response = aggregator.call()

            assertEquals(HealthCheckResponse.Status.UP, response.status)
        }
    }

    // ── ReadinessAggregator ───────────────────────────────────────

    @Nested
    inner class ReadinessAggregatorTests {

        @Test
        fun `all contributors UP - response is UP`() {
            val instance = mockInstance(listOf(
                contributor("a", readiness = ProbeResult(HealthStatus.UP)),
                contributor("b", readiness = ProbeResult(HealthStatus.UP)),
            ))
            val aggregator = ReadinessAggregator(instance)

            val response = aggregator.call()

            assertEquals(HealthCheckResponse.Status.UP, response.status)
        }

        @Test
        fun `one contributor DOWN - response is DOWN`() {
            val instance = mockInstance(listOf(
                contributor("a", readiness = ProbeResult(HealthStatus.UP)),
                contributor("b", readiness = ProbeResult(HealthStatus.DOWN)),
            ))
            val aggregator = ReadinessAggregator(instance)

            val response = aggregator.call()

            assertEquals(HealthCheckResponse.Status.DOWN, response.status)
        }

        @Test
        fun `DEGRADED treated as UP - does not cause DOWN`() {
            val instance = mockInstance(listOf(
                contributor("a", readiness = ProbeResult(HealthStatus.DEGRADED)),
                contributor("b", readiness = ProbeResult(HealthStatus.UP)),
            ))
            val aggregator = ReadinessAggregator(instance)

            val response = aggregator.call()

            assertEquals(HealthCheckResponse.Status.UP, response.status)
        }

        @Test
        fun `null readiness result is skipped`() {
            val instance = mockInstance(listOf(
                contributor("a", readiness = null),
                contributor("b", readiness = ProbeResult(HealthStatus.UP)),
            ))
            val aggregator = ReadinessAggregator(instance)

            val response = aggregator.call()

            assertEquals(HealthCheckResponse.Status.UP, response.status)
            val data = response.data.orElse(emptyMap())
            assertTrue(data.keys.none { it.startsWith("a.") })
        }

        @Test
        fun `no contributors - response is UP`() {
            val instance = mockInstance(emptyList())
            val aggregator = ReadinessAggregator(instance)

            val response = aggregator.call()

            assertEquals(HealthCheckResponse.Status.UP, response.status)
        }

        @Test
        fun `details from contributors included in response`() {
            val details = mapOf("connections" to "5/10")
            val instance = mockInstance(listOf(
                contributor("pool", readiness = ProbeResult(HealthStatus.UP, details)),
            ))
            val aggregator = ReadinessAggregator(instance)

            val response = aggregator.call()

            val data = response.data.orElse(emptyMap())
            assertEquals("UP", data["pool.status"])
            assertEquals("5/10", data["pool.connections"])
        }

        @Test
        fun `response name is mapreduce-readiness`() {
            val instance = mockInstance(emptyList())
            val aggregator = ReadinessAggregator(instance)

            val response = aggregator.call()

            assertEquals("mapreduce-readiness", response.name)
        }

        @Test
        fun `multiple DOWN contributors - still DOWN`() {
            val instance = mockInstance(listOf(
                contributor("a", readiness = ProbeResult(HealthStatus.DOWN)),
                contributor("b", readiness = ProbeResult(HealthStatus.DOWN)),
            ))
            val aggregator = ReadinessAggregator(instance)

            val response = aggregator.call()

            assertEquals(HealthCheckResponse.Status.DOWN, response.status)
        }
    }
}
