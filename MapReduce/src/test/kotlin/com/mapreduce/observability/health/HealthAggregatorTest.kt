package com.mapreduce.observability.health

import jakarta.enterprise.inject.Instance
import org.eclipse.microprofile.health.HealthCheckResponse
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class HealthAggregatorTest {
    // ── helpers ─────────────────────────────────────────────────────

    private fun contributor(
        name: String,
        liveness: ProbeResult? = null,
        readiness: ProbeResult? = null,
    ) = object : HealthContributor {
        override val name = name

        override fun liveness() = liveness

        override fun readiness() = readiness
    }

    private fun liveness(vararg contributors: HealthContributor): HealthCheckResponse =
        LivenessAggregator(FakeInstance(contributors.toList())).call()

    private fun readiness(vararg contributors: HealthContributor): HealthCheckResponse =
        ReadinessAggregator(FakeInstance(contributors.toList())).call()

    // ── liveness tests ──────────────────────────────────────────────

    @Test
    fun `all contributors UP - liveness is UP`() {
        val result =
            liveness(
                contributor("worker-loop", liveness = ProbeResult(HealthStatus.UP)),
                contributor("oracle", liveness = ProbeResult(HealthStatus.UP)),
                contributor("leader-election", liveness = ProbeResult(HealthStatus.UP)),
            )
        assertEquals(HealthCheckResponse.Status.UP, result.status)
    }

    @Test
    fun `worker loop DOWN - liveness is DOWN`() {
        val result =
            liveness(
                contributor("worker-loop", liveness = ProbeResult(HealthStatus.DOWN, mapOf("reason" to "stale"))),
                contributor("oracle", liveness = ProbeResult(HealthStatus.UP)),
            )
        assertEquals(HealthCheckResponse.Status.DOWN, result.status)
    }

    @Test
    fun `leader election DOWN - liveness is DOWN`() {
        val result =
            liveness(
                contributor("worker-loop", liveness = ProbeResult(HealthStatus.UP)),
                contributor("leader-election", liveness = ProbeResult(HealthStatus.DOWN)),
            )
        assertEquals(HealthCheckResponse.Status.DOWN, result.status)
    }

    @Test
    fun `oracle unreachable - liveness DOWN`() {
        val result =
            liveness(
                contributor("oracle", liveness = ProbeResult(HealthStatus.DOWN)),
                contributor("worker-loop", liveness = ProbeResult(HealthStatus.UP)),
            )
        assertEquals(HealthCheckResponse.Status.DOWN, result.status)
    }

    @Test
    fun `null liveness contributors are skipped`() {
        val result =
            liveness(
                contributor("shutdown", liveness = null),
                contributor("worker-loop", liveness = ProbeResult(HealthStatus.UP)),
            )
        assertEquals(HealthCheckResponse.Status.UP, result.status)
    }

    // ── readiness tests ─────────────────────────────────────────────

    @Test
    fun `all contributors UP - readiness is UP`() {
        val result =
            readiness(
                contributor("worker-loop", readiness = ProbeResult(HealthStatus.UP, mapOf("handlers" to 8))),
                contributor("oracle", readiness = ProbeResult(HealthStatus.UP)),
                contributor("shutdown", readiness = ProbeResult(HealthStatus.UP)),
                contributor("circuit-breakers", readiness = ProbeResult(HealthStatus.UP)),
            )
        assertEquals(HealthCheckResponse.Status.UP, result.status)
    }

    @Test
    fun `DEGRADED circuit breaker - readiness is still UP`() {
        val result =
            readiness(
                contributor("worker-loop", readiness = ProbeResult(HealthStatus.UP)),
                contributor("circuit-breakers", readiness = ProbeResult(HealthStatus.DEGRADED, mapOf("open" to listOf("sftp.upload")))),
                contributor("shutdown", readiness = ProbeResult(HealthStatus.UP)),
            )
        assertEquals(HealthCheckResponse.Status.UP, result.status)
    }

    @Test
    fun `all circuit breakers open (DOWN) - readiness is DOWN`() {
        val result =
            readiness(
                contributor("circuit-breakers", readiness = ProbeResult(HealthStatus.DOWN, mapOf("reason" to "All breakers open"))),
                contributor("worker-loop", readiness = ProbeResult(HealthStatus.UP)),
            )
        assertEquals(HealthCheckResponse.Status.DOWN, result.status)
    }

    @Test
    fun `shutdown DOWN - readiness is DOWN, liveness is UP`() {
        val shutdownContrib =
            contributor(
                "shutdown",
                liveness = null,
                readiness = ProbeResult(HealthStatus.DOWN, mapOf("state" to "DRAINING")),
            )
        val workerContrib =
            contributor(
                "worker-loop",
                liveness = ProbeResult(HealthStatus.UP),
                readiness = ProbeResult(HealthStatus.UP),
            )

        val live = liveness(shutdownContrib, workerContrib)
        val ready = readiness(shutdownContrib, workerContrib)

        assertEquals(HealthCheckResponse.Status.UP, live.status)
        assertEquals(HealthCheckResponse.Status.DOWN, ready.status)
    }

    @Test
    fun `non-leader pod - stale reaper null - no effect on liveness`() {
        val result =
            liveness(
                contributor("stale-reaper", liveness = null),
                contributor("worker-loop", liveness = ProbeResult(HealthStatus.UP)),
                contributor("oracle", liveness = ProbeResult(HealthStatus.UP)),
            )
        assertEquals(HealthCheckResponse.Status.UP, result.status)
    }

    @Test
    fun `oracle unreachable - both liveness and readiness DOWN`() {
        val oracleContrib =
            contributor(
                "oracle",
                liveness = ProbeResult(HealthStatus.DOWN, mapOf("reason" to "unreachable")),
                readiness = ProbeResult(HealthStatus.DOWN, mapOf("reason" to "unreachable")),
            )

        val live = liveness(oracleContrib)
        val ready = readiness(oracleContrib)

        assertEquals(HealthCheckResponse.Status.DOWN, live.status)
        assertEquals(HealthCheckResponse.Status.DOWN, ready.status)
    }

    // ── Fake CDI Instance ───────────────────────────────────────────

    @Suppress("UNCHECKED_CAST")
    private class FakeInstance<T>(
        private val items: List<T>,
    ) : Instance<T> {
        override fun iterator(): MutableIterator<T> = items.toMutableList().iterator()

        override fun get(): T = items.first()

        override fun isAmbiguous(): Boolean = false

        override fun isUnsatisfied(): Boolean = items.isEmpty()

        override fun isResolvable(): Boolean = items.isNotEmpty()

        override fun destroy(instance: T & Any) {}

        override fun select(vararg qualifiers: Annotation): Instance<T> = this

        override fun <U : T> select(
            subtype: Class<U>,
            vararg qualifiers: Annotation,
        ): Instance<U> = this as Instance<U>

        override fun <U : T> select(
            subtype: jakarta.enterprise.util.TypeLiteral<U>,
            vararg qualifiers: Annotation,
        ): Instance<U> = this as Instance<U>

        override fun getHandle(): jakarta.enterprise.inject.Instance.Handle<T> = throw UnsupportedOperationException()

        override fun handles(): MutableIterable<jakarta.enterprise.inject.Instance.Handle<T>> = throw UnsupportedOperationException()
    }
}
