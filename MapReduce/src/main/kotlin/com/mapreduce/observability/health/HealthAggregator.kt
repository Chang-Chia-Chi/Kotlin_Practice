package com.mapreduce.observability.health

import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Instance
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Liveness
import org.eclipse.microprofile.health.Readiness

/**
 * Liveness aggregator — iterates all [HealthContributor] beans, calls [HealthContributor.liveness],
 * and combines results.
 *
 * Aggregation rule: the pod is live if all non-null liveness checks return UP or DEGRADED.
 * Any single DOWN means K8s should restart the pod.
 */
@Liveness
@ApplicationScoped
class LivenessAggregator(
    private val contributors: Instance<HealthContributor>,
) : HealthCheck {

    override fun call(): HealthCheckResponse {
        val builder = HealthCheckResponse.named("mapreduce-liveness")
        var allUp = true

        for (contributor in contributors) {
            val result = contributor.liveness() ?: continue
            builder.withData("${contributor.name}.status", result.status.name)
            for ((key, value) in result.details) {
                builder.withData("${contributor.name}.$key", value.toString())
            }
            if (result.status == HealthStatus.DOWN) {
                allUp = false
            }
        }

        return if (allUp) builder.up().build() else builder.down().build()
    }
}

/**
 * Readiness aggregator — iterates all [HealthContributor] beans, calls [HealthContributor.readiness],
 * and combines results.
 *
 * Aggregation rule: the pod is ready if all non-null readiness checks return UP or DEGRADED.
 * Any single DOWN means K8s should remove the pod from the Service.
 */
@Readiness
@ApplicationScoped
class ReadinessAggregator(
    private val contributors: Instance<HealthContributor>,
) : HealthCheck {

    override fun call(): HealthCheckResponse {
        val builder = HealthCheckResponse.named("mapreduce-readiness")
        var allUp = true

        for (contributor in contributors) {
            val result = contributor.readiness() ?: continue
            builder.withData("${contributor.name}.status", result.status.name)
            for ((key, value) in result.details) {
                builder.withData("${contributor.name}.$key", value.toString())
            }
            if (result.status == HealthStatus.DOWN) {
                allUp = false
            }
        }

        return if (allUp) builder.up().build() else builder.down().build()
    }
}
