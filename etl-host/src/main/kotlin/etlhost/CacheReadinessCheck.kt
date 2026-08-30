package etlhost

import jakarta.inject.Singleton
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Readiness

/**
 * SimpleEtl spec 8.6's readiness row, at the path a deployment manifest actually probes.
 *
 * A hand-rolled JAX-RS resource is a perfectly good readiness endpoint and answers nothing at
 * `/q/health/ready`, which is where a stock Quarkus manifest looks. The pod then fails its probe
 * against a 404 forever while the service behind it runs perfectly - a failure no test in this
 * module could see, because no test probed the conventional path.
 *
 * **This check computes nothing.** It reads [EtlHost.readinessState] and so does
 * [ReadinessResource]; a probe on either path gets the same answer for the same reason, and there
 * is no second copy of "what does ready mean" to drift. The state string is carried through as
 * `state` data so `/q/health/ready`'s JSON distinguishes *not yet published* from *shutting down*
 * exactly as the older path's body does.
 */
@Readiness
@Singleton
class CacheReadinessCheck(private val host: EtlHost) : HealthCheck {

    override fun call(): HealthCheckResponse {
        val state = host.readinessState
        return HealthCheckResponse.named("snapshot-cache")
            .status(state == EtlHost.READY)
            .withData("state", state)
            .build()
    }
}
