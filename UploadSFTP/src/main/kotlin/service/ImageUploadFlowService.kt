package service

import adapter.MinIOAdapter
import adapter.NatsAdapter
import adapter.SftpAdapter
import com.google.common.util.concurrent.RateLimiter
import infra.coroutine.pullExpiresInAsFlow
import infra.coroutine.takeUntilSignal
import infra.coroutine.unorderedMapAsync
import infra.fault.circuitbreakr.BreakerAdmitted
import infra.fault.circuitbreakr.admitWithCircuitBreaker
import infra.fault.ratelimiter.admitWithRateLimiter
import infra.statistic.Ewma
import infra.statistic.fromEwma
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.nats.client.JetStreamSubscription
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.map
import kotlinx.serialization.json.Json
import model.HeartBeatNatsMessage
import model.ImageInfo

class ImageUploadFlowService(
    private val nats: NatsAdapter,
    private val minio: MinIOAdapter,
    private val sftp: SftpAdapter,
) {
    suspend fun start() {
        val minioRateLimiter = RateLimiter.create(10.0)
        val sftpRateLimiter = RateLimiter.create((1024 * 1024 * 100).toDouble())
        val minioEstimator = fromEwma<BreakerAdmitted<JetStreamSubscription>>(Ewma(init = 5.0))
        val sftpEstimator = fromEwma<BreakerAdmitted<JetStreamSubscription>>(Ewma(init = (1024 * 1024 * 5).toDouble()))
        val breaker = CircuitBreaker.ofDefaults("flow")
        val defer = CompletableDeferred<Unit>()
        nats
            .subscriptionAsFlow()
            .takeUntilSignal(defer)
            .admitWithCircuitBreaker(breaker)
            .admitWithRateLimiter(minioRateLimiter, minioEstimator)
            .admitWithRateLimiter(sftpRateLimiter, sftpEstimator)
            .collect { permitted ->
                val sub = permitted.value
                try {
                    sub
                        .pullExpiresInAsFlow(10, 1000)
                        .map { HeartBeatNatsMessage(it, 1000) }
                        .unorderedMapAsync(concurrency = 4) { msg ->
                            msg.heartBeat()
                            val data = msg.delegate.data
                            val sid = msg.delegate.sid
                            val imageInfo = Json.decodeFromString<ImageInfo>(data.toString())
                        }
                } finally {
                    sub.drain(java.time.Duration.ofSeconds(1))
                    sub.unsubscribe()
                }
            }
    }
}
