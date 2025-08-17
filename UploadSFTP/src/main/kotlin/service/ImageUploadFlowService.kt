package service

import adapter.MinIOAdapter
import adapter.NatsAdapter
import adapter.SftpAdapter
import com.google.common.util.concurrent.RateLimiter
import infra.coroutine.takeUntilSignal
import infra.coroutine.unorderedMapAsync
import infra.fault.circuitbreakr.BreakerAdmitted
import infra.fault.circuitbreakr.admitWithCircuitBreaker
import infra.fault.ratelimiter.admitWithRateLimiter
import infra.statistic.Ewma
import infra.statistic.fromEwma
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.nats.client.Message
import kotlinx.coroutines.CompletableDeferred

class ImageUploadFlowService(
    private val nats: NatsAdapter,
    private val minio: MinIOAdapter,
    private val sftp: SftpAdapter,
) {
    suspend fun start() {
        val minioRateLimiter = RateLimiter.create(10.0)
        val sftpRateLimiter = RateLimiter.create((1024 * 1024 * 100).toDouble())
        val minioEstimator = fromEwma<BreakerAdmitted<Message>>(Ewma(init = 5.0))
        val sftpEstimator = fromEwma<BreakerAdmitted<Message>>(Ewma(init = (1024 * 1024 * 5).toDouble()))
        val breaker = CircuitBreaker.ofDefaults("flow")
        val defer = CompletableDeferred<Unit>()
        nats
            .consumeAsFlow()
            .takeUntilSignal(defer)
            .admitWithCircuitBreaker(breaker)
            .admitWithRateLimiter(minioRateLimiter, minioEstimator)
            .admitWithRateLimiter(sftpRateLimiter, sftpEstimator)
            .unorderedMapAsync(2) { message ->
                message.value
            }.collect {}
    }
}
