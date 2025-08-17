package infra.fault.ratelimiter

import com.google.common.util.concurrent.RateLimiter
import infra.statistic.CostEstimator
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import kotlin.math.ceil

fun <T> Flow<T>.admitWithRateLimiter(
    limiter: RateLimiter,
    estimator: CostEstimator<T>,
): Flow<T> =
    onEach {
        val cost = estimator.estimate(it)
        limiter.suspendableAcquire(ceil(cost).toInt())
    }
