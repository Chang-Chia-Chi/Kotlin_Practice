package infra.fault.ratelimiter

import com.google.common.util.concurrent.RateLimiter
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlin.time.Duration

suspend fun RateLimiter.suspendableAcquire(
    permits: Int = 1,
    dispatcher: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(4),
): Double =
    withContext(dispatcher) {
        acquire(permits)
    }

suspend fun RateLimiter.suspendableTryAcquire(
    permits: Int = 1,
    timeout: Duration,
    dispatcher: kotlin.coroutines.CoroutineContext = Dispatchers.IO.limitedParallelism(4),
): Boolean =
    withContext(dispatcher) {
        tryAcquire(permits, timeout.inWholeNanoseconds, java.util.concurrent.TimeUnit.NANOSECONDS)
    }
