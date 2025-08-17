package infra.fault.circuitbreakr

import infra.coroutine.isCancellation
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.transform
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

fun <T> Flow<T>.admitWithCircuitBreaker(
    breaker: CircuitBreaker,
    probe: Duration = 50.milliseconds,
    maxDelay: Duration? = null,
): Flow<BreakerAdmitted<T>> =
    transform { value ->
        val acquired = AtomicBoolean(false)
        try {
            val ok = breaker.awaitPermission(probe, maxDelay)
            if (!ok) throw CallNotPermittedException.createCallNotPermittedException(breaker)
            acquired.compareAndSet(false, true)
            BreakerAdmitted(value, BreakerCall(breaker))
        } catch (t: Throwable) {
            if (acquired.get()) breaker.releasePermission()
            if (isCancellation(coroutineContext, t)) breaker.releasePermission()
            return@transform
        }
    }
