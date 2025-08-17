package infra.fault.circuitbreakr

import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.transform
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

class BreakerCall internal constructor(
    private val breaker: CircuitBreaker,
) {
    private val start = System.nanoTime()
    private val closed = AtomicBoolean(false)

    fun success() =
        endOnce {
            breaker.onSuccess(System.nanoTime() - start, TimeUnit.NANOSECONDS)
        }

    fun error(t: Throwable) =
        endOnce {
            breaker.onError(System.nanoTime() - start, TimeUnit.NANOSECONDS, t)
        }

    fun release() = endOnce { breaker.releasePermission() }

    private inline fun endOnce(block: () -> Unit) {
        if (closed.compareAndSet(false, true)) block()
    }
}

data class BreakerAdmitted<T>(
    val value: T,
    val call: BreakerCall,
)
