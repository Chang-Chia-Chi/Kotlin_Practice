package infra.fault.circuitbreakr

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import kotlinx.coroutines.delay
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

suspend fun CircuitBreaker.awaitPermission(
    probe: Duration = 50.milliseconds,
    maxDelay: Duration? = null,
): Boolean {
    val deadline = maxDelay?.let { System.nanoTime() + it.inWholeNanoseconds }
    while (true) {
        if (tryAcquirePermission()) return true
        if (deadline != null && System.nanoTime() >= deadline) return false
        delay(probe)
    }
}
