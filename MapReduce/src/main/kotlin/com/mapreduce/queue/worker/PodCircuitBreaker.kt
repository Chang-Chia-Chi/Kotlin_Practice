package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.CBState
import com.mapreduce.event.CircuitBreakerStateChanged
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Event
import org.jboss.logging.Logger
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Pod-level circuit breaker that tracks consecutive task failures
 * across all handlers on this worker.
 *
 * When a worker pod is malfunctioning (corrupted disk, broken NIC, etc.),
 * it rapidly claims and fails tasks, exhausting retry limits. The circuit
 * breaker detects this pattern and voluntarily quarantines the pod.
 *
 * **Tripped state:**
 * - Worker stops polling for new tasks
 * - Kubernetes readiness probe reports DOWN
 * - K8s orchestrator restarts or terminates the pod
 */
@ApplicationScoped
class PodCircuitBreaker(
    private val config: FrameworkConfig,
    private val circuitBreakerEvent: Event<CircuitBreakerStateChanged>,
) {
    private val log = Logger.getLogger(PodCircuitBreaker::class.java)
    private val consecutiveFailures = AtomicInteger(0)
    private val _tripped = AtomicBoolean(false)

    val isTripped: Boolean get() = _tripped.get()

    fun recordSuccess() {
        consecutiveFailures.set(0)
    }

    fun recordFailure() {
        val count = consecutiveFailures.incrementAndGet()
        val threshold = config.worker().circuitBreakerThreshold()
        if (count >= threshold && _tripped.compareAndSet(false, true)) {
            log.errorf(
                "CIRCUIT BREAKER TRIPPED: %d consecutive failures (threshold=%d). " +
                    "Worker is quarantined — readiness probe will report DOWN.",
                count, threshold
            )
            try {
                circuitBreakerEvent.fireAsync(CircuitBreakerStateChanged(
                    name = "pod",
                    previousState = CBState.CLOSED,
                    newState = CBState.OPEN,
                    failureRate = count.toDouble() / threshold,
                ))
            } catch (e: Exception) {
                log.warnf(e, "Failed to fire CircuitBreakerStateChanged event")
            }
        }
    }

    fun reset() {
        consecutiveFailures.set(0)
        if (_tripped.compareAndSet(true, false)) {
            log.info("Circuit breaker reset")
            try {
                circuitBreakerEvent.fireAsync(CircuitBreakerStateChanged(
                    name = "pod",
                    previousState = CBState.OPEN,
                    newState = CBState.CLOSED,
                    failureRate = 0.0,
                ))
            } catch (e: Exception) {
                log.warnf(e, "Failed to fire CircuitBreakerStateChanged event")
            }
        }
    }
}
