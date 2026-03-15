package com.mapreduce.queue.pipeline

import com.mapreduce.event.CBState
import com.mapreduce.event.CircuitBreakerStateChanged
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.registry.HandlerRegistry
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Event
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap

/**
 * Per-handler circuit breaker middleware (order 30).
 *
 * Prevents a downstream outage from draining the queue into dead-letter.
 * Only active for handlers annotated with [HandlerCircuitBreaker].
 *
 * When the breaker is **OPEN**, returns `Retry(delay=waitDuration, consumeRetry=false)`
 * — the retry does NOT consume a retry attempt (system-level concern, not handler failure).
 *
 * Fires [CircuitBreakerStateChanged] events on state transitions, enabling
 * the claim loop to suppress handlers with open breakers.
 */
@ApplicationScoped
class CircuitBreakerMiddleware(
    private val handlerRegistry: HandlerRegistry,
    private val circuitBreakerEvent: Event<CircuitBreakerStateChanged>,
) : HandlerMiddleware {

    override val order: Int = 30

    private val log = Logger.getLogger(CircuitBreakerMiddleware::class.java)

    /** Wrapper to store null (no breaker) vs. present breaker in ConcurrentHashMap. */
    private data class BreakerHolder(val breaker: SlidingWindowCircuitBreaker?)

    private val breakers = ConcurrentHashMap<String, BreakerHolder>()

    override suspend fun invoke(
        context: TaskExecutionContext,
        next: suspend (TaskExecutionContext) -> TaskResult,
    ): TaskResult {
        val breaker = getBreaker(context.handler)
            ?: return next(context) // no breaker configured — pass through

        if (!breaker.tryAcquire()) {
            log.debugf(
                "Circuit breaker OPEN for handler '%s' — returning Retry(consumeRetry=false)",
                context.handler,
            )
            return TaskResult.Retry(
                delay = breaker.waitDuration,
                reason = "Circuit breaker open for '${context.handler}'",
                consumeRetry = false,
            )
        }

        val result = next(context)

        when (result) {
            is TaskResult.Success -> breaker.recordSuccess()
            is TaskResult.Retry,
            is TaskResult.Failure,
            is TaskResult.DeadLetter,
            -> breaker.recordFailure()
        }

        return result
    }

    /** Get the set of currently suppressed handler names (breaker OPEN). */
    fun suppressedHandlers(): Set<String> =
        breakers.values
            .mapNotNull { it.breaker }
            .filter { it.currentState == SlidingWindowCircuitBreaker.State.OPEN }
            .map { it.handlerName }
            .toSet()

    private fun getBreaker(handlerName: String): SlidingWindowCircuitBreaker? =
        breakers.computeIfAbsent(handlerName) { BreakerHolder(createBreaker(it)) }.breaker

    private fun createBreaker(handlerName: String): SlidingWindowCircuitBreaker? {
        val handler = handlerRegistry.resolve(handlerName) ?: return null
        val annotation =
            handler::class.java.getAnnotation(HandlerCircuitBreaker::class.java) ?: return null

        return SlidingWindowCircuitBreaker(
            handlerName = handlerName,
            failureRateThreshold = annotation.failureRateThreshold,
            slidingWindowSize = annotation.slidingWindowSize,
            waitDurationMs = annotation.waitDurationSeconds * 1000,
            permittedCallsInHalfOpen = annotation.permittedCallsInHalfOpen,
        ) { prevState, newState ->
            fireStateChangedEvent(handlerName, prevState, newState)
        }
    }

    private fun fireStateChangedEvent(
        handlerName: String,
        prev: SlidingWindowCircuitBreaker.State,
        new: SlidingWindowCircuitBreaker.State,
    ) {
        log.infof("Circuit breaker for '%s': %s → %s", handlerName, prev, new)
        try {
            circuitBreakerEvent.fireAsync(
                CircuitBreakerStateChanged(
                    name = handlerName,
                    previousState = toCBState(prev),
                    newState = toCBState(new),
                    failureRate = 0.0, // approximation — exact rate is internal to the breaker
                ),
            )
        } catch (e: Exception) {
            log.warnf(e, "Failed to fire CircuitBreakerStateChanged event for '%s'", handlerName)
        }
    }

    private fun toCBState(state: SlidingWindowCircuitBreaker.State): CBState =
        when (state) {
            SlidingWindowCircuitBreaker.State.CLOSED -> CBState.CLOSED
            SlidingWindowCircuitBreaker.State.OPEN -> CBState.OPEN
            SlidingWindowCircuitBreaker.State.HALF_OPEN -> CBState.HALF_OPEN
        }
}
