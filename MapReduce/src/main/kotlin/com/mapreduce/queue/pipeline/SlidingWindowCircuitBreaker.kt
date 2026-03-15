package com.mapreduce.queue.pipeline

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * Per-handler sliding window circuit breaker.
 *
 * ```
 * CLOSED ──(failure rate ≥ threshold)──► OPEN ──(wait elapsed)──► HALF_OPEN
 *   ▲                                                                │
 *   └──────────────(probes succeed)──────────────────────────────────┘
 *                                                                    │
 *                        OPEN ◄──(any probe fails)───────────────────┘
 * ```
 *
 * Unlike the pod-level [com.mapreduce.queue.worker.PodCircuitBreaker] which
 * detects pod-wide malfunction via consecutive failures, this breaker
 * protects individual handlers against downstream outages using a sliding
 * window failure rate.
 */
class SlidingWindowCircuitBreaker(
    val handlerName: String,
    private val failureRateThreshold: Double,
    private val slidingWindowSize: Int,
    private val waitDurationMs: Long,
    private val permittedCallsInHalfOpen: Int,
    private val onStateChange: (State, State) -> Unit = { _, _ -> },
) {

    enum class State { CLOSED, OPEN, HALF_OPEN }

    private val state = AtomicReference(State.CLOSED)
    private val openedAt = AtomicLong(0)
    private val halfOpenCalls = AtomicInteger(0)
    private val halfOpenSuccesses = AtomicInteger(0)
    private val halfOpenFailures = AtomicInteger(0)

    // Sliding window: ring buffer of outcomes (true = success)
    private val window = BooleanArray(slidingWindowSize)
    private var windowIndex = 0
    private var windowCount = 0

    val currentState: State get() = state.get()

    val waitDuration: Duration get() = Duration.ofMillis(waitDurationMs)

    /**
     * Check whether the breaker allows execution.
     *
     * @return `true` if CLOSED or HALF_OPEN with remaining probe capacity
     */
    fun tryAcquire(): Boolean =
        when (state.get()) {
            State.CLOSED -> true
            State.OPEN -> {
                if (System.currentTimeMillis() - openedAt.get() >= waitDurationMs) {
                    if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                        halfOpenCalls.set(0)
                        halfOpenSuccesses.set(0)
                        halfOpenFailures.set(0)
                        onStateChange(State.OPEN, State.HALF_OPEN)
                    }
                    halfOpenCalls.incrementAndGet() <= permittedCallsInHalfOpen
                } else {
                    false
                }
            }
            State.HALF_OPEN ->
                halfOpenCalls.incrementAndGet() <= permittedCallsInHalfOpen
        }

    fun recordSuccess() {
        when (state.get()) {
            State.CLOSED -> recordInWindow(true)
            State.HALF_OPEN -> {
                val successes = halfOpenSuccesses.incrementAndGet()
                if (successes + halfOpenFailures.get() >= permittedCallsInHalfOpen) {
                    if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                        resetWindow()
                        onStateChange(State.HALF_OPEN, State.CLOSED)
                    }
                }
            }
            State.OPEN -> { /* shouldn't happen — defensive no-op */ }
        }
    }

    fun recordFailure() {
        when (state.get()) {
            State.CLOSED -> {
                recordInWindow(false)
                checkThreshold()
            }
            State.HALF_OPEN -> {
                halfOpenFailures.incrementAndGet()
                if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
                    openedAt.set(System.currentTimeMillis())
                    onStateChange(State.HALF_OPEN, State.OPEN)
                }
            }
            State.OPEN -> { /* shouldn't happen — defensive no-op */ }
        }
    }

    @Synchronized
    private fun recordInWindow(success: Boolean) {
        val idx = windowIndex
        windowIndex = (windowIndex + 1) % slidingWindowSize
        window[idx] = success
        windowCount = minOf(windowCount + 1, slidingWindowSize)
    }

    @Synchronized
    private fun checkThreshold() {
        val count = windowCount
        if (count < slidingWindowSize) return // not enough data

        val failures = window.count { !it }
        val rate = failures.toDouble() / count * 100.0
        if (rate >= failureRateThreshold) {
            if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                openedAt.set(System.currentTimeMillis())
                onStateChange(State.CLOSED, State.OPEN)
            }
        }
    }

    @Synchronized
    private fun resetWindow() {
        windowCount = 0
        windowIndex = 0
    }
}
