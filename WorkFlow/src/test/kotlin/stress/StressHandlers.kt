package com.workflow.stress

import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import java.util.concurrent.atomic.AtomicInteger

/** Points in the worker lifecycle where a crash can be simulated. */
enum class CrashPoint {
    /** Crash before handler.execute() is called. */
    BEFORE_HANDLER,
    /** Crash during handler.execute() (mid-computation). */
    MID_HANDLER,
    /** Crash after handler returns, before barrier is called.
     *  Since barrier is called by WorkerLoop (not handler), this is simulated
     *  by throwing after the handler returns. WorkerLoop catches this and
     *  routes through the failure path, which leaves the task in PROCESSING
     *  if resetForRetry also fails (because we throw CancellationException). */
    AFTER_HANDLER,
}

/**
 * A handler that crashes at a specified [CrashPoint] on the Nth invocation.
 * Before/after the crash invocation, it delegates to [delegate].
 *
 * @param crashAt where in the lifecycle to crash
 * @param crashOnInvocation 1-based: crash on the Nth call (default: 1 = first call)
 * @param delegate the real handler to run when not crashing
 */
class CrashableHandler(
    private val crashAt: CrashPoint,
    private val crashOnInvocation: Int = 1,
    private val delegate: TransitionHandler = PassThroughHandler(),
) : TransitionHandler {


    private val invocationCount = AtomicInteger(0)

    override suspend fun execute(input: HandlerInput): HandlerResult {
        val n = invocationCount.incrementAndGet()
        val shouldCrash = (n == crashOnInvocation)

        if (shouldCrash && crashAt == CrashPoint.BEFORE_HANDLER) {
            throw CancellationException("Simulated crash BEFORE handler")
        }

        if (shouldCrash && crashAt == CrashPoint.MID_HANDLER) {
            // Start some work, then crash
            delay(10)
            throw CancellationException("Simulated crash MID handler")
        }

        val output = delegate.execute(input)

        if (shouldCrash && crashAt == CrashPoint.AFTER_HANDLER) {
            throw CancellationException("Simulated crash AFTER handler")
        }

        return output
    }
}

/** Returns input as output result. Prefers taskPayload (scatter chunk) over inputs. */
class PassThroughHandler : TransitionHandler {
    override suspend fun execute(input: HandlerInput): HandlerResult =
        HandlerResult(result = input.taskPayload ?: input.inputs)
}

/** Always throws after optional delay. */
class FailingHandler(
    private val delayMs: Long = 0,
    private val message: String = "Simulated failure",
) : TransitionHandler {
    override suspend fun execute(input: HandlerInput): HandlerResult {
        if (delayMs > 0) delay(delayMs)
        throw RuntimeException(message)
    }
}

/** Delays for [delayMs] then delegates. Useful for simulating slow handlers. */
class SlowHandler(
    private val delayMs: Long,
    private val delegate: TransitionHandler = PassThroughHandler(),
) : TransitionHandler {
    override suspend fun execute(input: HandlerInput): HandlerResult {
        delay(delayMs)
        return delegate.execute(input)
    }
}

/**
 * Handler that blocks until explicitly released via [release].
 * Useful for controlling timing in race condition tests.
 */
class GatedHandler(
    private val delegate: TransitionHandler = PassThroughHandler(),
) : TransitionHandler {
    private val gate = CompletableDeferred<Unit>()

    override suspend fun execute(input: HandlerInput): HandlerResult {
        gate.await()
        return delegate.execute(input)
    }

    fun release() { gate.complete(Unit) }
}

/**
 * Tracks invocation count per task ID. Useful for verifying no duplicate processing.
 */
class CountingHandler(
    private val delegate: TransitionHandler = PassThroughHandler(),
) : TransitionHandler {
    val invocations = java.util.concurrent.ConcurrentHashMap<String, AtomicInteger>()
    val totalInvocations = AtomicInteger(0)

    override suspend fun execute(input: HandlerInput): HandlerResult {
        invocations.computeIfAbsent(input.taskId) { AtomicInteger(0) }.incrementAndGet()
        totalInvocations.incrementAndGet()
        return delegate.execute(input)
    }
}

/**
 * Fails the first N invocations, then succeeds.
 */
class FailNThenSucceedHandler(
    private val failCount: Int,
    private val delegate: TransitionHandler = PassThroughHandler(),
) : TransitionHandler {
    private val count = AtomicInteger(0)

    override suspend fun execute(input: HandlerInput): HandlerResult {
        if (count.incrementAndGet() <= failCount) {
            throw RuntimeException("Simulated failure #${count.get()}")
        }
        return delegate.execute(input)
    }
}
