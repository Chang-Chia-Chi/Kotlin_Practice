package infra.shuttle.testkit

import infra.shuttle.core.Hook
import infra.shuttle.core.HookPoint
import infra.shuttle.core.TransferId
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import java.util.concurrent.ConcurrentHashMap

/**
 * The test's `Hook` (spec 4.4). A point that is not paused is a no-op. `pauseAt` arms a point once:
 * every coroutine reaching it suspends until `resume`, `cancelAt` or `crash`, each of which also
 * disarms it, so the pipeline's next run passes through. No sleeps anywhere: `awaitArrival`
 * suspends on the driver's own signal, which is what makes it usable under `runTest`.
 */
class HookDriver : Hook {
    private class Gate {
        val arrivals = Channel<TransferId>(Channel.UNLIMITED)
        val jobs = mutableListOf<Job>()
        val release = CompletableDeferred<Unit>()
    }

    private val gates = ConcurrentHashMap<HookPoint, Gate>()

    fun pauseAt(point: HookPoint) {
        gates[point] = Gate()
    }

    /** Suspends until a pipeline reaches [point]; returns the transfer that arrived. */
    suspend fun awaitArrival(point: HookPoint): TransferId = gate(point).arrivals.receive()

    /** Lets every coroutine paused at [point] continue. A second call is a no-op. */
    fun resume(point: HookPoint) {
        gates.remove(point)?.release?.complete(Unit)
    }

    /** Cancels the job of every coroutine paused at [point]; code after the point never runs. */
    fun cancelAt(point: HookPoint) {
        gates.remove(point)?.jobs?.forEach { it.cancel(CancellationException("cancelled at $point")) }
    }

    /** The process dies here: a `CancellationException` is thrown inside the paused coroutine. */
    fun crash(point: HookPoint) {
        gates.remove(point)?.release?.completeExceptionally(CancellationException("crash at $point"))
    }

    override suspend fun at(point: HookPoint, transfer: TransferId) {
        val gate = gates[point] ?: return
        currentCoroutineContext()[Job]?.let { synchronized(gate.jobs) { gate.jobs += it } }
        gate.arrivals.send(transfer)
        gate.release.await()
    }

    private fun gate(point: HookPoint) = checkNotNull(gates[point]) { "$point is not paused" }
}
