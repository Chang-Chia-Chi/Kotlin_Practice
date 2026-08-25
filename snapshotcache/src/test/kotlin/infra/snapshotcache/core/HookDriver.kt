package infra.snapshotcache.core

import infra.snapshotcache.api.Hook
import infra.snapshotcache.api.HookRunner
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * P5 deliverable (plan P5): the latch-based hook driver. One [HookRunner] instance serves
 * registry AND cycle hooks - AFTER_READ_CURRENT / AFTER_POINTER_SWAP / BEFORE_DETACH fire
 * from [GenerationRegistry], AFTER_VERIFY / BEFORE_POINTER_SWAP from [RefreshCycle] - and
 * parks selectively: only an armed [Hook] value parks, everything else runs through.
 *
 * Arm-once semantics: a [Gate] is spent on its first passage, so a retry of the same code
 * path (e.g. the facade's second tryAcquire after a wait) cannot re-park and deadlock.
 *
 * Every await is a throwing proceed-check with a 10-second bound: a broken interleaving
 * fails loudly in whichever thread is stuck instead of hanging the suite. The bounds are
 * bounds on broken implementations, never sequencing (spec 17.4 - zero sleeps).
 */
internal class HookDriver : HookRunner {

    /** One armed park point. Spent after its first passage; later passages run through unparked. */
    class Gate internal constructor(internal val hook: Hook) {
        internal val reached = CountDownLatch(1)
        internal val proceed = CountDownLatch(1)
        internal val armed = AtomicBoolean(true)
    }

    private val gates = CopyOnWriteArrayList<Gate>()

    /** Arms a one-shot park at the next passage of [hook]. */
    fun arm(hook: Hook): Gate = Gate(hook).also { gates += it }

    override fun at(hook: Hook) {
        for (gate in gates) {
            if (gate.hook == hook && gate.armed.compareAndSet(true, false)) {
                gate.reached.countDown()
                check(gate.proceed.await(10, TimeUnit.SECONDS)) {
                    "HookDriver: thread parked at $hook was never released - " +
                        "broken interleaving, failing loudly instead of hanging"
                }
            }
        }
    }

    /** Bounded precondition check that some thread actually parked at [gate]; throws, never hangs. */
    fun awaitParked(gate: Gate) {
        check(gate.reached.await(10, TimeUnit.SECONDS)) {
            "HookDriver: ${gate.hook} was never reached - the expected interleaving did not happen"
        }
    }

    /** Releases the thread parked at [gate]. */
    fun release(gate: Gate) {
        gate.proceed.countDown()
    }
}
