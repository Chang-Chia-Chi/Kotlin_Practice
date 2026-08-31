package infra.etl.task

import infra.etl.P7Tasks
import infra.etl.P7World
import infra.etl.task.TriggerResult
import infra.etl.task.TriggerSource
import java.nio.file.Path
import kotlin.coroutines.ContinuationInterceptor
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir

/**
 * The `CoroutineName` `TaskRunner` hands into the run body is the task name, and each
 * task is confined to its own serialised view rather than to a bare `Dispatchers.IO`.
 *
 * ### Why the name is asserted from the context and from nowhere else
 *
 * Three measured facts, each closing a route that looks obvious until it is run:
 *
 * 1. **Not readable from inside the run.** The engine is ordinary blocking code with no suspending
 *    frame, so `coroutineContext` is unreachable from it; a probe was measured reading null.
 * 2. **Must not be read from the thread name.** The `@wip-summary#1` tag exists only under `-ea` -
 *    measured, `DefaultDispatcher-worker-1 @wip-summary#1` with assertions on and
 *    `DefaultDispatcher-worker-2` with them off. Surefire sets `-ea`; production does
 *    not. Asserting it that way would take the test's discriminating power from a JVM flag, which
 *    is P4's Windows-file-lock finding in a new costume.
 * 3. **Not readable from underneath the limited view.** Read in kotlinx-coroutines-core-jvm
 *    1.10.1, `internal/LimitedDispatcher.kt`: `dispatch` is
 *    `dispatchInternal(block) { worker -> dispatcher.safeDispatch(this, worker) }`, handing the
 *    *LimitedDispatcher itself* to the underlying dispatcher as the context and never the
 *    coroutine's - its own KDoc says "By design, 'LimitedDispatcher' never dispatches originally
 *    sent tasks to the underlying dispatcher." So a recording dispatcher wrapping
 *    `Dispatchers.IO` *before* `limitedParallelism(1)` is applied reads `context[CoroutineName]`
 *    as null on every dispatch **against a correct implementation**: a green test asserting
 *    nothing. This is written down because that wrapper is the natural first idea, and it fails
 *    silently rather than loudly.
 *
 * That leaves the context itself. [aRunIsSubmittedIntoTheContextThatWasAsserted] is what stops
 * `context` being a decorative accessor no `submit` uses.
 */
class TaskRunnerCoroutineNameTest {

    @TempDir
    lateinit var root: Path

    private val world: P7World by lazy { P7World(root) }

    @AfterEach
    fun tearDown() = world.close()

    private fun nameIn(task: String): String? = world.runner.context(task)[CoroutineName]?.name

    @Test
    fun theCoroutineNameHandedIntoTheRunBodyIsTheTaskName() {
        // Two tasks, not one: a runner handing over a constant, or the wrong task's name, passes
        // a single-task assertion and fails this one.
        assertAll(
            { assertEquals("wip-summary", nameIn("wip-summary")) },
            { assertEquals("lot-rollup", nameIn("lot-rollup")) },
        )
    }

    /**
     * A bare `Dispatchers.IO` is rejected by name - it does not serialise per task, so two
     * firings of one task could overlap. A *shared* limited view would fail in the other
     * direction, serialising every task against every other so that no two could run at once.
     */
    @Test
    fun eachTaskIsConfinedToItsOwnViewAndNotToTheBareIoDispatcher() {
        val first = world.runner.context("wip-summary")[ContinuationInterceptor]
        val second = world.runner.context("lot-rollup")[ContinuationInterceptor]

        assertAll(
            {
                assertNotNull(first) {
                    "a bare Dispatchers.IO does not serialise per task (spec 8.3); there was no interceptor at all"
                }
            },
            {
                assertNotSame(Dispatchers.IO, first) {
                    "a bare Dispatchers.IO does not serialise per task (spec 8.3); was $first"
                }
            },
            {
                assertNotSame(first, second) {
                    "one view per task, or two tasks could not run at once (spec 8.4); both were $first"
                }
            },
        )
    }

    /**
     * The tie between the asserted context and a real run: the same runner, the same task name,
     * and a run body that demonstrably executed somewhere other than the triggering thread.
     * Without it the two tests above would pass against a `context` no `submit` ever launches on.
     */
    @Test
    fun aRunIsSubmittedIntoTheContextThatWasAsserted() {
        val probe = world.probe("probe_ds")
        probe.parking = false
        val definition = P7Tasks.parking("wip-summary", "probe_ds")

        assertEquals(definition.name, nameIn(definition.name))
        assertInstanceOf(
            TriggerResult.Accepted::class.java,
            world.runner.submit(definition, TriggerSource.API, "ops"),
        )

        val worker = probe.awaitEntry()
        assertAll(
            {
                assertNotSame(Thread.currentThread(), worker) {
                    "the run body executed on the triggering thread: $worker"
                }
            },
            // "not Dispatchers.IO" and "not the same object" are both satisfied by a per-task
            // newSingleThreadContext, which is rejected by name for keeping an idle thread alive
            // per task. The pool's thread-name *prefix* is what separates them, and unlike the
            // `@taskName#1` suffix it does not depend on `-ea`.
            {
                assertTrue(worker.name.startsWith("DefaultDispatcher-worker")) {
                    "a limited view shares the IO pool rather than owning a thread (spec 8.3); " +
                        "the worker was named ${worker.name}"
                }
            },
        )
    }
}
