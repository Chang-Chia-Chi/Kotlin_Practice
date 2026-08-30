package etlhost

import infra.etl.task.CronScheduler
import infra.etl.task.EtlWiring
import infra.etl.task.Outcome
import infra.etl.task.StepContext
import infra.etl.task.StepResult
import infra.etl.task.TaskContext
import infra.etl.task.TaskMetrics
import infra.etl.task.TriggerResult
import infra.etl.task.WiringResult
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.bootstrap.ManagedSnapshotCache
import infra.etl.task.CacheBinding
import io.quarkus.test.common.WithTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import jakarta.inject.Named
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Test

/**
 * **SimpleEtl spec 8.6's listener row, tested by its symptom rather than by its happy path.**
 *
 * `TaskEngine`'s `listener` parameter defaults to `TaskRunListener.NONE`, a *specified no-op* and
 * not a default binding. A host that omits it gets a 30-minute run that emits nothing at all - no
 * start, no step, no failure - while every call site exists and every one of them succeeds. Nothing
 * fails, no log line is missing from anywhere it was configured, and there is nothing to notice.
 *
 * A test asserting "the host's listener logged" would be green on the day someone deletes the
 * argument. This one runs the *same task* through a second `EtlWiring` built exactly like the
 * host's **except for that one argument**, and asserts the silence.
 *
 * The run's completion is observed through the metrics seam, not the listener seam, which is what
 * makes the assertion mean something: metrics prove the run really happened and really ended, so
 * the listener's emptiness is a fact about the listener and not about a run that never started.
 */
@QuarkusTest
@WithTestResource(HostFixture::class)
class ListenerSilenceTest {

    @Inject
    lateinit var config: HostConfig

    @Inject
    lateinit var managed: ManagedSnapshotCache

    @Inject
    lateinit var listener: RecordingListener

    // `@field:` is load-bearing. Without it Kotlin does not put the qualifier where ArC looks, the
    // injection point resolves as @Default, and both Jdbi producers match - AmbiguousResolution at
    // augmentation rather than a wrong connection at run time, which is the good failure mode.
    @Inject
    @field:Named(Producers.TARGET)
    lateinit var target: Jdbi

    /** Counts the run down when the engine reports it ended, whatever the listener did or did not. */
    private class EndLatch : TaskMetrics {
        val ended = CountDownLatch(1)

        @Volatile
        var outcome: Outcome? = null

        override fun taskEnded(ctx: TaskContext, outcome: Outcome, durationMs: Long) {
            this.outcome = outcome
            ended.countDown()
        }

        override fun stepEnded(ctx: StepContext, result: StepResult) = Unit
        override fun stepRetried(ctx: StepContext) = Unit
        override fun scratchBytes(ctx: TaskContext, bytes: Long) = Unit
    }

    @Test
    fun `a wiring built without a listener runs the task and says nothing about it`() {
        val metrics = EndLatch()
        // Identical to Producers.etlWiring but for the missing `listener` - which is the mistake.
        val forgetful = EtlWiring(
            scratchDirectory = config.scratchDirectory,
            cron = CronScheduler { _, _, _ -> AutoCloseable { } },
            datasources = mapOf(config.targetName to target),
            caches = mapOf(HostFixture.GROUP to CacheBinding(managed.cache, GroupId(HostFixture.GROUP))),
            scratchMemoryLimitMb = config.scratchMemoryLimitMb,
            metrics = metrics,
        )

        val wired = forgetful.start(config.taskDirectory)
        assertThat(wired).isInstanceOf(WiringResult.Wired::class.java)
        listener.clear()

        val trigger = (wired as WiringResult.Wired).admin.trigger(HostFixture.TASK, "silence-probe")
        assertThat(trigger).isInstanceOf(TriggerResult.Accepted::class.java)

        try {
            assertThat(metrics.ended.await(60, TimeUnit.SECONDS))
                .withFailMessage("the run never ended, so this test proves nothing about the listener")
                .isTrue()
            // A run that failed instantly would also be silent, and silence would then be a fact
            // about the failure rather than about the seam. It has to have really done the work.
            assertThat(metrics.outcome).isEqualTo(Outcome.SUCCEEDED)

            assertThat(listener.events)
                .withFailMessage(
                    "the listener saw %s. It should have seen nothing: this wiring was built without " +
                        "one, and the point of the row is that the framework is silent by design when " +
                        "the host forgets - not broken, not loud, silent.",
                    listener.events,
                )
                .isEmpty()
        } finally {
            wired.close()
        }
    }
}
