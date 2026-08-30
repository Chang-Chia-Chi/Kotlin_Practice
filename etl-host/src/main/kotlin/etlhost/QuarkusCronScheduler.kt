package etlhost

import infra.etl.task.CronScheduler
import io.quarkus.scheduler.Scheduler
import jakarta.inject.Singleton
import org.jboss.logging.Logger

/**
 * SimpleEtl spec 8.6 rows 2 and 5: the host's binding over Quarkus's **programmatic** [Scheduler].
 *
 * Two obligations, and they are separate things that happen to live in one method.
 *
 * **Row 5 - it really parses.** Validation rule 16 is structural only, so this is the one place in
 * the system where a cron expression meets a parser. `schedule()` throws here rather than
 * returning, which is what `TaskScheduler.apply` needs to roll a whole batch back: a host that
 * accepts a bad expression makes spec 8.5's atomic reload a lie, and `EtlWiring.start` can only
 * report what this rejects. Quarkus's own parser is the one doing the work - a hand-rolled
 * field-count check accepts a six-token expression whose last token is `BADFIELD`, and the
 * framework never finds out.
 *
 * **Row 2 - it hands off and does not run.** [run] is `TaskScheduler`'s callback, which reaches
 * `TaskRunner.submit`, which dispatches onto the task's own `limitedParallelism(1)` view and
 * returns. So this call is non-blocking by construction and the scheduler's worker is released
 * immediately. That is the whole obligation: running a 5-30 minute ETL inline on a scheduler thread
 * produces Vert.x blocked-thread warnings past 60 seconds and pins a worker for the duration.
 * `HandsOffTest` asserts the property that would break first - the thread the step body runs on is
 * not the thread the scheduler fired.
 *
 * Concurrent execution is left at Quarkus's default on purpose. `TaskRunner`'s `AtomicBoolean` is
 * spec 8.4's skip-don't-queue guard and it already answers `AlreadyRunning` for an overlapping
 * firing; a second guard here would be a second answer to one question, and the framework's is the
 * one `TaskAdmin.list()` reports.
 */
@Singleton
class QuarkusCronScheduler(private val scheduler: Scheduler) : CronScheduler {

    override fun schedule(taskName: String, cron: String, run: () -> Unit): AutoCloseable {
        val identity = IDENTITY_PREFIX + taskName
        // A re-registration for the same task (every reload re-registers what changed) would
        // otherwise collide with the surviving identity and throw for the wrong reason.
        scheduler.unscheduleJob(identity)
        scheduler.newJob(identity)
            .setCron(cron)
            .setTask { run() }
            .schedule()
        log.debugv("registered cron {0} for task {1} as job {2}", cron, taskName, identity)
        return AutoCloseable { scheduler.unscheduleJob(identity) }
    }

    private companion object {
        /**
         * Namespaced because [Scheduler] identities are global to the application and this host
         * also owns `@Scheduled` methods. A task called `tick` must not be able to unschedule the
         * cache's refresh tick by being reloaded.
         */
        const val IDENTITY_PREFIX = "etl-task:"
        val log: Logger = Logger.getLogger(QuarkusCronScheduler::class.java)
    }
}
