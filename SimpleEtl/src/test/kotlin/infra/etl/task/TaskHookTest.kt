package infra.etl.task

import infra.etl.Etl
import infra.etl.EventTrace
import infra.etl.TaskFiles
import infra.etl.TaskHarness
import infra.etl.task.LoadResult
import infra.etl.task.Outcome
import infra.etl.task.TaskFileLoader
import infra.etl.task.TaskHook
import infra.etl.task.TaskHooks
import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * P8a, contract 4 items 6 to 10 and 13: **spec 9.4's hooks** - when they run, what a throwing one
 * does to the outcome, and what an unregistered name means.
 *
 * ### Hooks and listener calls share one trace
 *
 * Every assertion about a hook here is an assertion about *where* it ran relative to the listener
 * calls around it, because that is what spec 9.4 actually says: `onSuccess` after every phase has
 * succeeded, `onFailure` after it if it throws, and `onTaskEnd` after both. Recording hooks in
 * their own list would leave the ordering unstated and the interesting failures invisible.
 *
 * ### Identity, not equality
 *
 * Contract 2.3 says a throwing `onSuccess` makes `TaskOutcome.failure` *identically* that
 * throwable, and that a throwing `onFailure` never replaces the failure being reported. Both are
 * asserted with `isSameAs`. `isEqualTo` would not do: `Throwable` has no value equality, so the
 * check would silently degrade into the same reference check on the happy path and into a
 * confusing failure elsewhere - and the clause worth pinning is precisely that nothing wrapped,
 * re-created or substituted the object on its way out.
 *
 * ### Why the swallowed `onFailure` needs the hook to be recorded
 *
 * "The outcome did not change" is satisfied for free by an engine that never calls `onFailure` at
 * all. So every swallowing test also asserts `runs == 1` on the hook it expected to be reached.
 */
class TaskHookTest {

    @TempDir
    lateinit var root: Path

    /**
     * The smallest run that needs no external datasource: one phase, one `sql` step on scratch.
     * Its success trace is the same six lines in every test below, so a hook's position in the
     * trace is unambiguous.
     */
    private fun okTask(name: String = "wip-hooks") = Etl.task(
        name,
        Etl.phase("only", Etl.sql("touch", Etl.SCRATCH, "create or replace table touched as select 1 as ok")),
    )

    private val successPrefix = listOf(
        "onTaskStart(wip-hooks)",
        "onPhaseStart(only)",
        "onStepStart(only/touch)",
        "onStepEnd(only/touch, attempt=1)",
        "onPhaseEnd(only, SUCCEEDED)",
    )

    /** Contract 4.6: once, after every phase succeeded, before the run is reported ended. */
    @Test
    fun onSuccessRunsOnceAfterEveryPhaseSucceededAndBeforeTheRunEnds() {
        TaskHarness(root).use { harness ->
            val trace = EventTrace()
            harness.listener = trace.listener()
            val notify = trace.hook("notify")
            val page = trace.hook("page")
            harness.hooks.register("notify", notify)
            harness.hooks.register("page", page)

            val outcome = harness.runExpectingSuccess(
                Etl.withHooks(okTask(), onSuccess = "notify", onFailure = "page"),
            )

            assertThat(trace.entries)
                .containsExactlyElementsOf(successPrefix + listOf("hook(notify)", "onTaskEnd(wip-hooks, SUCCEEDED)"))
            assertThat(notify.runs).describedAs("once, not once per phase").isEqualTo(1)
            assertThat(page.runs).describedAs("a successful run has nothing for onFailure to report").isZero()
            assertThat(notify.contexts.single().runId).isEqualTo(outcome.runId)
            assertThat(notify.contexts.single().taskName).isEqualTo("wip-hooks")
        }
    }

    /**
     * Contract 4.7 and 2.3. The phases all succeeded, so their `onPhaseEnd` events say SUCCEEDED
     * and only the task's outcome flips - a whole-trace assertion is the only thing that shows
     * both halves of that at once.
     */
    @Test
    fun aThrowingOnSuccessFailsTheRunWithItsOwnThrowableAndThenRunsOnFailure() {
        TaskHarness(root).use { harness ->
            val boom = IllegalStateException("probe: the onSuccess hook failed")
            val trace = EventTrace()
            harness.listener = trace.listener()
            val notify = trace.hook("notify", failure = boom)
            val page = trace.hook("page")
            harness.hooks.register("notify", notify)
            harness.hooks.register("page", page)

            val outcome = harness.run(Etl.withHooks(okTask(), onSuccess = "notify", onFailure = "page"))

            assertThat(outcome.outcome).isEqualTo(Outcome.FAILED)
            assertThat(outcome.failure)
                .describedAs("the run carries the hook's own throwable, unwrapped and unreplaced")
                .isSameAs(boom)
            assertThat(trace.entries).containsExactlyElementsOf(
                successPrefix + listOf("hook(notify)", "hook(page)", "onTaskEnd(wip-hooks, FAILED)"),
            )
            assertThat(page.runs).isEqualTo(1)
        }
    }

    /**
     * Contract 4.8 and 2.3: the failure-reporting path may never change the failure it is
     * reporting. Both hooks throw; the run must carry the *first* throwable and must have reached
     * the second hook anyway.
     */
    @Test
    fun aThrowingOnFailureIsSwallowedAndNeverReplacesTheFailureItReports() {
        TaskHarness(root).use { harness ->
            val boom = IllegalStateException("probe: the onSuccess hook failed")
            val secondary = IllegalStateException("probe: the onFailure hook failed too")
            val trace = EventTrace()
            harness.listener = trace.listener()
            val notify = trace.hook("notify", failure = boom)
            val page = trace.hook("page", failure = secondary)
            harness.hooks.register("notify", notify)
            harness.hooks.register("page", page)

            val outcome = harness.run(Etl.withHooks(okTask(), onSuccess = "notify", onFailure = "page"))

            assertThat(outcome.outcome).isEqualTo(Outcome.FAILED)
            assertThat(outcome.failure)
                .describedAs("the onFailure throwable never becomes the run's failure")
                .isSameAs(boom)
            assertThat(page.runs)
                .describedAs("swallowed means called and its exception dropped, not never called")
                .isEqualTo(1)
            assertThat(trace.entries).containsExactlyElementsOf(
                successPrefix + listOf("hook(notify)", "hook(page)", "onTaskEnd(wip-hooks, FAILED)"),
            )
        }
    }

    /**
     * Contract 4.9, first half. Hook names resolve at invocation, so an unregistered `onSuccess`
     * behaves exactly like an `onSuccess` that threw.
     *
     * Only the message is asserted, never the exception type: contract 2.3 fixes what the failure
     * has to *say* - the task and the hook, so the operator can find both - and leaves the type to
     * the engineer.
     */
    @Test
    fun anUnregisteredOnSuccessFailsTheRunAndThenRunsOnFailure() {
        TaskHarness(root).use { harness ->
            val trace = EventTrace()
            harness.listener = trace.listener()
            val page = trace.hook("page")
            harness.hooks.register("page", page)

            val outcome = harness.run(Etl.withHooks(okTask(), onSuccess = "missing-hook", onFailure = "page"))

            assertThat(outcome.outcome).isEqualTo(Outcome.FAILED)
            assertThat(outcome.failure?.message)
                .describedAs("the diagnostic names the task and the hook an operator has to go and register")
                .contains("wip-hooks", "missing-hook")
            assertThat(page.runs).isEqualTo(1)
            assertThat(trace.entries).containsExactlyElementsOf(
                successPrefix + listOf("hook(page)", "onTaskEnd(wip-hooks, FAILED)"),
            )
        }
    }

    /**
     * Contract 4.9, second half. An unregistered `onFailure` is not a failure of its own: a run
     * that succeeded never reaches it and stays SUCCEEDED.
     */
    @Test
    fun anUnregisteredOnFailureLeavesASuccessfulRunSucceededAndIsNeverReached() {
        TaskHarness(root).use { harness ->
            val trace = EventTrace()
            harness.listener = trace.listener()

            harness.runExpectingSuccess(Etl.withHooks(okTask(), onFailure = "missing-hook"))

            assertThat(trace.entries)
                .containsExactlyElementsOf(successPrefix + listOf("onTaskEnd(wip-hooks, SUCCEEDED)"))
        }
    }

    /**
     * Contract 2.3's rationale, stated as a test: an unregistered `onFailure` behaves exactly as
     * an `onFailure` that threw - logged, swallowed, outcome untouched. The run fails for its own
     * reason and must still fail for *that* reason, not for the missing hook.
     */
    @Test
    fun anUnregisteredOnFailureNeverChangesTheOutcomeItReports() {
        TaskHarness(root).use { harness ->
            harness.datasource("report_oracle")
            val trace = EventTrace()
            harness.listener = trace.listener()

            val outcome = harness.run(
                Etl.withHooks(
                    Etl.task("wip-hooks", Etl.phase("only", Etl.sql("bad-step", "report_oracle", "this is not sql"))),
                    onFailure = "missing-hook",
                ),
            )

            assertThat(outcome.outcome).isEqualTo(Outcome.FAILED)
            val reported = generateSequence(outcome.failure) { if (it.cause === it) null else it.cause }
                .take(16)
                .joinToString(" ") { it.message.orEmpty() }
            assertThat(reported)
                .describedAs("the step's own failure is what the run carries")
                .contains("this is not sql")
            assertThat(reported)
                .describedAs("the missing hook did not overwrite the failure it was there to report")
                .doesNotContain("missing-hook")
            assertThat(trace.entries.last()).isEqualTo("onTaskEnd(wip-hooks, FAILED)")
        }
    }

    /**
     * Contract 4.10: validation rule 5, proved through the composition the application actually
     * has. **One** [TaskHooks]; its hook is what the engine will invoke, and its `names` is what
     * the loader validates against. Both directions are asserted, because
     * `TaskFileLoader(hooks = TaskHooks().names)` validates against an empty set and rejects every
     * name it is ever shown - a rejection test standing alone would pass against that and prove
     * nothing about the registry the engine reads.
     */
    @Test
    fun aHookNameIsValidatedAgainstTheSameRegistryTheEngineResolvesItIn() {
        TaskHarness(root.resolve("engine")).use { harness ->
            val trace = EventTrace()
            harness.listener = trace.listener()
            val notify = trace.hook("notify-downstream")
            harness.hooks.register("notify-downstream", notify)

            val loader = TaskFileLoader(hooks = harness.hooks.names)

            val accepted = loader.load(taskFileNaming("notify-downstream", "accepted"))
            assertThat(accepted)
                .describedAs("a registered name is a valid task file")
                .isInstanceOf(LoadResult.Loaded::class.java)
            harness.runExpectingSuccess((accepted as LoadResult.Loaded).tasks.single())
            assertThat(notify.runs)
                .describedAs("the name the loader accepted resolved to the hook the engine ran")
                .isEqualTo(1)

            val rejected = loader.load(taskFileNaming("not-registered", "rejected"))
            assertThat(rejected)
                .describedAs("a typo is caught at boot, not at the end of a 30 minute run")
                .isInstanceOf(LoadResult.Invalid::class.java)
            assertThat((rejected as LoadResult.Invalid).report.errors.joinToString(" ") { it.message })
                .contains("not-registered")
        }
    }

    /**
     * Contract 4.13 and 2.4: an `Error` is not a task failure. It propagates out of `run` - which
     * is P5's documented contract for a step that is not built yet - and `onTaskEnd` still fires,
     * because it comes from a `finally` and a listener must never see a run that started and never
     * ended. No hook runs: host code has no business executing while an `Error` unwinds.
     *
     * The trace is asserted at its ends rather than whole. Whether `onPhaseEnd(FAILED)` fires on
     * this path is not fixed by the contract - a `catch (Exception)` around the phase skips it and
     * a `finally` does not - so pinning the middle would pin a choice that is the engineer's.
     */
    @Test
    fun anErrorEscapingTheEngineStillEndsTheRunAndRunsNoHook() {
        TaskHarness(root).use { harness ->
            val trace = EventTrace()
            harness.listener = trace.listener()
            harness.hooks.register("notify", trace.hook("notify"))
            harness.hooks.register("page", trace.hook("page"))
            val definition = Etl.withHooks(
                Etl.task(
                    "wip-cache-copy",
                    Etl.phase(
                        "only",
                        Etl.cacheCopy("copy-in", cache = "wip", sql = "select 1", output = "wip_stg"),
                    ),
                ),
                onSuccess = "notify",
                onFailure = "page",
            )

            assertThatThrownBy { harness.run(definition) }
                .describedAs("P5's contract: a step that is not built yet propagates its Error")
                .isInstanceOf(NotImplementedError::class.java)

            assertThat(trace.entries).isNotEmpty()
            assertThat(trace.entries.first()).isEqualTo("onTaskStart(wip-cache-copy)")
            assertThat(trace.entries.last())
                .describedAs("onTaskEnd comes from a finally, so no run starts without ending")
                .isEqualTo("onTaskEnd(wip-cache-copy, FAILED)")
            assertThat(trace.entries)
                .describedAs("no host code runs while an Error unwinds")
                .noneMatch { it.startsWith("hook(") }
        }
    }

    /**
     * The registry's own contract (contract 1.2). A second registration under one name is refused
     * rather than silently winning: two `@Startup` beans both claiming `invalidate-wip` is a
     * deployment mistake, and the alternative to refusing is that which hook runs depends on bean
     * initialisation order.
     */
    @Test
    fun twoHooksRegisteredUnderOneNameAreRejected() {
        val hooks = TaskHooks()
        val first = TaskHook { }
        hooks.register("notify", first)

        assertThatThrownBy { hooks.register("notify", TaskHook { }) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("notify")

        assertThat(hooks.names).describedAs("what TaskFileLoader validates against").containsExactly("notify")
        assertThat(hooks["notify"]).describedAs("the first registration stands").isSameAs(first)
        assertThat(hooks["absent"]).isNull()
    }

    /**
     * A one-file task directory whose `onSuccess` names [hook]. Deliberately built here rather
     * than by editing `TaskFiles.VALID`: this file has to be valid under every rule *except*
     * possibly rule 5, and the smallest such file is easier to read than a nine-phase one with an
     * anchored replace in it.
     */
    private fun taskFileNaming(hook: String, directory: String): Path {
        val yaml = """
            name: hooked-task
            onSuccess: $hook
            phases:
              - name: only
                steps:
                  - name: touch
                    type: sql
                    datasource: scratch
                    statements:
                      - "create or replace table touched as select 1 as ok"
        """.trimIndent()
        return TaskFiles.dirOf(Files.createDirectories(root.resolve(directory)), "task.yaml" to yaml)
    }
}
