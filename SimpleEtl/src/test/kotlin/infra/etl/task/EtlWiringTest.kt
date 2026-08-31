package infra.etl.task

import infra.etl.Etl
import infra.etl.P7Tasks
import infra.etl.RecordingCron
import infra.etl.Trig
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.createDirectories
import kotlin.io.path.writeText
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir

/**
 * E15b: `EtlWiring`, the wiring stated once.
 *
 * Four of the host's wiring obligations are the same shape - *pass the same thing to two
 * constructors* - and this class exists so a host cannot get one of them wrong. What is asserted
 * here is therefore never "the object was constructed": it is the **consequence** of the two
 * constructors agreeing, taken from the far end of the machine.
 *
 * ### The hook pair is the discriminating case, and it takes both tests
 *
 * `TaskFileLoader`'s four name sets all default to empty, so a bare `TaskFileLoader()` compiles and
 * validation rule 5 then passes vacuously for every hook name in every file. That gives
 * two ways to be wrong and each test only catches one:
 *
 * - [aHookTheHostRegisteredValidatesAndThenActuallyRuns] fails if the wiring passed the *engine's*
 *   registry and an empty set to the loader - startup would reject a name the engine can resolve.
 * - [aHookNobodyRegisteredIsRejectedAtStartup] fails if the loader got no set at all - the typo
 *   would load cleanly and die at the end of the run, which is the failure hook validation exists
 *   to prevent.
 *
 * Neither is satisfiable by a constant, because the two files differ only in one hook name.
 *
 * ### Nothing here asserts anything about the obligations `EtlWiring` cannot absorb
 *
 * `start-mode=forced`, `@RolesAllowed`, the `AdminResource` mapping, a `CronScheduler` that throws
 * on a bad cron, micrometer on the runtime classpath and `MicrometerTaskMetrics.seed` are all
 * still the host's, and this file is evidence about none of them. That list is in the
 * class's own KDoc so a green run here is not read as covering it. [RecordingCron] does honour the
 * throw-on-bad-cron obligation, which is what makes [aRejectedCronLeavesNothingWiredOrRegistered]
 * a test of `EtlWiring`'s reporting rather than of the host's parsing.
 */
class EtlWiringTest {

    @TempDir
    lateinit var root: Path

    private val cron = RecordingCron()

    private val hooks = TaskHooks()

    private fun wiring() = EtlWiring(
        scratchDirectory = root.resolve("scratch"),
        cron = cron,
        hooks = hooks,
        scratchMemoryLimitMb = Etl.MEMORY_LIMIT_MB,
    )

    /**
     * One task file whose only datasource is `scratch`, so a wiring with no `Jdbi` at all is still
     * a valid host - `scratch` is a reserved name rather than a configured datasource.
     */
    private fun taskDirectory(hookName: String): Path {
        val directory = root.resolve("tasks").also { it.createDirectories() }
        directory.resolve("wired.yaml").writeText(
            """
            name: wired-task
            onSuccess: $hookName
            phases:
              - name: only
                steps:
                  - name: touch
                    type: sql
                    datasource: scratch
                    statements:
                      - "create table marker as select 1 as i"
            """.trimIndent(),
        )
        return directory
    }

    private fun wired(result: WiringResult): TaskAdmin = when (result) {
        is WiringResult.Wired -> result.admin
        is WiringResult.Invalid -> error("the wiring was rejected: ${result.report.errors}")
    }

    private fun invalid(result: WiringResult): ValidationReport = when (result) {
        is WiringResult.Wired -> error("the wiring was accepted, and this test needs it rejected")
        is WiringResult.Invalid -> result.report
    }

    /**
     * The hook obligation, end to end: the name the host registered survives validation *and*
     * resolves at run time, which is only true if the loader and the engine were handed the same
     * registry.
     */
    @Test
    fun aHookTheHostRegisteredValidatesAndThenActuallyRuns() {
        val ran = AtomicInteger()
        hooks.register("notify-downstream") { ran.incrementAndGet() }

        val admin = wired(wiring().start(taskDirectory("notify-downstream")))
        val runId = Trig.acceptedRunId(admin.trigger("wired-task", "tester"))
        Trig.awaitSucceeded(admin, "wired-task", runId)

        assertEquals(1, ran.get()) {
            "the file's onSuccess name passed validation but resolved to nothing at run time - " +
                "the loader and the engine are looking at different registries"
        }
    }

    /** The other half. Without it the test above passes for a loader given no hook set at all. */
    @Test
    fun aHookNobodyRegisteredIsRejectedAtStartup() {
        hooks.register("notify-downstream") { }

        val report = invalid(wiring().start(taskDirectory("notify-dowstream")))

        assertAll(
            { assertEquals(1, report.errors.size) { "one file, one wrong name: ${report.errors}" } },
            {
                assertTrue(report.errors.single().message.contains("notify-dowstream")) {
                    "the error must name the typo, not merely say something was wrong: " +
                        report.errors.single().message
                }
            },
        )
    }

    /**
     * The longest of the host's obligations: a host that builds definitions in code must call
     * `TaskScheduler.apply` itself, and missing it leaves `list()` reporting a task that will never
     * fire, with no error raised. `start(definitions)` calls it, so `TaskStatus.scheduled` - E14's
     * observable form of that disagreement - is true.
     */
    @Test
    fun theProgrammaticPathAppliesTheCronsItWasGiven() {
        val definition = P7Tasks.scheduled("nightly-roll", cron = "0 0 2 * * ?")

        val admin = wired(wiring().start(listOf(definition)))
        val status = admin.list().single()

        assertAll(
            { assertEquals("0 0 2 * * ?", status.cron) },
            {
                assertTrue(status.scheduled) {
                    "EtlWiring did not call TaskScheduler.apply, so this task is listed with a " +
                        "schedule and will never fire (spec 8.6)"
                }
            },
            { assertEquals(mapOf("nightly-roll" to "0 0 2 * * ?"), cron.registered) },
        )
    }

    /**
     * The failure shape of the programmatic path. `apply` runs before the `TaskAdmin` exists, so a
     * rejected batch leaves nothing registered and no half-wired admin holding definitions that
     * nothing will fire.
     */
    @Test
    fun aRejectedCronLeavesNothingWiredOrRegistered() {
        cron.rejectCron = { it == "not a cron" }

        val report = invalid(
            wiring().start(
                listOf(
                    P7Tasks.scheduled("good", cron = "0 0 2 * * ?"),
                    P7Tasks.scheduled("bad", cron = "not a cron"),
                ),
            ),
        )

        assertAll(
            { assertEquals(1, report.errors.size) { "one bad expression: ${report.errors}" } },
            { assertEquals("bad", report.errors.single().file) },
            {
                assertTrue(cron.registered.isEmpty()) {
                    "a rejected batch is rolled back whole (spec 8.5); still live: ${cron.registered}"
                }
            },
        )
    }
}
