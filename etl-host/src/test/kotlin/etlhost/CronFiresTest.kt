package etlhost

import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.junit.QuarkusTestProfile
import io.quarkus.test.junit.TestProfile
import jakarta.inject.Inject
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/** Fires every second, which is the fastest a Quartz expression can say "soon". */
private const val EVERY_SECOND = "0/1 * * * * ?"

/**
 * **SimpleEtl spec 8.6, row 1 - the row that is the reason this module exists.**
 *
 * "Set `quarkus.scheduler.start-mode=forced` in the application's `application.properties`;
 * symptom if missed: no task ever fires, and no error is raised." P7 could not test it and said so:
 * Quarkus does not read `application.properties` out of a dependency jar, so shipping the property
 * from the framework would have produced a green test for a production failure. The property has to
 * live in an application, and until now the reactor had no application.
 *
 * So this asserts the thing the row is about and nothing weaker: a cron registered through the
 * host's own [QuarkusCronScheduler], on a real Quarkus scheduler, **runs**.
 */
@QuarkusTest
@QuarkusTestResource(HostFixture::class)
class CronFiresTest {

    @Inject
    lateinit var cron: QuarkusCronScheduler

    @Test
    fun `a cron registered through the host binding actually fires`() {
        val fired = CountDownLatch(1)

        val registration = cron.schedule("probe-fires", EVERY_SECOND) { fired.countDown() }

        try {
            assertThat(fired.await(20, TimeUnit.SECONDS))
                .withFailMessage(
                    "nothing fired in 20s. Either quarkus.scheduler.start-mode is not forced, or the " +
                        "programmatic registration never reached a running scheduler - which is spec " +
                        "8.6 row 1's symptom exactly: no error, no log, no run.",
                )
                .isTrue()
        } finally {
            registration.close()
        }
    }

    /**
     * Row 5, and the reason it cannot be a field count. Validation rule 16 is structural only, so
     * this binding is the single place an expression meets a parser; if it accepts a bad one,
     * `TaskScheduler.apply` has nothing to roll back on and spec 8.5's atomic reload silently
     * accepts a cron that will never fire.
     *
     * `1-2-3` has the right number of tokens and is not a cron field, which is precisely what a
     * hand-rolled check waves through.
     */
    @Test
    fun `an unparseable cron is rejected by the binding, not accepted and forgotten`() {
        assertThrows<RuntimeException> {
            cron.schedule("probe-bad", "1-2-3 * * * * ?") { }
        }
    }

    /**
     * A cancelled registration stops firing. Not decoration: `TaskScheduler.apply` rolls a rejected
     * batch back by closing every registration it just made, and `WiringResult.Wired.close()` stops
     * the schedule at shutdown through the same handle. Both are only true if this works.
     */
    @Test
    fun `closing a registration stops it firing`() {
        val fired = CountDownLatch(1)
        cron.schedule("probe-cancel", EVERY_SECOND) { fired.countDown() }.close()

        assertThat(fired.await(3, TimeUnit.SECONDS)).isFalse()
    }
}

/** The scheduler registered but never triggered - Quarkus's own name for "nothing will fire". */
class HaltedScheduler : QuarkusTestProfile {
    override fun getConfigOverrides() = mapOf("quarkus.scheduler.start-mode" to "halted")
}

/**
 * The same registration, on a host whose scheduler start mode is wrong. **This is the symptom, not
 * the happy path**: it boots cleanly, registers cleanly, logs nothing, raises nothing, and never
 * runs. Spec 8.6's "no task ever fires, and no error is raised" is one assertion here.
 *
 * Its value is what it says about the test above. A green `CronFiresTest` on its own is evidence
 * that *something* fires; the pair is evidence that the property is what makes it fire, which is
 * the claim the row actually makes.
 */
@QuarkusTest
@QuarkusTestResource(HostFixture::class)
@TestProfile(HaltedScheduler::class)
class CronDoesNotFireWhenTheSchedulerIsHaltedTest {

    @Inject
    lateinit var cron: QuarkusCronScheduler

    @Test
    fun `a registered cron never fires and nothing complains`() {
        val fired = CountDownLatch(1)

        val registration = cron.schedule("probe-halted", EVERY_SECOND) { fired.countDown() }

        try {
            assertThat(fired.await(3, TimeUnit.SECONDS))
                .withFailMessage("it fired, so this test no longer discriminates on start-mode")
                .isFalse()
        } finally {
            registration.close()
        }
    }
}
