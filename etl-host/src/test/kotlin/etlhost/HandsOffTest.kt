package etlhost

import io.quarkus.test.common.WithTestResource
import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.junit.QuarkusTestProfile
import io.quarkus.test.junit.TestProfile
import jakarta.inject.Inject
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * A task directory of its own, holding one task that fires every second and touches nothing shared.
 *
 * Its own profile, so it gets its own Quarkus instance: a one-second cron pointed at the module's
 * shared task directory would run underneath every other test class for the rest of the run.
 */
class FastCronTasks : QuarkusTestProfile {
    override fun getConfigOverrides(): Map<String, String> {
        val directory = Files.createTempDirectory("etl-host-fastcron")
        Files.writeString(
            directory.resolve("heartbeat.yaml"),
            """
            name: heartbeat
            schedule:
              cron: "0/1 * * * * ?"
            phases:
              - name: beat
                steps:
                  - name: touch-scratch
                    type: materialize
                    datasource: scratch
                    output: beat
                    sql: select 1 as id
            """.trimIndent(),
        )
        return mapOf("etl-host.etl.task-directory" to directory.toString())
    }
}

/**
 * **SimpleEtl spec 8.6, row 2:** the host's `CronScheduler` must hand off to `TaskRunner` rather
 * than run inline. Symptom if missed: Vert.x blocked-thread warnings past 60 seconds, and a 5-30
 * minute ETL run pinned to a scheduler worker for its whole duration.
 *
 * The assertion is the property that breaks first and the one an operator would eventually see in a
 * stack dump: **the step body did not execute on the thread the scheduler fired on.** SimpleEtl
 * spec 8.3 puts every run on `Dispatchers.IO.limitedParallelism(1)`, so a correct hand-off lands on
 * a `DefaultDispatcher` worker; running inline would land on Quarkus's `executor-thread-N`.
 *
 * It is also the strongest form of row 1 available: not a probe registration this test made, but a
 * **task file's own cron**, loaded by `EtlWiring.start`, registered by `TaskScheduler`, fired by
 * Quarkus, and run by `TaskRunner` - the whole chain the deployment actually uses.
 */
@QuarkusTest
@WithTestResource(HostFixture::class)
@TestProfile(FastCronTasks::class)
class HandsOffTest {

    @Inject
    lateinit var listener: RecordingListener

    @Test
    fun `a scheduled task runs on its own dispatcher, not on the scheduler thread`() {
        val ended = listener.latch("heartbeat")

        assertThat(ended.await(20, TimeUnit.SECONDS))
            .withFailMessage("the task file's cron never fired in 20s")
            .isTrue()

        val threads = listener.stepThreads.map { it.second }.distinct()
        assertThat(threads).isNotEmpty()
        assertThat(threads).allSatisfy { thread ->
            assertThat(thread)
                .withFailMessage(
                    "the step ran on %s. A name from Quarkus's scheduler pool means the run happened " +
                        "inline on the firing thread, which is spec 8.6 row 2's symptom: a 30 minute " +
                        "run pins a worker and Vert.x starts warning at 60 seconds.",
                    thread,
                )
                .doesNotStartWith("executor-thread")
                .doesNotStartWith("vert.x-")
                .contains("DefaultDispatcher")
        }
    }
}
