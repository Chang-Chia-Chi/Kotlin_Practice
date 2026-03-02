package com.exporter.engine

import com.exporter.config.ExporterConfig
import com.exporter.config.MetricType
import com.exporter.config.ResolvedMetric
import com.exporter.config.ResolvedQuery
import com.exporter.config.ResolvedSchedule
import com.exporter.db.QueryExecutor
import com.exporter.metrics.MetricStateRegistry
import com.exporter.validation.ConfigValidationException
import com.exporter.validation.ConfigValidator
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.*
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import io.quarkus.scheduler.ScheduledExecution
import io.quarkus.scheduler.Scheduler
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

class ExecutionEngineTest {

    private lateinit var config: ExporterConfig
    private lateinit var validator: ConfigValidator
    private lateinit var scheduler: Scheduler
    private lateinit var queryExecutor: QueryExecutor
    private lateinit var metricRegistry: MetricStateRegistry
    private lateinit var engine: ExecutionEngine

    // Mock builder chain for Scheduler
    private lateinit var jobDefinition: Scheduler.JobDefinition<*>

    @BeforeEach
    fun setUp() {
        config = mockk()
        validator = mockk()
        queryExecutor = mockk()
        metricRegistry = MetricStateRegistry(SimpleMeterRegistry())

        // Set up Scheduler mock chain — use relaxed mock to handle the recursive generic type
        jobDefinition = mockk<Scheduler.JobDefinition<*>>(relaxed = true)
        every { jobDefinition.setTask(any<Consumer<ScheduledExecution>>()) } returns jobDefinition
        every { jobDefinition.setInterval(any<String>()) } returns jobDefinition
        every { jobDefinition.setCron(any()) } returns jobDefinition
        every { jobDefinition.schedule() } returns mockk()

        scheduler = mockk()
        every { scheduler.newJob(any()) } returns jobDefinition

        engine = ExecutionEngine(config, validator, scheduler, queryExecutor, metricRegistry)
    }

    private fun resolvedQuery(
        name: String = "test_query",
        interval: Duration = Duration.ofSeconds(5),
    ) = ResolvedQuery(
        name = name,
        sql = "SELECT 1 as value",
        datasource = "default",
        schedule = ResolvedSchedule(interval = interval, cron = null),
        metrics = listOf(
            ResolvedMetric("test_metric", MetricType.GAUGE, "value",
                emptyList(), emptyList(), emptyList())
        ),
    )

    @Test
    fun `startup validates config and registers jobs`() {
        val queries = listOf(resolvedQuery("q1"), resolvedQuery("q2"))
        every { validator.validate(config) } returns queries

        engine.onStart(StartupEvent())

        assertThat(engine.getResolvedQueries()).hasSize(2)
        verify(exactly = 2) { scheduler.newJob(any()) }
        verify(exactly = 2) { jobDefinition.setInterval(any<String>()) }
        verify(exactly = 2) { jobDefinition.schedule() }
    }

    @Test
    fun `startup with cron schedule sets cron on job`() {
        val query = ResolvedQuery(
            name = "cron_query",
            sql = "SELECT 1",
            datasource = "default",
            schedule = ResolvedSchedule(interval = null, cron = "0 0/5 * * * ?"),
            metrics = listOf(
                ResolvedMetric("m", MetricType.GAUGE, "value",
                    emptyList(), emptyList(), emptyList())
            ),
        )
        every { validator.validate(config) } returns listOf(query)

        engine.onStart(StartupEvent())

        verify { jobDefinition.setCron("0 0/5 * * * ?") }
        verify(exactly = 0) { jobDefinition.setInterval(any<String>()) }
    }

    @Test
    fun `startup propagates validation exception`() {
        every { validator.validate(config) } throws
            ConfigValidationException(listOf("Bad config"))

        assertThatThrownBy { engine.onStart(StartupEvent()) }
            .isInstanceOf(ConfigValidationException::class.java)
    }

    @Test
    fun `startup with empty valid config registers no jobs`() {
        every { validator.validate(config) } returns emptyList()

        engine.onStart(StartupEvent())

        assertThat(engine.getResolvedQueries()).isEmpty()
        verify(exactly = 0) { scheduler.newJob(any()) }
    }

    @Test
    fun `resolved queries are empty before startup`() {
        assertThat(engine.getResolvedQueries()).isEmpty()
    }

    @Test
    fun `overlap protection skips duplicate execution`() {
        // Capture the task consumer when setTask is called
        var taskConsumer: Consumer<ScheduledExecution>? = null
        every { jobDefinition.setTask(any<Consumer<ScheduledExecution>>()) } answers {
            taskConsumer = firstArg()
            jobDefinition
        }

        val latch = CountDownLatch(1)
        val blockingLatch = CountDownLatch(1)

        // Make the query executor block so the first execution stays "running"
        every { queryExecutor.execute(any(), any()) } answers {
            blockingLatch.countDown() // Signal that we're executing
            latch.await(5, TimeUnit.SECONDS) // Block until released
            listOf(mapOf("value" to 42 as Any?))
        }

        val queries = listOf(resolvedQuery("blocking_query"))
        every { validator.validate(config) } returns queries

        engine.onStart(StartupEvent())

        // Trigger first execution — it will block inside the query
        val execution = mockk<ScheduledExecution>(relaxed = true)
        taskConsumer!!.accept(execution)

        // Wait for the first execution to be inside the query
        blockingLatch.await(5, TimeUnit.SECONDS)

        // Trigger second execution — should be skipped due to overlap
        taskConsumer!!.accept(execution)

        // Release the blocking execution
        latch.countDown()

        // Give coroutine time to finish
        Thread.sleep(100)

        // QueryExecutor should only have been called once (second was skipped)
        verify(exactly = 1) { queryExecutor.execute(any(), any()) }
    }

    @Test
    fun `onStop cancels coroutine scope`() {
        every { validator.validate(config) } returns emptyList()
        engine.onStart(StartupEvent())

        // Should not throw
        engine.onStop(ShutdownEvent())
    }

    @Test
    fun `onStop before onStart does not throw`() {
        // Scope is not initialized yet — should handle gracefully
        engine.onStop(ShutdownEvent())
    }
}
