package com.workflow.infrastructure.queryexporter

import com.workflow.infrastructure.queryexporter.config.ExporterConfig
import com.workflow.infrastructure.queryexporter.config.MetricConfig
import com.workflow.infrastructure.queryexporter.config.MetricType
import com.workflow.infrastructure.queryexporter.config.QueryConfig
import com.workflow.infrastructure.queryexporter.config.ScheduleConfig
import com.workflow.infrastructure.queryexporter.core.MetricWriter
import com.workflow.infrastructure.queryexporter.core.QueryExecutor
import com.workflow.infrastructure.queryexporter.core.QueryScheduler
import com.workflow.infrastructure.queryexporter.spi.DataSourceProvider
import com.workflow.infrastructure.queryexporter.spi.LeaderGuard
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.atLeast
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import javax.sql.DataSource
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class QuerySchedulerTest {

    private lateinit var executor: QueryExecutor
    private lateinit var registry: SimpleMeterRegistry
    private lateinit var writer: MetricWriter
    private lateinit var dataSource: DataSource
    private lateinit var dataSourceProvider: DataSourceProvider
    private val fixedClock: Clock = Clock.fixed(Instant.parse("2025-01-01T00:00:00Z"), ZoneOffset.UTC)

    @BeforeEach
    fun setUp() {
        executor = mock()
        registry = SimpleMeterRegistry()
        writer = MetricWriter(registry)
        dataSource = mock()
        dataSourceProvider = DataSourceProvider { dataSource }
    }

    @AfterEach
    fun tearDown() {
        writer.close()
        registry.close()
    }

    // -- Helpers ----------------------------------------------------------------

    private fun testLeaderGuard(initial: Boolean = true): MutableStateFlow<Boolean> =
        MutableStateFlow(initial)

    private fun leaderGuardOf(state: StateFlow<Boolean>): LeaderGuard = object : LeaderGuard {
        override val leaderState: StateFlow<Boolean> = state
    }

    private fun gaugeMetric(
        name: String = "test_gauge",
        valueColumn: String = "cnt",
    ) = MetricConfig(
        name = name,
        type = MetricType.GAUGE,
        valueColumn = valueColumn,
    )

    private fun queryConfig(
        sql: String = "SELECT count(*) AS cnt FROM tasks",
        datasource: String = "default",
        interval: Duration = Duration.ofSeconds(30),
        metrics: List<MetricConfig> = listOf(gaugeMetric()),
    ) = QueryConfig(
        sql = sql,
        datasource = datasource,
        schedule = ScheduleConfig(interval = interval),
        metrics = metrics,
    )

    private fun cronQueryConfig(
        sql: String = "SELECT count(*) AS cnt FROM tasks",
        datasource: String = "default",
        cron: String = "0 * * * *",
        metrics: List<MetricConfig> = listOf(gaugeMetric()),
    ) = QueryConfig(
        sql = sql,
        datasource = datasource,
        schedule = ScheduleConfig(cron = cron),
        metrics = metrics,
    )

    private fun config(vararg entries: Pair<String, QueryConfig>) =
        ExporterConfig(queries = mapOf(*entries))

    private fun createScheduler(
        leaderGuard: LeaderGuard = LeaderGuard.ALWAYS,
        clock: Clock = fixedClock,
    ) = QueryScheduler(
        executor = executor,
        writer = writer,
        dataSourceProvider = dataSourceProvider,
        leaderGuard = leaderGuard,
        clock = clock,
    )

    // ==========================================================================
    // A. Start/Stop lifecycle
    // ==========================================================================

    @Nested
    inner class Lifecycle {

        @Test
        fun `start then advance time past interval triggers executor`() = runTest {
            val queryRows = listOf(mapOf<String, Any?>("cnt" to 42.0))
            whenever(executor.execute(any(), any())).thenReturn(queryRows)

            val scheduler = createScheduler()
            val exporterConfig = config("q1" to queryConfig(interval = Duration.ofSeconds(10)))

            scheduler.start(exporterConfig, backgroundScope)
            runCurrent()
            advanceTimeBy(15_000)
            runCurrent()

            // Runs at t=0 (immediate) and t=10s (after first interval)
            verify(executor, atLeastOnce()).execute(eq(dataSource), eq("SELECT count(*) AS cnt FROM tasks"))
        }

        @Test
        fun `stop prevents further executor calls`() = runTest {
            val queryRows = listOf(mapOf<String, Any?>("cnt" to 1.0))
            whenever(executor.execute(any(), any())).thenReturn(queryRows)

            val scheduler = createScheduler()
            val exporterConfig = config("q1" to queryConfig(interval = Duration.ofSeconds(10)))

            scheduler.start(exporterConfig, backgroundScope)
            runCurrent()
            advanceTimeBy(15_000)
            runCurrent()

            // Capture call count before stop
            verify(executor, atLeastOnce()).execute(any(), any())
            val callsBeforeStop = org.mockito.Mockito.mockingDetails(executor).invocations.size

            scheduler.stop()
            runCurrent()
            advanceTimeBy(30_000)
            runCurrent()

            // After stop, no additional calls
            val callsAfterStop = org.mockito.Mockito.mockingDetails(executor).invocations.size
            assertTrue(callsAfterStop == callsBeforeStop, "No calls after stop")
        }

        @Test
        fun `stop with timeout completes gracefully`() = runTest {
            whenever(executor.execute(any(), any())).thenReturn(emptyList())

            val scheduler = createScheduler()
            val exporterConfig = config("q1" to queryConfig(interval = Duration.ofSeconds(10)))

            scheduler.start(exporterConfig, backgroundScope)
            runCurrent()
            advanceTimeBy(15_000)
            runCurrent()

            scheduler.stop(Duration.ofSeconds(5))
        }
    }

    // ==========================================================================
    // B. LeaderGuard
    // ==========================================================================

    @Nested
    inner class LeaderGuardBehavior {

        @Test
        fun `executor not called when leader guard state is false`() = runTest {
            val state = testLeaderGuard(initial = false)
            val scheduler = createScheduler(leaderGuard = leaderGuardOf(state))
            val exporterConfig = config("q1" to queryConfig(interval = Duration.ofSeconds(10)))

            scheduler.start(exporterConfig, backgroundScope)
            runCurrent()
            advanceTimeBy(30_000)
            runCurrent()

            verify(executor, never()).execute(any(), any())
        }

        @Test
        fun `follower to leader transition triggers executor`() = runTest {
            whenever(executor.execute(any(), any()))
                .thenReturn(listOf(mapOf<String, Any?>("cnt" to 1.0)))

            val state = testLeaderGuard(initial = false)
            val scheduler = createScheduler(leaderGuard = leaderGuardOf(state))
            val exporterConfig = config("q1" to queryConfig(interval = Duration.ofSeconds(10)))

            scheduler.start(exporterConfig, backgroundScope)
            runCurrent()
            advanceTimeBy(15_000)
            runCurrent()
            verify(executor, never()).execute(any(), any())

            // Become leader
            state.value = true
            runCurrent()
            advanceTimeBy(15_000)
            runCurrent()
            verify(executor, atLeastOnce()).execute(any(), any())
        }

        @Test
        fun `leader to follower transition stops executor calls`() = runTest {
            whenever(executor.execute(any(), any()))
                .thenReturn(listOf(mapOf<String, Any?>("cnt" to 1.0)))

            val state = testLeaderGuard(initial = true)
            val scheduler = createScheduler(leaderGuard = leaderGuardOf(state))
            val exporterConfig = config("q1" to queryConfig(interval = Duration.ofSeconds(10)))

            scheduler.start(exporterConfig, backgroundScope)
            runCurrent()
            advanceTimeBy(15_000)
            runCurrent()
            verify(executor, atLeastOnce()).execute(any(), any())
            val callsAsLeader = org.mockito.Mockito.mockingDetails(executor).invocations.size

            // Lose leadership
            state.value = false
            runCurrent()
            advanceTimeBy(30_000)
            runCurrent()

            val callsAfterLostLeadership = org.mockito.Mockito.mockingDetails(executor).invocations.size
            assertTrue(callsAfterLostLeadership == callsAsLeader, "No calls after losing leadership")
        }
    }

    // ==========================================================================
    // C. Error isolation
    // ==========================================================================

    @Nested
    inner class ErrorIsolation {

        @Test
        fun `error in one query does not affect another`() = runTest {
            val failingSql = "SELECT error FROM broken"
            val workingSql = "SELECT count(*) AS cnt FROM healthy"
            val workingRows = listOf(mapOf<String, Any?>("cnt" to 99.0))

            whenever(executor.execute(eq(dataSource), eq(failingSql)))
                .thenThrow(RuntimeException("SQL error"))
            whenever(executor.execute(eq(dataSource), eq(workingSql)))
                .thenReturn(workingRows)

            val scheduler = createScheduler()
            val exporterConfig = config(
                "failing_q" to queryConfig(
                    sql = failingSql,
                    interval = Duration.ofSeconds(10),
                    metrics = listOf(gaugeMetric(name = "fail_metric")),
                ),
                "working_q" to queryConfig(
                    sql = workingSql,
                    interval = Duration.ofSeconds(10),
                    metrics = listOf(gaugeMetric(name = "work_metric")),
                ),
            )

            scheduler.start(exporterConfig, backgroundScope)
            runCurrent()
            advanceTimeBy(15_000)
            runCurrent()

            // Working query should have been called despite failing query's error
            verify(executor, atLeastOnce()).execute(eq(dataSource), eq(workingSql))
        }

        @Test
        fun `query that throws continues to be retried on next interval`() = runTest {
            whenever(executor.execute(any(), any()))
                .thenThrow(RuntimeException("transient error"))
                .thenReturn(listOf(mapOf<String, Any?>("cnt" to 1.0)))

            val scheduler = createScheduler()
            val exporterConfig = config("q1" to queryConfig(interval = Duration.ofSeconds(10)))

            scheduler.start(exporterConfig, backgroundScope)
            runCurrent()

            // Advance enough for at least 2 executions (t=0 throws, t=10 succeeds)
            advanceTimeBy(15_000)
            runCurrent()

            verify(executor, atLeast(2)).execute(any(), any())
        }
    }

    // ==========================================================================
    // D. Cron scheduling
    // ==========================================================================

    @Nested
    inner class CronScheduling {

        @Test
        fun `cron-based query config is accepted by scheduler`() = runTest {
            whenever(executor.execute(any(), any()))
                .thenReturn(listOf(mapOf<String, Any?>("cnt" to 1.0)))

            val scheduler = createScheduler()
            val exporterConfig = config(
                "cron_q" to cronQueryConfig(cron = "0 * * * *"),
            )

            scheduler.start(exporterConfig, backgroundScope)
            runCurrent()
            advanceTimeBy(3_600_000 + 1_000)
            runCurrent()

            // Scheduler must not crash with a cron config
        }
    }

    // ==========================================================================
    // E. Idempotent start
    // ==========================================================================

    @Nested
    inner class IdempotentStart {

        @Test
        fun `calling start twice does not create duplicate query coroutines`() = runTest {
            whenever(executor.execute(any(), any()))
                .thenReturn(listOf(mapOf<String, Any?>("cnt" to 1.0)))

            val scheduler = createScheduler()
            val exporterConfig = config("q1" to queryConfig(interval = Duration.ofSeconds(10)))

            scheduler.start(exporterConfig, backgroundScope)
            scheduler.start(exporterConfig, backgroundScope)
            runCurrent()

            advanceTimeBy(15_000)
            runCurrent()

            // With 10s interval, t=0 and t=10 = exactly 2 calls (not 4 from duplicate start)
            verify(executor, atLeast(1)).execute(any(), any())
            val totalCalls = org.mockito.Mockito.mockingDetails(executor).invocations.size
            assertTrue(totalCalls <= 2, "Expected at most 2 calls (no duplicates), got $totalCalls")
        }
    }

    // ==========================================================================
    // E2. Restart after stop
    // ==========================================================================

    @Nested
    inner class RestartAfterStop {

        @Test
        fun `scheduler can be restarted after stop`() = runTest {
            whenever(executor.execute(any(), any()))
                .thenReturn(listOf(mapOf<String, Any?>("cnt" to 1.0)))

            val scheduler = createScheduler()
            val exporterConfig = config("q1" to queryConfig(interval = Duration.ofSeconds(10)))

            scheduler.start(exporterConfig, backgroundScope)
            runCurrent()
            advanceTimeBy(15_000)
            runCurrent()
            val callsFirstRun = org.mockito.Mockito.mockingDetails(executor).invocations.size
            assertTrue(callsFirstRun > 0, "Should have executed at least once")

            scheduler.stop()
            runCurrent()

            // Restart
            scheduler.start(exporterConfig, backgroundScope)
            runCurrent()
            advanceTimeBy(15_000)
            runCurrent()
            val callsAfterRestart = org.mockito.Mockito.mockingDetails(executor).invocations.size
            assertTrue(callsAfterRestart > callsFirstRun, "Should have new executions after restart")
        }
    }

    // ==========================================================================
    // F. Multiple queries with different intervals
    // ==========================================================================

    @Nested
    inner class MultipleQueries {

        @Test
        fun `queries with different intervals execute at their own cadence`() = runTest {
            val fastSql = "SELECT 1 AS cnt"
            val slowSql = "SELECT 2 AS cnt"

            whenever(executor.execute(eq(dataSource), eq(fastSql)))
                .thenReturn(listOf(mapOf<String, Any?>("cnt" to 1.0)))
            whenever(executor.execute(eq(dataSource), eq(slowSql)))
                .thenReturn(listOf(mapOf<String, Any?>("cnt" to 2.0)))

            val scheduler = createScheduler()
            val exporterConfig = config(
                "fast" to queryConfig(
                    sql = fastSql,
                    interval = Duration.ofSeconds(10),
                    metrics = listOf(gaugeMetric(name = "fast_metric")),
                ),
                "slow" to queryConfig(
                    sql = slowSql,
                    interval = Duration.ofSeconds(30),
                    metrics = listOf(gaugeMetric(name = "slow_metric")),
                ),
            )

            scheduler.start(exporterConfig, backgroundScope)
            runCurrent()

            // After 35s: fast fires at t=0,10,20,30 (4x), slow fires at t=0,30 (2x)
            advanceTimeBy(35_000)
            runCurrent()

            // Fast query should have more invocations than slow query
            verify(executor, atLeast(3)).execute(eq(dataSource), eq(fastSql))
            verify(executor, atLeast(1)).execute(eq(dataSource), eq(slowSql))
        }
    }

    // ==========================================================================
    // G. DataSourceProvider resolution
    // ==========================================================================

    @Nested
    inner class DataSourceResolution {

        @Test
        fun `datasource name from config is passed to provider`() = runTest {
            val mockProvider = mock<DataSourceProvider>()
            val mockDs = mock<DataSource>()
            whenever(mockProvider.resolve("custom_ds")).thenReturn(mockDs)
            whenever(executor.execute(eq(mockDs), any()))
                .thenReturn(listOf(mapOf<String, Any?>("cnt" to 1.0)))

            val scheduler = QueryScheduler(
                executor = executor,
                writer = writer,
                dataSourceProvider = mockProvider,
                leaderGuard = LeaderGuard.ALWAYS,
                clock = fixedClock,
            )

            val exporterConfig = config(
                "q1" to queryConfig(datasource = "custom_ds"),
            )

            scheduler.start(exporterConfig, backgroundScope)
            runCurrent()
            advanceTimeBy(35_000)
            runCurrent()

            verify(mockProvider).resolve("custom_ds")
        }
    }

    // ==========================================================================
    // H. Integration: executor -> writer pipeline
    // ==========================================================================

    @Nested
    inner class ExecutorWriterPipeline {

        @Test
        fun `executor results are written to registry via writer`() = runTest {
            val queryRows = listOf(mapOf<String, Any?>("cnt" to 42.0))
            whenever(executor.execute(any(), any())).thenReturn(queryRows)

            val scheduler = createScheduler()
            val exporterConfig = config(
                "q1" to queryConfig(
                    metrics = listOf(gaugeMetric(name = "pipeline_gauge", valueColumn = "cnt")),
                ),
            )

            scheduler.start(exporterConfig, backgroundScope)
            runCurrent()
            advanceTimeBy(35_000)
            runCurrent()

            val gauge = registry.find("pipeline_gauge").gauge()
            assertNotNull(gauge)
            assertTrue(gauge.value() == 42.0, "Gauge should reflect executor result")
        }
    }
}
