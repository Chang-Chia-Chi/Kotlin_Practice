package com.exporter.engine

import com.exporter.config.MetricType
import com.exporter.config.ResolvedMetric
import com.exporter.config.ResolvedQuery
import com.exporter.config.ResolvedSchedule
import com.exporter.db.QueryExecutor
import com.exporter.metrics.MetricStateRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.*
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration

class QueryJobTest {

    private lateinit var queryExecutor: QueryExecutor
    private lateinit var metricRegistry: MetricStateRegistry
    private lateinit var meterRegistry: SimpleMeterRegistry

    @BeforeEach
    fun setUp() {
        queryExecutor = mockk()
        meterRegistry = SimpleMeterRegistry()
        metricRegistry = MetricStateRegistry(meterRegistry)
    }

    private fun query(
        name: String = "test_query",
        sql: String = "SELECT 1 as value",
        datasource: String = "default",
        metrics: List<ResolvedMetric> = listOf(
            ResolvedMetric("test_gauge", MetricType.GAUGE, "value", emptyList(), emptyList(), emptyList())
        ),
    ) = ResolvedQuery(
        name = name,
        sql = sql,
        datasource = datasource,
        schedule = ResolvedSchedule(interval = Duration.ofSeconds(5), cron = null),
        metrics = metrics,
    )

    // ─── Happy path ───────────────────────────────────────────

    @Nested
    inner class HappyPath {
        @Test
        fun `executes query and updates gauge metric`() = runTest {
            val q = query()
            every { queryExecutor.execute("default", q.sql) } returns listOf(
                mapOf("value" to 42)
            )

            val job = QueryJob(q, queryExecutor, metricRegistry)
            val rowCount = job.execute()

            assertThat(rowCount).isEqualTo(1)
            val gauge = meterRegistry.find("test_gauge").gauge()
            assertThat(gauge).isNotNull
            assertThat(gauge!!.value()).isEqualTo(42.0)
        }

        @Test
        fun `processes multiple rows`() = runTest {
            val q = query()
            every { queryExecutor.execute("default", q.sql) } returns listOf(
                mapOf("value" to 10),
                mapOf("value" to 20),
                mapOf("value" to 30),
            )

            val job = QueryJob(q, queryExecutor, metricRegistry)
            val rowCount = job.execute()

            assertThat(rowCount).isEqualTo(3)
            // Last gauge value wins
            val gauge = meterRegistry.find("test_gauge").gauge()
            assertThat(gauge!!.value()).isEqualTo(30.0)
        }

        @Test
        fun `processes multiple metrics per query`() = runTest {
            val metrics = listOf(
                ResolvedMetric("gauge_a", MetricType.GAUGE, "val_a", emptyList(), emptyList(), emptyList()),
                ResolvedMetric("gauge_b", MetricType.GAUGE, "val_b", emptyList(), emptyList(), emptyList()),
            )
            val q = query(metrics = metrics)
            every { queryExecutor.execute(any(), any()) } returns listOf(
                mapOf("val_a" to 100, "val_b" to 200)
            )

            val job = QueryJob(q, queryExecutor, metricRegistry)
            job.execute()

            assertThat(meterRegistry.find("gauge_a").gauge()!!.value()).isEqualTo(100.0)
            assertThat(meterRegistry.find("gauge_b").gauge()!!.value()).isEqualTo(200.0)
        }

        @Test
        fun `processes gauge with tags`() = runTest {
            val metrics = listOf(
                ResolvedMetric("tagged_gauge", MetricType.GAUGE, "value",
                    listOf("host"), emptyList(), emptyList())
            )
            val q = query(metrics = metrics)
            every { queryExecutor.execute(any(), any()) } returns listOf(
                mapOf("value" to 10, "host" to "srv01"),
                mapOf("value" to 20, "host" to "srv02"),
            )

            val job = QueryJob(q, queryExecutor, metricRegistry)
            job.execute()

            assertThat(meterRegistry.find("tagged_gauge").tag("host", "srv01").gauge()!!.value())
                .isEqualTo(10.0)
            assertThat(meterRegistry.find("tagged_gauge").tag("host", "srv02").gauge()!!.value())
                .isEqualTo(20.0)
        }

        @Test
        fun `processes counter metric`() = runTest {
            val metrics = listOf(
                ResolvedMetric("request_count", MetricType.COUNTER, "cnt",
                    emptyList(), emptyList(), emptyList())
            )
            val q = query(metrics = metrics)
            every { queryExecutor.execute(any(), any()) } returns listOf(
                mapOf("cnt" to 500)
            )

            val job = QueryJob(q, queryExecutor, metricRegistry)
            job.execute()

            assertThat(meterRegistry.find("request_count").counter()!!.count()).isEqualTo(500.0)
        }

        @Test
        fun `processes histogram metric`() = runTest {
            val metrics = listOf(
                ResolvedMetric("latency", MetricType.HISTOGRAM, "duration_ms",
                    emptyList(), listOf(10.0, 50.0, 100.0), emptyList())
            )
            val q = query(metrics = metrics)
            every { queryExecutor.execute(any(), any()) } returns listOf(
                mapOf("duration_ms" to 15),
                mapOf("duration_ms" to 75),
                mapOf("duration_ms" to 5),
            )

            val job = QueryJob(q, queryExecutor, metricRegistry)
            job.execute()

            val summary = meterRegistry.find("latency").summary()!!
            assertThat(summary.count()).isEqualTo(3)
            assertThat(summary.totalAmount()).isEqualTo(95.0)
        }

        @Test
        fun `processes enum metric`() = runTest {
            val metrics = listOf(
                ResolvedMetric("status", MetricType.ENUM, "state",
                    emptyList(), emptyList(), listOf("up", "down"))
            )
            val q = query(metrics = metrics)
            every { queryExecutor.execute(any(), any()) } returns listOf(
                mapOf("state" to "up")
            )

            val job = QueryJob(q, queryExecutor, metricRegistry)
            job.execute()

            assertThat(meterRegistry.find("status").tag("state", "up").gauge()!!.value())
                .isEqualTo(1.0)
            assertThat(meterRegistry.find("status").tag("state", "down").gauge()!!.value())
                .isEqualTo(0.0)
        }
    }

    // ─── Edge cases ───────────────────────────────────────────

    @Nested
    inner class EdgeCases {
        @Test
        fun `empty result set returns 0 rows`() = runTest {
            val q = query()
            every { queryExecutor.execute(any(), any()) } returns emptyList()

            val job = QueryJob(q, queryExecutor, metricRegistry)
            val rowCount = job.execute()

            assertThat(rowCount).isEqualTo(0)
        }

        @Test
        fun `query exception is caught and returns 0`() = runTest {
            val q = query()
            every { queryExecutor.execute(any(), any()) } throws RuntimeException("DB down")

            val job = QueryJob(q, queryExecutor, metricRegistry)
            val rowCount = job.execute()

            assertThat(rowCount).isEqualTo(0)
        }

        @Test
        fun `null value column is skipped gracefully`() = runTest {
            val q = query()
            every { queryExecutor.execute(any(), any()) } returns listOf(
                mapOf("value" to null),
                mapOf("value" to 42),
            )

            val job = QueryJob(q, queryExecutor, metricRegistry)
            val rowCount = job.execute()

            assertThat(rowCount).isEqualTo(2) // rows processed, even if value is null
            val gauge = meterRegistry.find("test_gauge").gauge()
            assertThat(gauge!!.value()).isEqualTo(42.0)
        }

        @Test
        fun `missing value column is skipped gracefully`() = runTest {
            val q = query()
            every { queryExecutor.execute(any(), any()) } returns listOf(
                mapOf("wrong_column" to 42)
            )

            val job = QueryJob(q, queryExecutor, metricRegistry)
            val rowCount = job.execute()

            assertThat(rowCount).isEqualTo(1)
            // No gauge should be created since value extraction failed
            val gauge = meterRegistry.find("test_gauge").gauge()
            assertThat(gauge).isNull()
        }
    }
}
