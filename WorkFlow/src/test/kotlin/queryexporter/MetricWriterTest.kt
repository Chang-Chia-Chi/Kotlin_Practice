package com.workflow.queryexporter

import com.workflow.queryexporter.config.MetricConfig
import com.workflow.queryexporter.config.MetricType
import com.workflow.queryexporter.core.MetricWriter
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class MetricWriterTest {

    private lateinit var registry: SimpleMeterRegistry
    private lateinit var writer: MetricWriter

    @BeforeEach
    fun setUp() {
        registry = SimpleMeterRegistry()
        writer = MetricWriter(registry)
    }

    @AfterEach
    fun tearDown() {
        writer.close()
        registry.close()
    }

    // -- Helpers ----------------------------------------------------------------

    private fun gaugeMetric(
        name: String = "test_gauge",
        valueColumn: String = "value",
        tagColumns: List<String> = emptyList(),
    ) = MetricConfig(
        name = name,
        type = MetricType.GAUGE,
        valueColumn = valueColumn,
        tagColumns = tagColumns,
    )

    // ==========================================================================
    // A. GAUGE happy paths
    // ==========================================================================

    @Nested
    inner class GaugeHappyPath {

        @Test
        fun `write rows with value column registers gauge with correct value`() {
            val metric = gaugeMetric()
            val rows = listOf(mapOf<String, Any?>("value" to 42.0))

            writer.write(metric, rows)

            val gauge = registry.find("test_gauge").gauge()
            assertNotNull(gauge)
            assertEquals(42.0, gauge.value())
        }

        @Test
        fun `write rows with Int value converts correctly`() {
            val metric = gaugeMetric()
            val rows = listOf(mapOf<String, Any?>("value" to 7))

            writer.write(metric, rows)

            val gauge = registry.find("test_gauge").gauge()
            assertNotNull(gauge)
            assertEquals(7.0, gauge.value())
        }

        @Test
        fun `write rows with Long value converts correctly`() {
            val metric = gaugeMetric()
            val rows = listOf(mapOf<String, Any?>("value" to 123456789L))

            writer.write(metric, rows)

            val gauge = registry.find("test_gauge").gauge()
            assertNotNull(gauge)
            assertEquals(123456789.0, gauge.value())
        }

        @Test
        fun `write rows with Double value converts correctly`() {
            val metric = gaugeMetric()
            val rows = listOf(mapOf<String, Any?>("value" to 3.14))

            writer.write(metric, rows)

            val gauge = registry.find("test_gauge").gauge()
            assertNotNull(gauge)
            assertEquals(3.14, gauge.value())
        }
    }

    // ==========================================================================
    // B. GAUGE with tags
    // ==========================================================================

    @Nested
    inner class GaugeWithTags {

        @Test
        fun `write rows with tag columns registers gauge with correct tags`() {
            val metric = gaugeMetric(tagColumns = listOf("status"))
            val rows = listOf(
                mapOf<String, Any?>("value" to 10.0, "status" to "PENDING"),
            )

            writer.write(metric, rows)

            val gauge = registry.find("test_gauge").tag("status", "PENDING").gauge()
            assertNotNull(gauge)
            assertEquals(10.0, gauge.value())
        }

        @Test
        fun `write rows with multiple tag columns registers gauge with all tags`() {
            val metric = gaugeMetric(tagColumns = listOf("region", "env"))
            val rows = listOf(
                mapOf<String, Any?>("value" to 5.0, "region" to "us-east", "env" to "prod"),
            )

            writer.write(metric, rows)

            val gauge = registry.find("test_gauge")
                .tag("region", "us-east")
                .tag("env", "prod")
                .gauge()
            assertNotNull(gauge)
            assertEquals(5.0, gauge.value())
        }

        @Test
        fun `write multiple rows with different tag combos creates separate gauges`() {
            val metric = gaugeMetric(tagColumns = listOf("status"))
            val rows = listOf(
                mapOf<String, Any?>("value" to 10.0, "status" to "PENDING"),
                mapOf<String, Any?>("value" to 20.0, "status" to "COMPLETED"),
            )

            writer.write(metric, rows)

            val pendingGauge = registry.find("test_gauge").tag("status", "PENDING").gauge()
            val completedGauge = registry.find("test_gauge").tag("status", "COMPLETED").gauge()
            assertNotNull(pendingGauge)
            assertNotNull(completedGauge)
            assertEquals(10.0, pendingGauge.value())
            assertEquals(20.0, completedGauge.value())
        }
    }

    // ==========================================================================
    // C. GAUGE update behavior
    // ==========================================================================

    @Nested
    inner class GaugeUpdate {

        @Test
        fun `write rows twice with different values reflects latest value`() {
            val metric = gaugeMetric()

            writer.write(metric, listOf(mapOf<String, Any?>("value" to 10.0)))

            val gauge1 = registry.find("test_gauge").gauge()
            assertNotNull(gauge1)
            assertEquals(10.0, gauge1.value())

            writer.write(metric, listOf(mapOf<String, Any?>("value" to 25.0)))

            val gauge2 = registry.find("test_gauge").gauge()
            assertNotNull(gauge2)
            assertEquals(25.0, gauge2.value())
        }

        @Test
        fun `write rows with tag combo then write without it resets absent combo to zero`() {
            val metric = gaugeMetric(tagColumns = listOf("status"))

            // First write: both PENDING and COMPLETED present
            writer.write(metric, listOf(
                mapOf<String, Any?>("value" to 10.0, "status" to "PENDING"),
                mapOf<String, Any?>("value" to 20.0, "status" to "COMPLETED"),
            ))

            val pendingBefore = registry.find("test_gauge").tag("status", "PENDING").gauge()
            assertNotNull(pendingBefore)
            assertEquals(10.0, pendingBefore.value())

            // Second write: only COMPLETED present; PENDING should be reset to 0
            writer.write(metric, listOf(
                mapOf<String, Any?>("value" to 30.0, "status" to "COMPLETED"),
            ))

            val pendingAfter = registry.find("test_gauge").tag("status", "PENDING").gauge()
            assertNotNull(pendingAfter)
            assertEquals(0.0, pendingAfter.value(), "Stale tag combo should be reset to 0")

            val completedAfter = registry.find("test_gauge").tag("status", "COMPLETED").gauge()
            assertNotNull(completedAfter)
            assertEquals(30.0, completedAfter.value())
        }
    }

    // ==========================================================================
    // D. Multiple independent metrics
    // ==========================================================================

    @Nested
    inner class MultipleMetrics {

        @Test
        fun `write with different metric configs creates independent gauges`() {
            val metricA = gaugeMetric(name = "metric_a", valueColumn = "val_a")
            val metricB = gaugeMetric(name = "metric_b", valueColumn = "val_b")

            writer.write(metricA, listOf(mapOf<String, Any?>("val_a" to 100.0)))
            writer.write(metricB, listOf(mapOf<String, Any?>("val_b" to 200.0)))

            val gaugeA = registry.find("metric_a").gauge()
            val gaugeB = registry.find("metric_b").gauge()
            assertNotNull(gaugeA)
            assertNotNull(gaugeB)
            assertEquals(100.0, gaugeA.value())
            assertEquals(200.0, gaugeB.value())
        }

        @Test
        fun `updating one metric does not affect another`() {
            val metricA = gaugeMetric(name = "metric_a", valueColumn = "val_a")
            val metricB = gaugeMetric(name = "metric_b", valueColumn = "val_b")

            writer.write(metricA, listOf(mapOf<String, Any?>("val_a" to 10.0)))
            writer.write(metricB, listOf(mapOf<String, Any?>("val_b" to 20.0)))

            // Update only metricA
            writer.write(metricA, listOf(mapOf<String, Any?>("val_a" to 50.0)))

            assertEquals(50.0, registry.find("metric_a").gauge()!!.value())
            assertEquals(20.0, registry.find("metric_b").gauge()!!.value(), "metric_b should be unchanged")
        }
    }

    // ==========================================================================
    // E. Unsupported metric types
    // ==========================================================================

    @Nested
    inner class UnsupportedTypes {

        @Test
        fun `write with COUNTER type throws UnsupportedOperationException`() {
            val metric = MetricConfig(
                name = "counter_metric",
                type = MetricType.COUNTER,
                valueColumn = "cnt",
            )

            assertThrows<UnsupportedOperationException> {
                writer.write(metric, listOf(mapOf("cnt" to 1.0)))
            }
        }

        @Test
        fun `write with HISTOGRAM type throws UnsupportedOperationException`() {
            val metric = MetricConfig(
                name = "hist_metric",
                type = MetricType.HISTOGRAM,
                valueColumn = "ms",
                buckets = listOf(10.0, 50.0),
            )

            assertThrows<UnsupportedOperationException> {
                writer.write(metric, listOf(mapOf("ms" to 42.0)))
            }
        }

        @Test
        fun `write with SUMMARY type throws UnsupportedOperationException`() {
            val metric = MetricConfig(
                name = "summary_metric",
                type = MetricType.SUMMARY,
                valueColumn = "ms",
            )

            assertThrows<UnsupportedOperationException> {
                writer.write(metric, listOf(mapOf("ms" to 42.0)))
            }
        }

        @Test
        fun `write with ENUM type throws UnsupportedOperationException`() {
            val metric = MetricConfig(
                name = "enum_metric",
                type = MetricType.ENUM,
                valueColumn = "state",
                states = listOf("up", "down"),
            )

            assertThrows<UnsupportedOperationException> {
                writer.write(metric, listOf(mapOf("state" to "up")))
            }
        }
    }

    // ==========================================================================
    // F. close() behavior
    // ==========================================================================

    @Nested
    inner class CloseLifecycle {

        @Test
        fun `close removes all meters registered by this writer`() {
            val metricA = gaugeMetric(name = "close_test_a")
            val metricB = gaugeMetric(name = "close_test_b")

            writer.write(metricA, listOf(mapOf<String, Any?>("value" to 1.0)))
            writer.write(metricB, listOf(mapOf<String, Any?>("value" to 2.0)))

            assertNotNull(registry.find("close_test_a").gauge())
            assertNotNull(registry.find("close_test_b").gauge())

            writer.close()

            val metersAfterClose = registry.meters.filter {
                it.id.name in listOf("close_test_a", "close_test_b")
            }
            assertTrue(metersAfterClose.isEmpty(), "All writer meters should be removed after close()")
        }

        @Test
        fun `close with tagged gauges removes all tag combinations`() {
            val metric = gaugeMetric(name = "close_tagged", tagColumns = listOf("env"))

            writer.write(metric, listOf(
                mapOf<String, Any?>("value" to 1.0, "env" to "prod"),
                mapOf<String, Any?>("value" to 2.0, "env" to "staging"),
            ))

            assertNotNull(registry.find("close_tagged").tag("env", "prod").gauge())
            assertNotNull(registry.find("close_tagged").tag("env", "staging").gauge())

            writer.close()

            val remaining = registry.meters.filter { it.id.name == "close_tagged" }
            assertTrue(remaining.isEmpty(), "Tagged gauges should be removed after close()")
        }
    }

    // ==========================================================================
    // G. Edge cases: numeric value extraction
    // ==========================================================================

    @Nested
    inner class NumericExtraction {

        @Test
        fun `write with empty rows does not register gauge`() {
            val metric = gaugeMetric()

            writer.write(metric, emptyList())

            val gauge = registry.find("test_gauge").gauge()
            assertTrue(gauge == null, "No gauge should be registered for empty rows")
        }

        @Test
        fun `write multiple rows without tags uses last row value`() {
            val metric = gaugeMetric()
            val rows = listOf(
                mapOf<String, Any?>("value" to 1.0),
                mapOf<String, Any?>("value" to 2.0),
                mapOf<String, Any?>("value" to 3.0),
            )

            writer.write(metric, rows)

            val gauge = registry.find("test_gauge").gauge()
            assertNotNull(gauge)
            assertEquals(3.0, gauge.value())
        }

        @Test
        fun `write row with Float value converts correctly`() {
            val metric = gaugeMetric()
            val rows = listOf(mapOf<String, Any?>("value" to 2.5f))

            writer.write(metric, rows)

            val gauge = registry.find("test_gauge").gauge()
            assertNotNull(gauge)
            assertEquals(2.5, gauge.value(), 0.001)
        }

        @Test
        fun `write row with null value column produces gauge at zero`() {
            val metric = gaugeMetric()
            val rows = listOf(mapOf<String, Any?>("value" to null))

            writer.write(metric, rows)

            val gauge = registry.find("test_gauge").gauge()
            assertNotNull(gauge)
            assertEquals(0.0, gauge.value())
        }

        @Test
        fun `write row with parseable String value converts correctly`() {
            val metric = gaugeMetric()
            val rows = listOf(mapOf<String, Any?>("value" to "42.5"))

            writer.write(metric, rows)

            val gauge = registry.find("test_gauge").gauge()
            assertNotNull(gauge)
            assertEquals(42.5, gauge.value())
        }

        @Test
        fun `write row with non-parseable String value produces gauge at zero`() {
            val metric = gaugeMetric()
            val rows = listOf(mapOf<String, Any?>("value" to "not_a_number"))

            writer.write(metric, rows)

            val gauge = registry.find("test_gauge").gauge()
            assertNotNull(gauge)
            assertEquals(0.0, gauge.value())
        }

        @Test
        fun `write row with non-numeric non-String type produces gauge at zero`() {
            val metric = gaugeMetric()
            val rows = listOf(mapOf<String, Any?>("value" to true))

            writer.write(metric, rows)

            val gauge = registry.find("test_gauge").gauge()
            assertNotNull(gauge)
            assertEquals(0.0, gauge.value())
        }
    }
}
