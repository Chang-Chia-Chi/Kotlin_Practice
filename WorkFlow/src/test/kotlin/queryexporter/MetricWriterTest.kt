package com.workflow.queryexporter

import com.workflow.queryexporter.config.MetricConfig
import com.workflow.queryexporter.config.MetricType
import com.workflow.queryexporter.core.MetricWriter
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

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

    private fun counterMetric(
        name: String = "test_counter",
        valueColumn: String = "value",
        tagColumns: List<String> = emptyList(),
    ) = MetricConfig(
        name = name,
        type = MetricType.COUNTER,
        valueColumn = valueColumn,
        tagColumns = tagColumns,
    )

    private fun histogramMetric(
        name: String = "test_histogram",
        valueColumn: String = "value",
        tagColumns: List<String> = emptyList(),
        buckets: List<Double> = listOf(10.0, 50.0, 100.0),
    ) = MetricConfig(
        name = name,
        type = MetricType.HISTOGRAM,
        valueColumn = valueColumn,
        tagColumns = tagColumns,
        buckets = buckets,
    )

    private fun summaryMetric(
        name: String = "test_summary",
        valueColumn: String = "value",
        tagColumns: List<String> = emptyList(),
    ) = MetricConfig(
        name = name,
        type = MetricType.SUMMARY,
        valueColumn = valueColumn,
        tagColumns = tagColumns,
    )

    private fun enumMetric(
        name: String = "test_enum",
        valueColumn: String = "state",
        tagColumns: List<String> = emptyList(),
        states: List<String> = listOf("up", "down", "degraded"),
    ) = MetricConfig(
        name = name,
        type = MetricType.ENUM,
        valueColumn = valueColumn,
        tagColumns = tagColumns,
        states = states,
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
    // E. COUNTER tests
    // ==========================================================================

    @Nested
    inner class CounterHappyPath {

        @Test
        fun `single row writes absolute value as function counter`() {
            val metric = counterMetric()
            val rows = listOf(mapOf<String, Any?>("value" to 42.0))

            writer.write(metric, rows)

            val counter = registry.find("test_counter").functionCounter()
            assertNotNull(counter)
            assertEquals(42.0, counter.count())
        }

        @Test
        fun `two writes with increasing values reflects latest absolute value`() {
            val metric = counterMetric()

            writer.write(metric, listOf(mapOf<String, Any?>("value" to 10.0)))
            val counter1 = registry.find("test_counter").functionCounter()
            assertNotNull(counter1)
            assertEquals(10.0, counter1.count())

            writer.write(metric, listOf(mapOf<String, Any?>("value" to 25.0)))
            val counter2 = registry.find("test_counter").functionCounter()
            assertNotNull(counter2)
            assertEquals(25.0, counter2.count())
        }

        @Test
        fun `value with tags creates tagged function counter`() {
            val metric = counterMetric(tagColumns = listOf("status"))
            val rows = listOf(
                mapOf<String, Any?>("value" to 15.0, "status" to "success"),
            )

            writer.write(metric, rows)

            val counter = registry.find("test_counter").tag("status", "success").functionCounter()
            assertNotNull(counter)
            assertEquals(15.0, counter.count())
        }

        @Test
        fun `multiple rows same tags last row wins`() {
            val metric = counterMetric(tagColumns = listOf("region"))
            val rows = listOf(
                mapOf<String, Any?>("value" to 10.0, "region" to "us"),
                mapOf<String, Any?>("value" to 20.0, "region" to "us"),
                mapOf<String, Any?>("value" to 30.0, "region" to "us"),
            )

            writer.write(metric, rows)

            val counter = registry.find("test_counter").tag("region", "us").functionCounter()
            assertNotNull(counter)
            assertEquals(30.0, counter.count())
        }

        @Test
        fun `null value produces counter at zero`() {
            val metric = counterMetric()
            val rows = listOf(mapOf<String, Any?>("value" to null))

            writer.write(metric, rows)

            val counter = registry.find("test_counter").functionCounter()
            assertNotNull(counter)
            assertEquals(0.0, counter.count())
        }

        @Test
        fun `non-numeric value produces counter at zero`() {
            val metric = counterMetric()
            val rows = listOf(mapOf<String, Any?>("value" to "not_a_number"))

            writer.write(metric, rows)

            val counter = registry.find("test_counter").functionCounter()
            assertNotNull(counter)
            assertEquals(0.0, counter.count())
        }

        @Test
        fun `no stale tag cleanup - absent tag combo retains previous value`() {
            val metric = counterMetric(tagColumns = listOf("status"))

            // First write: both success and failure present
            writer.write(metric, listOf(
                mapOf<String, Any?>("value" to 10.0, "status" to "success"),
                mapOf<String, Any?>("value" to 5.0, "status" to "failure"),
            ))

            val successBefore = registry.find("test_counter").tag("status", "success").functionCounter()
            assertNotNull(successBefore)
            assertEquals(10.0, successBefore.count())

            // Second write: only success present; failure should NOT be zeroed
            writer.write(metric, listOf(
                mapOf<String, Any?>("value" to 20.0, "status" to "success"),
            ))

            val failureAfter = registry.find("test_counter").tag("status", "failure").functionCounter()
            assertNotNull(failureAfter, "Counter for absent tag combo should still exist")
            assertEquals(5.0, failureAfter.count(), "Counter should retain previous value, not zero")
        }

        @Test
        fun `close removes function counters from registry`() {
            val metric = counterMetric(name = "close_counter")
            writer.write(metric, listOf(mapOf<String, Any?>("value" to 42.0)))

            assertNotNull(registry.find("close_counter").functionCounter())

            writer.close()

            val remaining = registry.meters.filter { it.id.name == "close_counter" }
            assertTrue(remaining.isEmpty(), "Function counters should be removed after close()")
        }
    }

    // ==========================================================================
    // F. HISTOGRAM tests
    // ==========================================================================

    @Nested
    inner class HistogramHappyPath {

        @Test
        fun `single row recorded as observation`() {
            val metric = histogramMetric()
            val rows = listOf(mapOf<String, Any?>("value" to 42.0))

            writer.write(metric, rows)

            val summary = registry.find("test_histogram").summary()
            assertNotNull(summary)
            assertEquals(1, summary.count())
            assertEquals(42.0, summary.totalAmount())
        }

        @Test
        fun `multiple rows recorded as multiple observations`() {
            val metric = histogramMetric()
            val rows = listOf(
                mapOf<String, Any?>("value" to 10.0),
                mapOf<String, Any?>("value" to 20.0),
                mapOf<String, Any?>("value" to 30.0),
            )

            writer.write(metric, rows)

            val summary = registry.find("test_histogram").summary()
            assertNotNull(summary)
            assertEquals(3, summary.count())
            assertEquals(60.0, summary.totalAmount())
        }

        @Test
        fun `buckets from MetricConfig applied to distribution`() {
            val metric = histogramMetric(buckets = listOf(10.0, 50.0, 100.0))
            val rows = listOf(
                mapOf<String, Any?>("value" to 5.0),
                mapOf<String, Any?>("value" to 25.0),
                mapOf<String, Any?>("value" to 75.0),
            )

            writer.write(metric, rows)

            val summary = registry.find("test_histogram").summary()
            assertNotNull(summary)
            val snapshot = summary.takeSnapshot()
            val histogramCounts = snapshot.histogramCounts()
            assertTrue(histogramCounts.isNotEmpty(), "Histogram should have bucket counts")
        }

        @Test
        fun `tags create separate distribution summaries per tag combo`() {
            val metric = histogramMetric(tagColumns = listOf("endpoint"))
            val rows = listOf(
                mapOf<String, Any?>("value" to 10.0, "endpoint" to "/api/a"),
                mapOf<String, Any?>("value" to 50.0, "endpoint" to "/api/b"),
            )

            writer.write(metric, rows)

            val summaryA = registry.find("test_histogram").tag("endpoint", "/api/a").summary()
            val summaryB = registry.find("test_histogram").tag("endpoint", "/api/b").summary()
            assertNotNull(summaryA)
            assertNotNull(summaryB)
            assertEquals(1, summaryA.count())
            assertEquals(10.0, summaryA.totalAmount())
            assertEquals(1, summaryB.count())
            assertEquals(50.0, summaryB.totalAmount())
        }

        @Test
        fun `empty rows produce no observations`() {
            val metric = histogramMetric()

            writer.write(metric, emptyList())

            val summary = registry.find("test_histogram").summary()
            // Either no meter registered, or count stays at 0
            assertTrue(summary == null || summary.count() == 0L,
                "No observations should be recorded for empty rows")
        }

        @Test
        fun `two write cycles accumulate observations`() {
            val metric = histogramMetric()

            writer.write(metric, listOf(mapOf<String, Any?>("value" to 10.0)))
            writer.write(metric, listOf(
                mapOf<String, Any?>("value" to 20.0),
                mapOf<String, Any?>("value" to 30.0),
            ))

            val summary = registry.find("test_histogram").summary()
            assertNotNull(summary)
            assertEquals(3, summary.count(), "Observations should accumulate across write cycles")
            assertEquals(60.0, summary.totalAmount())
        }

        @Test
        fun `close removes distribution summaries from registry`() {
            val metric = histogramMetric(name = "close_hist")
            writer.write(metric, listOf(mapOf<String, Any?>("value" to 42.0)))

            assertNotNull(registry.find("close_hist").summary())

            writer.close()

            val remaining = registry.meters.filter { it.id.name == "close_hist" }
            assertTrue(remaining.isEmpty(), "Distribution summaries should be removed after close()")
        }
    }

    // ==========================================================================
    // G. SUMMARY tests
    // ==========================================================================

    @Nested
    inner class SummaryHappyPath {

        @Test
        fun `single row recorded with count 1 and totalAmount equal to value`() {
            val metric = summaryMetric()
            val rows = listOf(mapOf<String, Any?>("value" to 42.0))

            writer.write(metric, rows)

            val summary = registry.find("test_summary").summary()
            assertNotNull(summary)
            assertEquals(1, summary.count())
            assertEquals(42.0, summary.totalAmount())
        }

        @Test
        fun `multiple rows produce correct count and total`() {
            val metric = summaryMetric()
            val rows = listOf(
                mapOf<String, Any?>("value" to 10.0),
                mapOf<String, Any?>("value" to 20.0),
                mapOf<String, Any?>("value" to 30.0),
                mapOf<String, Any?>("value" to 40.0),
            )

            writer.write(metric, rows)

            val summary = registry.find("test_summary").summary()
            assertNotNull(summary)
            assertEquals(4, summary.count())
            assertEquals(100.0, summary.totalAmount())
        }

        @Test
        fun `percentiles published in snapshot`() {
            val metric = summaryMetric()
            // Record enough observations for percentile computation
            val rows = (1..100).map { mapOf<String, Any?>("value" to it.toDouble()) }

            writer.write(metric, rows)

            val summary = registry.find("test_summary").summary()
            assertNotNull(summary)
            val snapshot = summary.takeSnapshot()
            val percentiles = snapshot.percentileValues()
            assertTrue(percentiles.isNotEmpty(), "Summary should publish percentile values")

            val publishedPercentiles = percentiles.map { it.percentile() }.toSet()
            // Expect at least the standard percentiles (p50, p90, p95, p99)
            assertTrue(publishedPercentiles.contains(0.5), "Should publish p50")
            assertTrue(publishedPercentiles.contains(0.9), "Should publish p90")
            assertTrue(publishedPercentiles.contains(0.95), "Should publish p95")
            assertTrue(publishedPercentiles.contains(0.99), "Should publish p99")
        }

        @Test
        fun `tags create separate distribution summaries per tag combo`() {
            val metric = summaryMetric(tagColumns = listOf("operation"))
            val rows = listOf(
                mapOf<String, Any?>("value" to 5.0, "operation" to "read"),
                mapOf<String, Any?>("value" to 15.0, "operation" to "write"),
            )

            writer.write(metric, rows)

            val readSummary = registry.find("test_summary").tag("operation", "read").summary()
            val writeSummary = registry.find("test_summary").tag("operation", "write").summary()
            assertNotNull(readSummary)
            assertNotNull(writeSummary)
            assertEquals(1, readSummary.count())
            assertEquals(5.0, readSummary.totalAmount())
            assertEquals(1, writeSummary.count())
            assertEquals(15.0, writeSummary.totalAmount())
        }

        @Test
        fun `close removes summaries from registry`() {
            val metric = summaryMetric(name = "close_summary")
            writer.write(metric, listOf(mapOf<String, Any?>("value" to 42.0)))

            assertNotNull(registry.find("close_summary").summary())

            writer.close()

            val remaining = registry.meters.filter { it.id.name == "close_summary" }
            assertTrue(remaining.isEmpty(), "Summaries should be removed after close()")
        }
    }

    // ==========================================================================
    // H. ENUM tests
    // ==========================================================================

    @Nested
    inner class EnumHappyPath {

        @Test
        fun `single state write sets current state gauge to 1 and others to 0`() {
            val metric = enumMetric()
            val rows = listOf(mapOf<String, Any?>("state" to "up"))

            writer.write(metric, rows)

            val upGauge = registry.find("test_enum").tag("state", "up").gauge()
            val downGauge = registry.find("test_enum").tag("state", "down").gauge()
            val degradedGauge = registry.find("test_enum").tag("state", "degraded").gauge()
            assertNotNull(upGauge, "Active state gauge should exist")
            assertNotNull(downGauge, "Inactive state gauge should exist")
            assertNotNull(degradedGauge, "Inactive state gauge should exist")
            assertEquals(1.0, upGauge.value(), "Active state should be 1.0")
            assertEquals(0.0, downGauge.value(), "Inactive state should be 0.0")
            assertEquals(0.0, degradedGauge.value(), "Inactive state should be 0.0")
        }

        @Test
        fun `state change updates gauges correctly`() {
            val metric = enumMetric()

            // Initial state: "up"
            writer.write(metric, listOf(mapOf<String, Any?>("state" to "up")))

            val upBefore = registry.find("test_enum").tag("state", "up").gauge()
            assertNotNull(upBefore)
            assertEquals(1.0, upBefore.value())

            // State change to "down"
            writer.write(metric, listOf(mapOf<String, Any?>("state" to "down")))

            val upAfter = registry.find("test_enum").tag("state", "up").gauge()
            val downAfter = registry.find("test_enum").tag("state", "down").gauge()
            assertNotNull(upAfter)
            assertNotNull(downAfter)
            assertEquals(0.0, upAfter.value(), "Previous state should become 0.0")
            assertEquals(1.0, downAfter.value(), "New state should become 1.0")
        }

        @Test
        fun `unknown state not in states list sets all state gauges to 0`() {
            val metric = enumMetric()
            val rows = listOf(mapOf<String, Any?>("state" to "unknown_state"))

            writer.write(metric, rows)

            val upGauge = registry.find("test_enum").tag("state", "up").gauge()
            val downGauge = registry.find("test_enum").tag("state", "down").gauge()
            val degradedGauge = registry.find("test_enum").tag("state", "degraded").gauge()
            assertNotNull(upGauge)
            assertNotNull(downGauge)
            assertNotNull(degradedGauge)
            assertEquals(0.0, upGauge.value(), "All states should be 0.0 for unknown state")
            assertEquals(0.0, downGauge.value(), "All states should be 0.0 for unknown state")
            assertEquals(0.0, degradedGauge.value(), "All states should be 0.0 for unknown state")
        }

        @Test
        fun `tags create separate gauge sets per tag combo`() {
            val metric = enumMetric(tagColumns = listOf("region"))
            val rows = listOf(
                mapOf<String, Any?>("state" to "up", "region" to "us"),
                mapOf<String, Any?>("state" to "down", "region" to "eu"),
            )

            writer.write(metric, rows)

            // US region: up=1, down=0
            val usUp = registry.find("test_enum")
                .tag("region", "us").tag("state", "up").gauge()
            val usDown = registry.find("test_enum")
                .tag("region", "us").tag("state", "down").gauge()
            assertNotNull(usUp)
            assertNotNull(usDown)
            assertEquals(1.0, usUp.value(), "US region should have up=1.0")
            assertEquals(0.0, usDown.value(), "US region should have down=0.0")

            // EU region: up=0, down=1
            val euUp = registry.find("test_enum")
                .tag("region", "eu").tag("state", "up").gauge()
            val euDown = registry.find("test_enum")
                .tag("region", "eu").tag("state", "down").gauge()
            assertNotNull(euUp)
            assertNotNull(euDown)
            assertEquals(0.0, euUp.value(), "EU region should have up=0.0")
            assertEquals(1.0, euDown.value(), "EU region should have down=1.0")
        }

        @Test
        fun `empty rows set all states to 0 via stale cleanup`() {
            val metric = enumMetric()

            // First write to establish gauges
            writer.write(metric, listOf(mapOf<String, Any?>("state" to "up")))
            assertEquals(1.0, registry.find("test_enum").tag("state", "up").gauge()!!.value())

            // Empty write should zero all
            writer.write(metric, emptyList())

            val upGauge = registry.find("test_enum").tag("state", "up").gauge()
            val downGauge = registry.find("test_enum").tag("state", "down").gauge()
            val degradedGauge = registry.find("test_enum").tag("state", "degraded").gauge()
            assertNotNull(upGauge)
            assertNotNull(downGauge)
            assertNotNull(degradedGauge)
            assertEquals(0.0, upGauge.value(), "All states should be 0.0 after empty write")
            assertEquals(0.0, downGauge.value(), "All states should be 0.0 after empty write")
            assertEquals(0.0, degradedGauge.value(), "All states should be 0.0 after empty write")
        }

        @Test
        fun `close removes all enum gauges from registry`() {
            val metric = enumMetric(name = "close_enum")
            writer.write(metric, listOf(mapOf<String, Any?>("state" to "up")))

            // Verify gauges exist before close
            assertNotNull(registry.find("close_enum").tag("state", "up").gauge())

            writer.close()

            val remaining = registry.meters.filter { it.id.name == "close_enum" }
            assertTrue(remaining.isEmpty(), "All enum gauges should be removed after close()")
        }
    }

    // ==========================================================================
    // I. close() behavior
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
    // J. Edge cases: numeric value extraction
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
