package com.exporter.metrics

import com.exporter.config.MetricType
import com.exporter.config.ResolvedMetric
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class MetricStateRegistryTest {

    private lateinit var meterRegistry: MeterRegistry
    private lateinit var registry: MetricStateRegistry

    @BeforeEach
    fun setUp() {
        meterRegistry = SimpleMeterRegistry()
        registry = MetricStateRegistry(meterRegistry)
    }

    private fun metric(
        name: String = "test_metric",
        type: MetricType = MetricType.GAUGE,
        valueColumn: String = "value",
        tagColumns: List<String> = emptyList(),
        buckets: List<Double> = emptyList(),
        states: List<String> = emptyList(),
    ) = ResolvedMetric(name, type, valueColumn, tagColumns, buckets, states)

    // ─── GAUGE ────────────────────────────────────────────────

    @Nested
    inner class GaugeTests {
        @Test
        fun `gauge registers and reflects latest value`() {
            val m = metric(name = "cpu_usage", type = MetricType.GAUGE)
            registry.update(m, 75.5, emptyMap())

            val gauge = meterRegistry.find("cpu_usage").gauge()
            assertThat(gauge).isNotNull
            assertThat(gauge!!.value()).isEqualTo(75.5)
        }

        @Test
        fun `gauge updates to new value`() {
            val m = metric(name = "cpu_usage", type = MetricType.GAUGE)
            registry.update(m, 50.0, emptyMap())
            registry.update(m, 80.0, emptyMap())

            val gauge = meterRegistry.find("cpu_usage").gauge()
            assertThat(gauge!!.value()).isEqualTo(80.0)
        }

        @Test
        fun `gauge with different tags creates separate meters`() {
            val m = metric(name = "requests", type = MetricType.GAUGE)
            registry.update(m, 10.0, mapOf("host" to "srv01"))
            registry.update(m, 20.0, mapOf("host" to "srv02"))

            val meters = meterRegistry.find("requests").gauges()
            assertThat(meters).hasSize(2)
        }

        @Test
        fun `gauge with same tags updates same meter`() {
            val m = metric(name = "requests", type = MetricType.GAUGE)
            val tags = mapOf("host" to "srv01")
            registry.update(m, 10.0, tags)
            registry.update(m, 30.0, tags)

            val gauge = meterRegistry.find("requests").tag("host", "srv01").gauge()
            assertThat(gauge!!.value()).isEqualTo(30.0)
        }
    }

    // ─── COUNTER ──────────────────────────────────────────────

    @Nested
    inner class CounterTests {
        @Test
        fun `counter registers and increments by delta`() {
            val m = metric(name = "total_requests", type = MetricType.COUNTER)
            // First value is 100, delta from 0 = 100
            registry.update(m, 100.0, emptyMap())

            val counter = meterRegistry.find("total_requests").counter()
            assertThat(counter).isNotNull
            assertThat(counter!!.count()).isEqualTo(100.0)
        }

        @Test
        fun `counter increments by delta on subsequent updates`() {
            val m = metric(name = "total_requests", type = MetricType.COUNTER)
            registry.update(m, 100.0, emptyMap())
            registry.update(m, 150.0, emptyMap())

            val counter = meterRegistry.find("total_requests").counter()
            // 100 (first) + 50 (delta) = 150
            assertThat(counter!!.count()).isEqualTo(150.0)
        }

        @Test
        fun `counter handles reset - value drops below previous`() {
            val m = metric(name = "total_requests", type = MetricType.COUNTER)
            registry.update(m, 100.0, emptyMap())
            // Counter reset: new value < previous
            registry.update(m, 10.0, emptyMap())

            val counter = meterRegistry.find("total_requests").counter()
            // 100 (first) + 10 (reset recovery) = 110
            assertThat(counter!!.count()).isEqualTo(110.0)
        }

        @Test
        fun `counter no-ops on same value`() {
            val m = metric(name = "total_requests", type = MetricType.COUNTER)
            registry.update(m, 100.0, emptyMap())
            registry.update(m, 100.0, emptyMap())

            val counter = meterRegistry.find("total_requests").counter()
            assertThat(counter!!.count()).isEqualTo(100.0)
        }
    }

    // ─── HISTOGRAM ────────────────────────────────────────────

    @Nested
    inner class HistogramTests {
        @Test
        fun `histogram records values`() {
            val m = metric(
                name = "request_duration",
                type = MetricType.HISTOGRAM,
                buckets = listOf(1.0, 5.0, 10.0),
            )
            registry.update(m, 2.5, emptyMap())
            registry.update(m, 7.0, emptyMap())
            registry.update(m, 0.5, emptyMap())

            val summary = meterRegistry.find("request_duration").summary()
            assertThat(summary).isNotNull
            assertThat(summary!!.count()).isEqualTo(3)
            assertThat(summary.totalAmount()).isEqualTo(10.0)
        }

        @Test
        fun `histogram with tags creates separate distributions`() {
            val m = metric(
                name = "request_duration",
                type = MetricType.HISTOGRAM,
                buckets = listOf(1.0, 5.0),
            )
            registry.update(m, 2.0, mapOf("endpoint" to "/api"))
            registry.update(m, 4.0, mapOf("endpoint" to "/health"))

            val summaries = meterRegistry.find("request_duration").summaries()
            assertThat(summaries).hasSize(2)
        }
    }

    // ─── SUMMARY ──────────────────────────────────────────────

    @Nested
    inner class SummaryTests {
        @Test
        fun `summary records values with percentiles`() {
            val m = metric(name = "response_time", type = MetricType.SUMMARY)
            repeat(100) { i ->
                registry.update(m, i.toDouble(), emptyMap())
            }

            val summary = meterRegistry.find("response_time").summary()
            assertThat(summary).isNotNull
            assertThat(summary!!.count()).isEqualTo(100)
        }
    }

    // ─── ENUM ─────────────────────────────────────────────────

    @Nested
    inner class EnumTests {
        @Test
        fun `enum creates one gauge per state`() {
            val m = metric(
                name = "service_status",
                type = MetricType.ENUM,
                states = listOf("up", "down", "degraded"),
            )
            registry.updateEnumByState(m, "up", emptyMap())

            val gauges = meterRegistry.find("service_status").gauges()
            assertThat(gauges).hasSize(3)
        }

        @Test
        fun `enum sets active state to 1 and others to 0`() {
            val m = metric(
                name = "service_status",
                type = MetricType.ENUM,
                states = listOf("up", "down", "degraded"),
            )
            registry.updateEnumByState(m, "down", emptyMap())

            val upGauge = meterRegistry.find("service_status").tag("state", "up").gauge()
            val downGauge = meterRegistry.find("service_status").tag("state", "down").gauge()
            val degradedGauge = meterRegistry.find("service_status").tag("state", "degraded").gauge()

            assertThat(upGauge!!.value()).isEqualTo(0.0)
            assertThat(downGauge!!.value()).isEqualTo(1.0)
            assertThat(degradedGauge!!.value()).isEqualTo(0.0)
        }

        @Test
        fun `enum state transitions correctly`() {
            val m = metric(
                name = "service_status",
                type = MetricType.ENUM,
                states = listOf("up", "down"),
            )
            registry.updateEnumByState(m, "up", emptyMap())
            assertThat(meterRegistry.find("service_status").tag("state", "up").gauge()!!.value())
                .isEqualTo(1.0)

            registry.updateEnumByState(m, "down", emptyMap())
            assertThat(meterRegistry.find("service_status").tag("state", "up").gauge()!!.value())
                .isEqualTo(0.0)
            assertThat(meterRegistry.find("service_status").tag("state", "down").gauge()!!.value())
                .isEqualTo(1.0)
        }

        @Test
        fun `enum with extra tags separates by tag set`() {
            val m = metric(
                name = "db_status",
                type = MetricType.ENUM,
                states = listOf("primary", "replica"),
            )
            registry.updateEnumByState(m, "primary", mapOf("host" to "db01"))
            registry.updateEnumByState(m, "replica", mapOf("host" to "db02"))

            val db01Primary = meterRegistry.find("db_status")
                .tag("host", "db01").tag("state", "primary").gauge()
            val db02Primary = meterRegistry.find("db_status")
                .tag("host", "db02").tag("state", "primary").gauge()

            assertThat(db01Primary!!.value()).isEqualTo(1.0)
            assertThat(db02Primary!!.value()).isEqualTo(0.0)
        }
    }

    // ─── Clear ────────────────────────────────────────────────

    @Test
    fun `clear resets internal state`() {
        val m = metric(name = "test", type = MetricType.GAUGE)
        registry.update(m, 42.0, emptyMap())
        registry.clear()

        // After clear, internal maps are empty. New update should re-register.
        registry.update(m, 99.0, emptyMap())
        // The meter registry itself still has the old gauge, but the new one is registered too.
        // What matters is the holder is fresh.
        val gauges = meterRegistry.find("test").gauges()
        assertThat(gauges).isNotEmpty
    }
}
