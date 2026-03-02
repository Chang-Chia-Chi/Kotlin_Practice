package com.exporter.validation

import com.exporter.config.ExporterConfig
import com.exporter.config.MetricType
import com.exporter.db.DataSourceResolver
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*
import javax.sql.DataSource

class ConfigValidatorTest {

    private lateinit var dsResolver: DataSourceResolver
    private lateinit var validator: ConfigValidator
    private val mockDataSource = mockk<DataSource>()

    @BeforeEach
    fun setUp() {
        dsResolver = mockk()
        // By default, "default" and "monitoring" exist
        every { dsResolver.resolve("default") } returns mockDataSource
        every { dsResolver.resolve("monitoring") } returns mockDataSource
        every { dsResolver.resolve(match { it != "default" && it != "monitoring" }) } returns null

        validator = ConfigValidator(dsResolver)
    }

    // ─── Helper builders ───────────────────────────────────────

    private fun buildConfig(
        queries: Map<String, ExporterConfig.QueryConfig> = emptyMap(),
    ): ExporterConfig = mockk {
        every { queries() } returns queries
    }

    private fun buildQuery(
        sql: String = "SELECT 1 as value",
        datasource: String = "default",
        interval: String? = "5s",
        cron: String? = null,
        metrics: List<ExporterConfig.MetricConfig> = listOf(gaugeMetric()),
    ): ExporterConfig.QueryConfig {
        val schedule = mockk<ExporterConfig.ScheduleConfig> {
            every { interval() } returns Optional.ofNullable(interval)
            every { cron() } returns Optional.ofNullable(cron)
        }
        return mockk {
            every { sql() } returns sql
            every { datasource() } returns datasource
            every { schedule() } returns schedule
            every { metrics() } returns metrics
        }
    }

    private fun gaugeMetric(
        name: String = "test_gauge",
        valueColumn: String = "value",
        tagColumns: List<String> = emptyList(),
    ): ExporterConfig.MetricConfig = mockk {
        every { name() } returns name
        every { type() } returns MetricType.GAUGE
        every { valueColumn() } returns valueColumn
        every { tagColumns() } returns Optional.of(tagColumns)
        every { buckets() } returns Optional.empty()
        every { states() } returns Optional.empty()
    }

    private fun histogramMetric(
        name: String = "test_histogram",
        buckets: List<Double> = listOf(1.0, 5.0, 10.0),
    ): ExporterConfig.MetricConfig = mockk {
        every { name() } returns name
        every { type() } returns MetricType.HISTOGRAM
        every { valueColumn() } returns "value"
        every { tagColumns() } returns Optional.of(emptyList())
        every { buckets() } returns Optional.of(buckets)
        every { states() } returns Optional.empty()
    }

    private fun enumMetric(
        name: String = "test_enum",
        states: List<String> = listOf("active", "inactive"),
    ): ExporterConfig.MetricConfig = mockk {
        every { name() } returns name
        every { type() } returns MetricType.ENUM
        every { valueColumn() } returns "status"
        every { tagColumns() } returns Optional.of(emptyList())
        every { buckets() } returns Optional.empty()
        every { states() } returns Optional.of(states)
    }

    // ─── Happy path ───────────────────────────────────────────

    @Test
    fun `valid config with single gauge query resolves successfully`() {
        val config = buildConfig(mapOf("q1" to buildQuery()))
        val result = validator.validate(config)

        assertThat(result).hasSize(1)
        assertThat(result[0].name).isEqualTo("q1")
        assertThat(result[0].sql).isEqualTo("SELECT 1 as value")
        assertThat(result[0].datasource).isEqualTo("default")
        assertThat(result[0].schedule.interval).isEqualTo(Duration.ofSeconds(5))
        assertThat(result[0].schedule.cron).isNull()
        assertThat(result[0].metrics).hasSize(1)
        assertThat(result[0].metrics[0].type).isEqualTo(MetricType.GAUGE)
    }

    @Test
    fun `valid config with cron schedule resolves successfully`() {
        val config = buildConfig(mapOf(
            "cron_q" to buildQuery(interval = null, cron = "0 0/5 * * * ?")
        ))
        val result = validator.validate(config)

        assertThat(result).hasSize(1)
        assertThat(result[0].schedule.interval).isNull()
        assertThat(result[0].schedule.cron).isEqualTo("0 0/5 * * * ?")
    }

    @Test
    fun `valid config with histogram and buckets resolves successfully`() {
        val config = buildConfig(mapOf(
            "hist_q" to buildQuery(metrics = listOf(histogramMetric()))
        ))
        val result = validator.validate(config)

        assertThat(result[0].metrics[0].type).isEqualTo(MetricType.HISTOGRAM)
        assertThat(result[0].metrics[0].buckets).containsExactly(1.0, 5.0, 10.0)
    }

    @Test
    fun `valid config with enum and states resolves successfully`() {
        val config = buildConfig(mapOf(
            "enum_q" to buildQuery(metrics = listOf(enumMetric()))
        ))
        val result = validator.validate(config)

        assertThat(result[0].metrics[0].type).isEqualTo(MetricType.ENUM)
        assertThat(result[0].metrics[0].states).containsExactly("active", "inactive")
    }

    @Test
    fun `multiple queries all valid`() {
        val config = buildConfig(mapOf(
            "q1" to buildQuery(sql = "SELECT 1 as value"),
            "q2" to buildQuery(sql = "SELECT 2 as value", datasource = "monitoring"),
        ))
        val result = validator.validate(config)
        assertThat(result).hasSize(2)
    }

    @Test
    fun `metric with tag columns resolves correctly`() {
        val config = buildConfig(mapOf(
            "q1" to buildQuery(metrics = listOf(gaugeMetric(tagColumns = listOf("host", "env"))))
        ))
        val result = validator.validate(config)
        assertThat(result[0].metrics[0].tagColumns).containsExactly("host", "env")
    }

    // ─── Failure: Empty queries ───────────────────────────────

    @Nested
    inner class EmptyConfig {
        @Test
        fun `no queries defined throws validation exception`() {
            val config = buildConfig(emptyMap())

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e -> e.contains("No queries defined") }
                })
        }
    }

    // ─── Failure: SQL ─────────────────────────────────────────

    @Nested
    inner class SqlValidation {
        @Test
        fun `empty SQL string fails`() {
            val config = buildConfig(mapOf("bad" to buildQuery(sql = "")))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e -> e.contains("empty SQL") }
                })
        }

        @Test
        fun `blank SQL string fails`() {
            val config = buildConfig(mapOf("bad" to buildQuery(sql = "   ")))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e -> e.contains("empty SQL") }
                })
        }

        @Test
        fun `SQL with semicolon fails`() {
            val config = buildConfig(mapOf(
                "bad" to buildQuery(sql = "SELECT 1; DROP TABLE users")
            ))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e -> e.contains("semicolon") }
                })
        }
    }

    // ─── Failure: Datasource ──────────────────────────────────

    @Nested
    inner class DatasourceValidation {
        @Test
        fun `unknown datasource fails`() {
            val config = buildConfig(mapOf("bad" to buildQuery(datasource = "nonexistent")))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e ->
                        e.contains("nonexistent") && e.contains("not found")
                    }
                })
        }
    }

    // ─── Failure: Schedule ────────────────────────────────────

    @Nested
    inner class ScheduleValidation {
        @Test
        fun `both interval and cron set fails`() {
            val config = buildConfig(mapOf(
                "bad" to buildQuery(interval = "5s", cron = "0 * * * * ?")
            ))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e -> e.contains("ambiguous schedule") }
                })
        }

        @Test
        fun `neither interval nor cron set fails`() {
            val config = buildConfig(mapOf(
                "bad" to buildQuery(interval = null, cron = null)
            ))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e -> e.contains("no schedule") }
                })
        }

        @Test
        fun `invalid interval string fails`() {
            val config = buildConfig(mapOf(
                "bad" to buildQuery(interval = "notaduration")
            ))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e -> e.contains("invalid interval") }
                })
        }

        @Test
        fun `zero interval fails`() {
            val config = buildConfig(mapOf(
                "bad" to buildQuery(interval = "0s")
            ))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e -> e.contains("invalid interval") }
                })
        }
    }

    // ─── Failure: Metric type constraints ─────────────────────

    @Nested
    inner class MetricTypeValidation {
        @Test
        fun `histogram without buckets fails`() {
            val config = buildConfig(mapOf(
                "bad" to buildQuery(metrics = listOf(histogramMetric(buckets = emptyList())))
            ))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e -> e.contains("missing buckets") }
                })
        }

        @Test
        fun `enum without states fails`() {
            val config = buildConfig(mapOf(
                "bad" to buildQuery(metrics = listOf(enumMetric(states = emptyList())))
            ))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e -> e.contains("missing states") }
                })
        }

        @Test
        fun `no metrics defined fails`() {
            val config = buildConfig(mapOf(
                "bad" to buildQuery(metrics = emptyList())
            ))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e -> e.contains("no metrics") }
                })
        }
    }

    // ─── Failure: Column overlap ──────────────────────────────

    @Nested
    inner class ColumnValidation {
        @Test
        fun `valueColumn in tagColumns fails`() {
            val metric = gaugeMetric(valueColumn = "value", tagColumns = listOf("host", "value"))
            val config = buildConfig(mapOf(
                "bad" to buildQuery(metrics = listOf(metric))
            ))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e ->
                        e.contains("cannot be both value and tag")
                    }
                })
        }

        @Test
        fun `valueColumn in tagColumns case-insensitive fails`() {
            val metric = gaugeMetric(valueColumn = "Value", tagColumns = listOf("host", "value"))
            val config = buildConfig(mapOf(
                "bad" to buildQuery(metrics = listOf(metric))
            ))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e ->
                        e.contains("cannot be both value and tag")
                    }
                })
        }

        @Test
        fun `empty metric name fails`() {
            val metric = gaugeMetric(name = "")
            val config = buildConfig(mapOf(
                "bad" to buildQuery(metrics = listOf(metric))
            ))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e -> e.contains("empty name") }
                })
        }

        @Test
        fun `invalid Prometheus metric name fails`() {
            val metric = gaugeMetric(name = "123-invalid!")
            val config = buildConfig(mapOf(
                "bad" to buildQuery(metrics = listOf(metric))
            ))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e -> e.contains("Prometheus format") }
                })
        }

        @Test
        fun `metric name starting with digit fails`() {
            val metric = gaugeMetric(name = "9cpu_usage")
            val config = buildConfig(mapOf(
                "bad" to buildQuery(metrics = listOf(metric))
            ))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e -> e.contains("Prometheus format") }
                })
        }

        @Test
        fun `metric name with hyphen fails`() {
            val metric = gaugeMetric(name = "cpu-usage")
            val config = buildConfig(mapOf(
                "bad" to buildQuery(metrics = listOf(metric))
            ))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    assertThat(ex.errors).anyMatch { e -> e.contains("Prometheus format") }
                })
        }

        @Test
        fun `valid Prometheus metric names pass validation`() {
            // Names with underscores, colons, and mixed case should all pass
            val metric1 = gaugeMetric(name = "cpu_usage")
            val metric2 = gaugeMetric(name = "_private_metric")
            val metric3 = gaugeMetric(name = "namespace:metric_name")

            val config = buildConfig(mapOf(
                "q1" to buildQuery(metrics = listOf(metric1)),
                "q2" to buildQuery(metrics = listOf(metric2)),
                "q3" to buildQuery(metrics = listOf(metric3)),
            ))

            val result = validator.validate(config)
            assertThat(result).hasSize(3)
        }
    }

    // ─── Error accumulation ───────────────────────────────────

    @Nested
    inner class ErrorAccumulation {
        @Test
        fun `multiple errors are collected in single exception`() {
            val config = buildConfig(mapOf(
                "bad1" to buildQuery(sql = "", datasource = "nonexistent"),
                "bad2" to buildQuery(interval = null, cron = null),
            ))

            assertThatThrownBy { validator.validate(config) }
                .isInstanceOf(ConfigValidationException::class.java)
                .satisfies({
                    val ex = it as ConfigValidationException
                    // At least: empty SQL, unknown datasource, no schedule
                    assertThat(ex.errors.size).isGreaterThanOrEqualTo(3)
                })
        }
    }

    // ─── Duration parsing ─────────────────────────────────────

    @Nested
    inner class DurationParsing {
        @Test
        fun `parses seconds`() {
            assertThat(ConfigValidator.parseDuration("5s")).isEqualTo(Duration.ofSeconds(5))
        }

        @Test
        fun `parses minutes`() {
            assertThat(ConfigValidator.parseDuration("2m")).isEqualTo(Duration.ofMinutes(2))
        }

        @Test
        fun `parses hours`() {
            assertThat(ConfigValidator.parseDuration("1h")).isEqualTo(Duration.ofHours(1))
        }

        @Test
        fun `parses milliseconds`() {
            assertThat(ConfigValidator.parseDuration("500ms")).isEqualTo(Duration.ofMillis(500))
        }

        @Test
        fun `parses days`() {
            assertThat(ConfigValidator.parseDuration("1d")).isEqualTo(Duration.ofDays(1))
        }

        @Test
        fun `returns null for garbage`() {
            assertThat(ConfigValidator.parseDuration("not-a-duration")).isNull()
        }

        @Test
        fun `handles whitespace`() {
            assertThat(ConfigValidator.parseDuration("  10s  ")).isEqualTo(Duration.ofSeconds(10))
        }
    }
}
