package com.workflow.infrastructure.queryexporter

import com.workflow.infrastructure.queryexporter.config.ExporterConfig
import com.workflow.infrastructure.queryexporter.config.ExporterConfigException
import com.workflow.infrastructure.queryexporter.config.ExporterConfigValidator
import com.workflow.infrastructure.queryexporter.config.MetricConfig
import com.workflow.infrastructure.queryexporter.config.MetricType
import com.workflow.infrastructure.queryexporter.config.QueryConfig
import com.workflow.infrastructure.queryexporter.config.ScheduleConfig
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ExporterConfigTest {

    // -- Helpers ----------------------------------------------------------------

    private fun yamlStream(yaml: String) = yaml.trimIndent().byteInputStream()

    private fun validGaugeMetric(
        name: String = "task_count",
        valueColumn: String = "cnt",
        tagColumns: List<String> = emptyList(),
    ) = MetricConfig(
        name = name,
        type = MetricType.GAUGE,
        valueColumn = valueColumn,
        tagColumns = tagColumns,
    )

    private fun validQueryConfig(
        sql: String = "SELECT count(*) AS cnt FROM tasks",
        datasource: String = "default",
        schedule: ScheduleConfig = ScheduleConfig(interval = Duration.ofSeconds(30)),
        metrics: List<MetricConfig> = listOf(validGaugeMetric()),
    ) = QueryConfig(
        sql = sql,
        datasource = datasource,
        schedule = schedule,
        metrics = metrics,
    )

    private fun validConfig(
        queries: Map<String, QueryConfig> = mapOf("task_count_query" to validQueryConfig()),
    ) = ExporterConfig(queries = queries)

    // ==========================================================================
    // A. YAML Parsing
    // ==========================================================================

    @Nested
    inner class YamlParsing {

        @Test
        fun `load valid YAML with interval-based GAUGE query populates all fields`() {
            val yaml = """
                queries:
                  task_count:
                    sql: "SELECT count(*) AS cnt FROM tasks"
                    datasource: default
                    schedule:
                      interval: PT30S
                    metrics:
                      - name: task_count
                        type: GAUGE
                        valueColumn: cnt
            """

            val config = ExporterConfig.load(yamlStream(yaml))

            assertEquals(1, config.queries.size)
            assertTrue(config.queries.containsKey("task_count"))

            val query = config.queries["task_count"]!!
            assertEquals("SELECT count(*) AS cnt FROM tasks", query.sql)
            assertEquals("default", query.datasource)
            assertEquals(Duration.ofSeconds(30), query.schedule.interval)
            assertNull(query.schedule.cron)

            assertEquals(1, query.metrics.size)
            val metric = query.metrics[0]
            assertEquals("task_count", metric.name)
            assertEquals(MetricType.GAUGE, metric.type)
            assertEquals("cnt", metric.valueColumn)
            assertTrue(metric.tagColumns.isEmpty())
        }

        @Test
        fun `load YAML with cron schedule parses cron string`() {
            val yaml = """
                queries:
                  hourly_check:
                    sql: "SELECT count(*) AS cnt FROM jobs"
                    datasource: default
                    schedule:
                      cron: "0 * * * *"
                    metrics:
                      - name: job_count
                        type: GAUGE
                        valueColumn: cnt
            """

            val config = ExporterConfig.load(yamlStream(yaml))

            val query = config.queries["hourly_check"]!!
            assertNull(query.schedule.interval)
            assertEquals("0 * * * *", query.schedule.cron)
        }

        @Test
        fun `load YAML with multiple queries populates all map keys`() {
            val yaml = """
                queries:
                  query_a:
                    sql: "SELECT 1 AS val"
                    datasource: ds1
                    schedule:
                      interval: PT10S
                    metrics:
                      - name: metric_a
                        type: GAUGE
                        valueColumn: val
                  query_b:
                    sql: "SELECT 2 AS val"
                    datasource: ds2
                    schedule:
                      interval: PT20S
                    metrics:
                      - name: metric_b
                        type: GAUGE
                        valueColumn: val
            """

            val config = ExporterConfig.load(yamlStream(yaml))

            assertEquals(2, config.queries.size)
            assertTrue(config.queries.containsKey("query_a"))
            assertTrue(config.queries.containsKey("query_b"))
        }

        @Test
        fun `load YAML parses ISO-8601 duration correctly`() {
            val yaml = """
                queries:
                  fast_poll:
                    sql: "SELECT 1 AS v"
                    datasource: default
                    schedule:
                      interval: PT30S
                    metrics:
                      - name: fast_metric
                        type: GAUGE
                        valueColumn: v
            """

            val config = ExporterConfig.load(yamlStream(yaml))

            assertEquals(Duration.ofSeconds(30), config.queries["fast_poll"]!!.schedule.interval)
        }

        @Test
        fun `load YAML with tag columns parses them`() {
            val yaml = """
                queries:
                  tagged:
                    sql: "SELECT count(*) AS cnt, status FROM tasks GROUP BY status"
                    datasource: default
                    schedule:
                      interval: PT30S
                    metrics:
                      - name: tasks_by_status
                        type: GAUGE
                        valueColumn: cnt
                        tagColumns:
                          - status
            """

            val config = ExporterConfig.load(yamlStream(yaml))

            val metric = config.queries["tagged"]!!.metrics[0]
            assertEquals(listOf("status"), metric.tagColumns)
        }

        @Test
        fun `load YAML with HISTOGRAM including buckets parses them`() {
            val yaml = """
                queries:
                  latency:
                    sql: "SELECT latency_ms FROM requests"
                    datasource: default
                    schedule:
                      interval: PT60S
                    metrics:
                      - name: request_latency
                        type: HISTOGRAM
                        valueColumn: latency_ms
                        buckets:
                          - 10.0
                          - 50.0
                          - 100.0
                          - 500.0
            """

            val config = ExporterConfig.load(yamlStream(yaml))

            val metric = config.queries["latency"]!!.metrics[0]
            assertEquals(MetricType.HISTOGRAM, metric.type)
            assertEquals(listOf(10.0, 50.0, 100.0, 500.0), metric.buckets)
        }

        @Test
        fun `load YAML with ENUM including states parses them`() {
            val yaml = """
                queries:
                  state_check:
                    sql: "SELECT current_state FROM machines"
                    datasource: default
                    schedule:
                      interval: PT30S
                    metrics:
                      - name: machine_state
                        type: ENUM
                        valueColumn: current_state
                        states:
                          - running
                          - stopped
                          - error
            """

            val config = ExporterConfig.load(yamlStream(yaml))

            val metric = config.queries["state_check"]!!.metrics[0]
            assertEquals(MetricType.ENUM, metric.type)
            assertEquals(listOf("running", "stopped", "error"), metric.states)
        }
    }

    // ==========================================================================
    // B. Validation Rules
    // ==========================================================================

    @Nested
    inner class Validation {

        @Test
        fun `valid config passes validation without exception`() {
            assertDoesNotThrow {
                ExporterConfigValidator.validate(validConfig())
            }
        }

        @Test
        fun `interval XOR cron - both set throws ExporterConfigException`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(
                        schedule = ScheduleConfig(
                            interval = Duration.ofSeconds(30),
                            cron = "0 * * * *",
                        ),
                    ),
                ),
            )

            val ex = assertThrows<ExporterConfigException> {
                ExporterConfigValidator.validate(config)
            }
            assertNotNull(ex.message)
        }

        @Test
        fun `interval XOR cron - neither set throws ExporterConfigException`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(
                        schedule = ScheduleConfig(interval = null, cron = null),
                    ),
                ),
            )

            val ex = assertThrows<ExporterConfigException> {
                ExporterConfigValidator.validate(config)
            }
            assertNotNull(ex.message)
        }

        @Test
        fun `negative interval throws ExporterConfigException`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(
                        schedule = ScheduleConfig(interval = Duration.ofSeconds(-5)),
                    ),
                ),
            )

            val ex = assertThrows<ExporterConfigException> {
                ExporterConfigValidator.validate(config)
            }
            assertNotNull(ex.message)
        }

        @Test
        fun `zero interval throws ExporterConfigException`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(
                        schedule = ScheduleConfig(interval = Duration.ZERO),
                    ),
                ),
            )

            val ex = assertThrows<ExporterConfigException> {
                ExporterConfigValidator.validate(config)
            }
            assertNotNull(ex.message)
        }

        @Test
        fun `blank SQL throws ExporterConfigException`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(sql = "   "),
                ),
            )

            val ex = assertThrows<ExporterConfigException> {
                ExporterConfigValidator.validate(config)
            }
            assertNotNull(ex.message)
        }

        @Test
        fun `empty SQL throws ExporterConfigException`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(sql = ""),
                ),
            )

            assertThrows<ExporterConfigException> {
                ExporterConfigValidator.validate(config)
            }
        }

        @Test
        fun `empty metrics list throws ExporterConfigException`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(metrics = emptyList()),
                ),
            )

            val ex = assertThrows<ExporterConfigException> {
                ExporterConfigValidator.validate(config)
            }
            assertNotNull(ex.message)
        }

        @Test
        fun `duplicate metric names across queries throws ExporterConfigException`() {
            val config = validConfig(
                queries = mapOf(
                    "q1" to validQueryConfig(
                        sql = "SELECT 1 AS v",
                        metrics = listOf(validGaugeMetric(name = "shared_name")),
                    ),
                    "q2" to validQueryConfig(
                        sql = "SELECT 2 AS v",
                        metrics = listOf(validGaugeMetric(name = "shared_name")),
                    ),
                ),
            )

            val ex = assertThrows<ExporterConfigException> {
                ExporterConfigValidator.validate(config)
            }
            assertNotNull(ex.message)
        }

        @Test
        fun `duplicate metric names within same query throws ExporterConfigException`() {
            val config = validConfig(
                queries = mapOf(
                    "q1" to validQueryConfig(
                        metrics = listOf(
                            validGaugeMetric(name = "dup"),
                            validGaugeMetric(name = "dup", valueColumn = "other_col"),
                        ),
                    ),
                ),
            )

            assertThrows<ExporterConfigException> {
                ExporterConfigValidator.validate(config)
            }
        }

        @Test
        fun `HISTOGRAM without buckets throws ExporterConfigException`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(
                        metrics = listOf(
                            MetricConfig(
                                name = "latency",
                                type = MetricType.HISTOGRAM,
                                valueColumn = "ms",
                                buckets = emptyList(),
                            ),
                        ),
                    ),
                ),
            )

            val ex = assertThrows<ExporterConfigException> {
                ExporterConfigValidator.validate(config)
            }
            assertNotNull(ex.message)
        }

        @Test
        fun `ENUM without states throws ExporterConfigException`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(
                        metrics = listOf(
                            MetricConfig(
                                name = "state",
                                type = MetricType.ENUM,
                                valueColumn = "current",
                                states = emptyList(),
                            ),
                        ),
                    ),
                ),
            )

            val ex = assertThrows<ExporterConfigException> {
                ExporterConfigValidator.validate(config)
            }
            assertNotNull(ex.message)
        }

        @Test
        fun `blank metric name throws ExporterConfigException`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(
                        metrics = listOf(validGaugeMetric(name = "  ")),
                    ),
                ),
            )

            val ex = assertThrows<ExporterConfigException> {
                ExporterConfigValidator.validate(config)
            }
            assertNotNull(ex.message)
        }

        @Test
        fun `empty metric name throws ExporterConfigException`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(
                        metrics = listOf(validGaugeMetric(name = "")),
                    ),
                ),
            )

            assertThrows<ExporterConfigException> {
                ExporterConfigValidator.validate(config)
            }
        }

        @Test
        fun `blank valueColumn throws ExporterConfigException`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(
                        metrics = listOf(validGaugeMetric(valueColumn = "  ")),
                    ),
                ),
            )

            val ex = assertThrows<ExporterConfigException> {
                ExporterConfigValidator.validate(config)
            }
            assertNotNull(ex.message)
        }

        @Test
        fun `empty valueColumn throws ExporterConfigException`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(
                        metrics = listOf(validGaugeMetric(valueColumn = "")),
                    ),
                ),
            )

            assertThrows<ExporterConfigException> {
                ExporterConfigValidator.validate(config)
            }
        }

        @Test
        fun `valid config with multiple unique metrics across queries passes`() {
            val config = validConfig(
                queries = mapOf(
                    "q1" to validQueryConfig(
                        sql = "SELECT 1 AS v",
                        metrics = listOf(validGaugeMetric(name = "metric_a")),
                    ),
                    "q2" to validQueryConfig(
                        sql = "SELECT 2 AS v",
                        metrics = listOf(validGaugeMetric(name = "metric_b")),
                    ),
                ),
            )

            assertDoesNotThrow {
                ExporterConfigValidator.validate(config)
            }
        }

        @Test
        fun `valid HISTOGRAM with non-empty buckets passes`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(
                        metrics = listOf(
                            MetricConfig(
                                name = "hist_metric",
                                type = MetricType.HISTOGRAM,
                                valueColumn = "ms",
                                buckets = listOf(10.0, 50.0, 100.0),
                            ),
                        ),
                    ),
                ),
            )

            assertDoesNotThrow {
                ExporterConfigValidator.validate(config)
            }
        }

        @Test
        fun `valid ENUM with non-empty states passes`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(
                        metrics = listOf(
                            MetricConfig(
                                name = "enum_metric",
                                type = MetricType.ENUM,
                                valueColumn = "state",
                                states = listOf("up", "down"),
                            ),
                        ),
                    ),
                ),
            )

            assertDoesNotThrow {
                ExporterConfigValidator.validate(config)
            }
        }

        @Test
        fun `cron-only schedule passes validation`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(
                        schedule = ScheduleConfig(interval = null, cron = "0 */5 * * *"),
                    ),
                ),
            )

            assertDoesNotThrow {
                ExporterConfigValidator.validate(config)
            }
        }

        @Test
        fun `interval-only schedule passes validation`() {
            val config = validConfig(
                queries = mapOf(
                    "q" to validQueryConfig(
                        schedule = ScheduleConfig(interval = Duration.ofMinutes(1), cron = null),
                    ),
                ),
            )

            assertDoesNotThrow {
                ExporterConfigValidator.validate(config)
            }
        }
    }

    // ==========================================================================
    // C. Production YAML
    // ==========================================================================

    @Nested
    inner class ProductionYaml {

        @Test
        fun `production query-exporter yaml loads and passes validation`() {
            val input = requireNotNull(
                Thread.currentThread().contextClassLoader.getResourceAsStream("query-exporter.yaml")
            ) { "query-exporter.yaml not found on classpath" }
            val config = ExporterConfig.load(input)
            assertDoesNotThrow { ExporterConfigValidator.validate(config) }
        }
    }
}
