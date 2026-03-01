package com.exporter.engine

import com.exporter.config.MetricType
import com.exporter.config.ResolvedMetric
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class RowProcessorTest {

    private fun metric(
        name: String = "test",
        type: MetricType = MetricType.GAUGE,
        valueColumn: String = "value",
        tagColumns: List<String> = emptyList(),
        states: List<String> = emptyList(),
    ) = ResolvedMetric(
        name = name,
        type = type,
        valueColumn = valueColumn,
        tagColumns = tagColumns,
        buckets = emptyList(),
        states = states,
    )

    // ─── Value extraction ─────────────────────────────────────

    @Nested
    inner class ValueExtraction {
        @Test
        fun `extracts Integer value`() {
            val row = mapOf<String, Any?>("value" to 42)
            assertThat(RowProcessor.extractValue(row, metric())).isEqualTo(42.0)
        }

        @Test
        fun `extracts Long value`() {
            val row = mapOf<String, Any?>("value" to 999_999L)
            assertThat(RowProcessor.extractValue(row, metric())).isEqualTo(999999.0)
        }

        @Test
        fun `extracts Double value`() {
            val row = mapOf<String, Any?>("value" to 3.14)
            assertThat(RowProcessor.extractValue(row, metric())).isEqualTo(3.14)
        }

        @Test
        fun `extracts BigDecimal value`() {
            val row = mapOf<String, Any?>("value" to BigDecimal("1234.56"))
            assertThat(RowProcessor.extractValue(row, metric())).isEqualTo(1234.56)
        }

        @Test
        fun `extracts String-encoded number`() {
            val row = mapOf<String, Any?>("value" to "99.5")
            assertThat(RowProcessor.extractValue(row, metric())).isEqualTo(99.5)
        }

        @Test
        fun `returns null for non-numeric string`() {
            val row = mapOf<String, Any?>("value" to "not_a_number")
            assertThat(RowProcessor.extractValue(row, metric())).isNull()
        }

        @Test
        fun `returns null for null column`() {
            val row = mapOf<String, Any?>("value" to null)
            assertThat(RowProcessor.extractValue(row, metric())).isNull()
        }

        @Test
        fun `returns null for missing column`() {
            val row = mapOf<String, Any?>("other_col" to 42)
            assertThat(RowProcessor.extractValue(row, metric())).isNull()
        }

        @Test
        fun `case-insensitive column lookup`() {
            val row = mapOf<String, Any?>("VALUE" to 77)
            assertThat(RowProcessor.extractValue(row, metric())).isEqualTo(77.0)
        }

        @Test
        fun `exact match preferred over case-insensitive`() {
            val row = mapOf<String, Any?>("value" to 10, "VALUE" to 20)
            assertThat(RowProcessor.extractValue(row, metric())).isEqualTo(10.0)
        }
    }

    // ─── Tag extraction ───────────────────────────────────────

    @Nested
    inner class TagExtraction {
        @Test
        fun `extracts tags from matching columns`() {
            val row = mapOf<String, Any?>("value" to 1, "host" to "srv01", "env" to "prod")
            val m = metric(tagColumns = listOf("host", "env"))
            val tags = RowProcessor.extractTags(row, m)

            assertThat(tags).containsEntry("host", "srv01")
            assertThat(tags).containsEntry("env", "prod")
        }

        @Test
        fun `missing tag column becomes unknown`() {
            val row = mapOf<String, Any?>("value" to 1, "host" to "srv01")
            val m = metric(tagColumns = listOf("host", "env"))
            val tags = RowProcessor.extractTags(row, m)

            assertThat(tags["env"]).isEqualTo("unknown")
        }

        @Test
        fun `null tag value becomes unknown`() {
            val row = mapOf<String, Any?>("value" to 1, "host" to null)
            val m = metric(tagColumns = listOf("host"))
            val tags = RowProcessor.extractTags(row, m)

            assertThat(tags["host"]).isEqualTo("unknown")
        }

        @Test
        fun `empty tag columns returns empty map`() {
            val row = mapOf<String, Any?>("value" to 1)
            val tags = RowProcessor.extractTags(row, metric())
            assertThat(tags).isEmpty()
        }

        @Test
        fun `numeric tag value is stringified`() {
            val row = mapOf<String, Any?>("value" to 1, "port" to 8080)
            val m = metric(tagColumns = listOf("port"))
            val tags = RowProcessor.extractTags(row, m)
            assertThat(tags["port"]).isEqualTo("8080")
        }
    }

    // ─── Enum state extraction ────────────────────────────────

    @Nested
    inner class EnumExtraction {
        @Test
        fun `extracts enum state as string`() {
            val row = mapOf<String, Any?>("status" to "active")
            val m = metric(type = MetricType.ENUM, valueColumn = "status", states = listOf("active", "inactive"))
            assertThat(RowProcessor.extractEnumState(row, m)).isEqualTo("active")
        }

        @Test
        fun `numeric enum value is stringified`() {
            val row = mapOf<String, Any?>("status" to 1)
            val m = metric(type = MetricType.ENUM, valueColumn = "status")
            assertThat(RowProcessor.extractEnumState(row, m)).isEqualTo("1")
        }

        @Test
        fun `null enum value returns null`() {
            val row = mapOf<String, Any?>("status" to null)
            val m = metric(type = MetricType.ENUM, valueColumn = "status")
            assertThat(RowProcessor.extractEnumState(row, m)).isNull()
        }
    }
}
