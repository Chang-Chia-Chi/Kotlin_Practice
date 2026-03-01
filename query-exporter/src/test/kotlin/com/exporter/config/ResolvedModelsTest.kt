package com.exporter.config

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Duration

class ResolvedModelsTest {

    @Test
    fun `ResolvedSchedule with interval only is valid`() {
        val schedule = ResolvedSchedule(interval = Duration.ofSeconds(10), cron = null)
        assertThat(schedule.interval).isEqualTo(Duration.ofSeconds(10))
        assertThat(schedule.cron).isNull()
    }

    @Test
    fun `ResolvedSchedule with cron only is valid`() {
        val schedule = ResolvedSchedule(interval = null, cron = "0 0/5 * * * ?")
        assertThat(schedule.interval).isNull()
        assertThat(schedule.cron).isEqualTo("0 0/5 * * * ?")
    }

    @Test
    fun `ResolvedSchedule with both interval and cron throws`() {
        assertThatThrownBy {
            ResolvedSchedule(interval = Duration.ofSeconds(5), cron = "0 * * * * ?")
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("Exactly one")
    }

    @Test
    fun `ResolvedSchedule with neither interval nor cron throws`() {
        assertThatThrownBy {
            ResolvedSchedule(interval = null, cron = null)
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("Exactly one")
    }

    @Test
    fun `ResolvedQuery data class equality`() {
        val q1 = ResolvedQuery(
            name = "q1", sql = "SELECT 1", datasource = "default",
            schedule = ResolvedSchedule(Duration.ofSeconds(5), null),
            metrics = listOf(
                ResolvedMetric("m1", MetricType.GAUGE, "value", emptyList(), emptyList(), emptyList())
            ),
        )
        val q2 = q1.copy()
        assertThat(q1).isEqualTo(q2)
        assertThat(q1.hashCode()).isEqualTo(q2.hashCode())
    }

    @Test
    fun `ResolvedMetric preserves all fields`() {
        val m = ResolvedMetric(
            name = "hist_metric",
            type = MetricType.HISTOGRAM,
            valueColumn = "latency",
            tagColumns = listOf("host", "env"),
            buckets = listOf(1.0, 5.0, 10.0, 50.0),
            states = emptyList(),
        )
        assertThat(m.name).isEqualTo("hist_metric")
        assertThat(m.type).isEqualTo(MetricType.HISTOGRAM)
        assertThat(m.valueColumn).isEqualTo("latency")
        assertThat(m.tagColumns).containsExactly("host", "env")
        assertThat(m.buckets).containsExactly(1.0, 5.0, 10.0, 50.0)
        assertThat(m.states).isEmpty()
    }
}
