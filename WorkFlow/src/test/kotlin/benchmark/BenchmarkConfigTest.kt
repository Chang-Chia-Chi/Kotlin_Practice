package com.workflow.benchmark

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BenchmarkConfigTest {

    @Test
    fun `quick scale generates small matrix for single scenario`() {
        val points = BenchmarkConfig.matrixFor(BenchmarkScale.QUICK, "single")
        assertTrue(points.isNotEmpty())
        for (p in points) {
            assertEquals("single", p.scenarioName)
            assertEquals(1, p.tasksPerWorkflow)
            assertTrue(p.workflows in listOf(20, 50))
            assertTrue(p.workers in listOf(5, 10))
        }
    }

    @Test
    fun `quick fanout matrix includes fanOutFactor axis`() {
        val points = BenchmarkConfig.matrixFor(BenchmarkScale.QUICK, "fanout")
        assertTrue(points.isNotEmpty())
        val factors = points.map { it.fanOutFactor }.distinct().sorted()
        assertEquals(listOf(10, 50), factors)
        for (p in points) {
            assertEquals(1 + p.fanOutFactor + 1, p.tasksPerWorkflow)
        }
    }

    @Test
    fun `thorough scale has more combinations than quick`() {
        val quick = BenchmarkConfig.matrixFor(BenchmarkScale.QUICK, "single")
        val thorough = BenchmarkConfig.matrixFor(BenchmarkScale.THOROUGH, "single")
        assertTrue(thorough.size > quick.size)
    }

    @Test
    fun `soak scale produces sustained mode points`() {
        val points = BenchmarkConfig.matrixFor(BenchmarkScale.SOAK, "single")
        assertTrue(points.all { it.isSustained })
        assertTrue(points.all { it.submissionRate > 0 })
        assertTrue(points.all { it.durationSeconds > 0 })
    }

    @Test
    fun `multistep tasksPerWorkflow equals stepCount`() {
        val points = BenchmarkConfig.matrixFor(BenchmarkScale.QUICK, "multistep")
        for (p in points) {
            assertEquals(p.stepCount, p.tasksPerWorkflow)
        }
    }

    @Test
    fun `parse defaults to quick scale with all scenarios`() {
        val config = BenchmarkConfig.parseFrom(emptyMap())
        assertEquals(BenchmarkScale.QUICK, config.scale)
        assertEquals(setOf("single", "fanout", "multistep"), config.scenarios)
        assertEquals(false, config.metricsEnabled)
    }

    @Test
    fun `parse reads system properties`() {
        val props = mapOf(
            "bench.scale" to "thorough",
            "bench.scenarios" to "fanout,single",
            "bench.metrics" to "true",
            "bench.workers" to "32",
        )
        val config = BenchmarkConfig.parseFrom(props)
        assertEquals(BenchmarkScale.THOROUGH, config.scale)
        assertEquals(setOf("fanout", "single"), config.scenarios)
        assertEquals(true, config.metricsEnabled)
        assertEquals(32, config.workerOverride)
    }

    @Test
    fun `timeout per point varies by scale`() {
        assertTrue(BenchmarkConfig.timeoutForScale(BenchmarkScale.QUICK).seconds <= 60)
        assertTrue(BenchmarkConfig.timeoutForScale(BenchmarkScale.THOROUGH).seconds <= 120)
        assertTrue(BenchmarkConfig.timeoutForScale(BenchmarkScale.SOAK).seconds >= 180)
    }
}
