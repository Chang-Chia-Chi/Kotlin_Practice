package com.workflow.benchmark

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BenchmarkReporterTest {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())

    @Test
    fun `saveReport writes valid JSON with all fields`(@TempDir tempDir: Path) {
        val report = BenchmarkReport(
            timestamp = "2026-03-29T14:30:00",
            scale = "quick",
            gitCommit = "abc1234",
            environment = EnvironmentInfo("Windows 11", 8, 4096, "21.0.2"),
            scenarios = listOf(
                ScenarioResult(
                    name = "single",
                    parameters = mapOf("workflows" to 50, "workers" to 10),
                    totalWorkflows = 50, totalTasks = 50,
                    wallClockMs = 2000, workflowsPerSec = 25.0, tasksPerSec = 25.0,
                    latency = LatencyStats(20, 45, 78),
                    phaseBreakdown = mapOf(
                        "task.claim" to PhaseSummary(50, 2.1, 1.8, 4.2, 6.1),
                    ),
                ),
            ),
        )

        BenchmarkReporter.saveReport(report, tempDir, objectMapper)

        val files = tempDir.toFile().listFiles()!!
        assertEquals(1, files.size)
        assertTrue(files[0].name.startsWith("quick-"))
        assertTrue(files[0].name.endsWith(".json"))

        val parsed = objectMapper.readValue<BenchmarkReport>(files[0])
        assertEquals("abc1234", parsed.gitCommit)
        assertEquals(1, parsed.scenarios.size)
        assertEquals(50, parsed.scenarios[0].totalWorkflows)
    }

    @Test
    fun `formatScenarioLine produces compact one-liner`() {
        val result = ScenarioResult(
            name = "fanout", parameters = mapOf("workflows" to 10, "fanOutFactor" to 500, "workers" to 20),
            totalWorkflows = 10, totalTasks = 5020,
            wallClockMs = 8432, workflowsPerSec = 1.19, tasksPerSec = 595.0,
            latency = LatencyStats(780, 1200, 1450),
            phaseBreakdown = emptyMap(),
        )
        val line = BenchmarkReporter.formatScenarioLine(result)
        assertTrue(line.contains("fanout"))
        assertTrue(line.contains("1.19"))
        assertTrue(line.contains("595.0"))
    }

    @Test
    fun `formatComparisonTable handles multiple results`() {
        val results = listOf(
            ScenarioResult("single", mapOf("workflows" to 20, "workers" to 5), 20, 20, 1000, 20.0, 20.0, LatencyStats(10, 20, 30), emptyMap()),
            ScenarioResult("single", mapOf("workflows" to 50, "workers" to 10), 50, 50, 2000, 25.0, 25.0, LatencyStats(15, 35, 50), emptyMap()),
        )
        val table = BenchmarkReporter.formatComparisonTable(results)
        assertTrue(table.contains("single"))
        assertTrue(table.contains("20.0"))
        assertTrue(table.contains("25.0"))
    }
}
