package com.workflow.benchmark

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

data class EnvironmentInfo(
    val os: String,
    val cpuCores: Int,
    val jvmMaxMemoryMb: Long,
    val javaVersion: String,
    val oracleVersion: String,
)

data class BenchmarkReport(
    val timestamp: String,
    val scale: String,
    val gitCommit: String,
    val environment: EnvironmentInfo,
    val scenarios: List<ScenarioResult>,
)

object BenchmarkReporter {

    fun saveReport(report: BenchmarkReport, outputDir: Path, objectMapper: ObjectMapper) {
        Files.createDirectories(outputDir)
        val ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss"))
        val file = outputDir.resolve("${report.scale}-$ts.json")
        objectMapper.copy()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .writeValue(file.toFile(), report)
        println("Results saved to $file")
    }

    fun formatScenarioLine(r: ScenarioResult): String {
        val params = r.parameters.entries.joinToString(" ") { "${it.key}=${it.value}" }
        return "[${r.name}] $params -> ${"%.2f".format(r.workflowsPerSec)} wf/s | " +
            "${"%.1f".format(r.tasksPerSec)} tasks/s | " +
            "p50=${r.latency.p50Ms}ms p95=${r.latency.p95Ms}ms p99=${r.latency.p99Ms}ms"
    }

    fun formatComparisonTable(results: List<ScenarioResult>): String {
        if (results.isEmpty()) return "(no results)"

        val paramKeys = results.flatMap { it.parameters.keys }.distinct().sorted()

        val headers = listOf("scenario") + paramKeys + listOf("wf/s", "tasks/s", "p50", "p95", "p99")
        val rows = results.map { r ->
            listOf(r.name) +
                paramKeys.map { r.parameters[it]?.toString() ?: "-" } +
                listOf(
                    "%.2f".format(r.workflowsPerSec),
                    "%.1f".format(r.tasksPerSec),
                    "${r.latency.p50Ms}ms",
                    "${r.latency.p95Ms}ms",
                    "${r.latency.p99Ms}ms",
                )
        }

        val widths = headers.indices.map { col ->
            maxOf(headers[col].length, rows.maxOf { it[col].length })
        }

        val sep = "+-" + widths.joinToString("-+-") { "-".repeat(it) } + "-+"
        val headerLine = "| " + headers.mapIndexed { i, h -> h.padEnd(widths[i]) }.joinToString(" | ") + " |"
        val dataLines = rows.map { row ->
            "| " + row.mapIndexed { i, v -> v.padEnd(widths[i]) }.joinToString(" | ") + " |"
        }

        return buildString {
            appendLine(sep)
            appendLine(headerLine)
            appendLine(sep)
            dataLines.forEach { appendLine(it) }
            appendLine(sep)
        }
    }

    fun captureEnvironment(oracleVersion: String = "unknown"): EnvironmentInfo = EnvironmentInfo(
        os = "${System.getProperty("os.name")} ${System.getProperty("os.version")}",
        cpuCores = Runtime.getRuntime().availableProcessors(),
        jvmMaxMemoryMb = Runtime.getRuntime().maxMemory() / (1024 * 1024),
        javaVersion = System.getProperty("java.version"),
        oracleVersion = oracleVersion,
    )

    fun captureGitCommit(): String = try {
        val process = ProcessBuilder("git", "rev-parse", "--short", "HEAD")
            .redirectErrorStream(true).start()
        process.inputStream.bufferedReader().readLine()?.trim() ?: "unknown"
    } catch (_: Exception) {
        "unknown"
    }
}
