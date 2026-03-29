package com.workflow.benchmark

import java.time.Duration

enum class BenchmarkScale { QUICK, THOROUGH, SOAK }

data class MatrixPoint(
    val scenarioName: String,
    val workflows: Int,
    val workers: Int,
    val handlerLatencyMs: Int,
    val payloadSizeBytes: Int,
    val fanOutFactor: Int = 0,
    val stepCount: Int = 0,
    val submissionRate: Int = 0,
    val durationSeconds: Int = 0,
) {
    val isSustained: Boolean get() = submissionRate > 0

    val tasksPerWorkflow: Int
        get() = when (scenarioName) {
            "single" -> 1
            "fanout" -> 1 + fanOutFactor + 1
            "multistep" -> stepCount
            else -> 1
        }

    fun toParameterMap(): Map<String, Any> = buildMap {
        put("workflows", workflows)
        put("workers", workers)
        put("handlerLatencyMs", handlerLatencyMs)
        put("payloadSizeBytes", payloadSizeBytes)
        if (fanOutFactor > 0) put("fanOutFactor", fanOutFactor)
        if (stepCount > 0) put("stepCount", stepCount)
        if (submissionRate > 0) put("submissionRate", submissionRate)
        if (durationSeconds > 0) put("durationSeconds", durationSeconds)
    }
}

data class BenchmarkRunConfig(
    val scale: BenchmarkScale,
    val scenarios: Set<String>,
    val metricsEnabled: Boolean,
    val workerOverride: Int? = null,
    val fanOutOverride: Int? = null,
)

object BenchmarkConfig {

    fun parse(): BenchmarkRunConfig = parseFrom(System.getProperties().map {
        it.key.toString() to it.value.toString()
    }.toMap())

    fun parseFrom(props: Map<String, String>): BenchmarkRunConfig {
        val scale = props["bench.scale"]?.uppercase()
            ?.let { BenchmarkScale.valueOf(it) }
            ?: BenchmarkScale.QUICK
        val scenarios = props["bench.scenarios"]
            ?.split(",")?.map { it.trim() }?.toSet()
            ?: setOf("single", "fanout", "multistep")
        val metricsEnabled = props["bench.metrics"]?.toBoolean() ?: false
        val workerOverride = props["bench.workers"]?.toIntOrNull()
        val fanOutOverride = props["bench.fanout.factor"]?.toIntOrNull()
        return BenchmarkRunConfig(scale, scenarios, metricsEnabled, workerOverride, fanOutOverride)
    }

    fun matrixFor(scale: BenchmarkScale, scenario: String): List<MatrixPoint> =
        when (scale) {
            BenchmarkScale.QUICK -> quickMatrix(scenario)
            BenchmarkScale.THOROUGH -> thoroughMatrix(scenario)
            BenchmarkScale.SOAK -> soakMatrix(scenario)
        }

    fun timeoutForScale(scale: BenchmarkScale): Duration = when (scale) {
        BenchmarkScale.QUICK -> Duration.ofSeconds(60)
        BenchmarkScale.THOROUGH -> Duration.ofSeconds(120)
        BenchmarkScale.SOAK -> Duration.ofSeconds(180)
    }

    private fun quickMatrix(scenario: String): List<MatrixPoint> {
        val latencies = listOf(0)
        val payloads = listOf(100)
        return when (scenario) {
            "single" -> cartesian(
                workflows = listOf(20, 50), workers = listOf(5, 10),
                latencies = latencies, payloads = payloads,
            ) { wf, w, lat, pay -> MatrixPoint("single", wf, w, lat, pay) }

            "fanout" -> cartesian(
                workflows = listOf(5), workers = listOf(10),
                latencies = latencies, payloads = payloads,
                extra = listOf(10, 50),
            ) { wf, w, lat, pay, fo -> MatrixPoint("fanout", wf, w, lat, pay, fanOutFactor = fo) }

            "multistep" -> cartesian(
                workflows = listOf(10), workers = listOf(5),
                latencies = latencies, payloads = payloads,
                extra = listOf(3, 5),
            ) { wf, w, lat, pay, sc -> MatrixPoint("multistep", wf, w, lat, pay, stepCount = sc) }

            else -> emptyList()
        }
    }

    private fun thoroughMatrix(scenario: String): List<MatrixPoint> {
        val latencies = listOf(0, 10)
        val payloads = listOf(100, 1000)
        return when (scenario) {
            "single" -> cartesian(
                workflows = listOf(50, 100, 200), workers = listOf(10, 20),
                latencies = latencies, payloads = payloads,
            ) { wf, w, lat, pay -> MatrixPoint("single", wf, w, lat, pay) }

            "fanout" -> cartesian(
                workflows = listOf(5, 10), workers = listOf(10, 20),
                latencies = latencies, payloads = payloads,
                extra = listOf(50, 100, 500),
            ) { wf, w, lat, pay, fo -> MatrixPoint("fanout", wf, w, lat, pay, fanOutFactor = fo) }

            "multistep" -> cartesian(
                workflows = listOf(10, 20), workers = listOf(10, 20),
                latencies = latencies, payloads = payloads,
                extra = listOf(3, 5, 10),
            ) { wf, w, lat, pay, sc -> MatrixPoint("multistep", wf, w, lat, pay, stepCount = sc) }

            else -> emptyList()
        }
    }

    private fun soakMatrix(scenario: String): List<MatrixPoint> {
        val latencies = listOf(0, 10, 50)
        val payloads = listOf(100, 1000, 10000)
        val dur = 120
        return when (scenario) {
            "single" -> cartesian(
                workflows = listOf(0), workers = listOf(10, 20, 50),
                latencies = latencies, payloads = payloads,
            ) { _, w, lat, pay ->
                MatrixPoint("single", 0, w, lat, pay, submissionRate = 50, durationSeconds = dur)
            }

            "fanout" -> cartesian(
                workflows = listOf(0), workers = listOf(20, 50),
                latencies = latencies, payloads = payloads,
                extra = listOf(100, 500, 1000),
            ) { _, w, lat, pay, fo ->
                MatrixPoint("fanout", 0, w, lat, pay, fanOutFactor = fo, submissionRate = 5, durationSeconds = dur)
            }

            "multistep" -> cartesian(
                workflows = listOf(0), workers = listOf(10, 20, 50),
                latencies = latencies, payloads = payloads,
                extra = listOf(5, 10, 20),
            ) { _, w, lat, pay, sc ->
                MatrixPoint("multistep", 0, w, lat, pay, stepCount = sc, submissionRate = 10, durationSeconds = dur)
            }

            else -> emptyList()
        }
    }

    private fun cartesian(
        workflows: List<Int>, workers: List<Int>,
        latencies: List<Int>, payloads: List<Int>,
        build: (Int, Int, Int, Int) -> MatrixPoint,
    ): List<MatrixPoint> =
        workflows.flatMap { wf ->
            workers.flatMap { w ->
                latencies.flatMap { lat ->
                    payloads.map { pay -> build(wf, w, lat, pay) }
                }
            }
        }

    private fun cartesian(
        workflows: List<Int>, workers: List<Int>,
        latencies: List<Int>, payloads: List<Int>,
        extra: List<Int>,
        build: (Int, Int, Int, Int, Int) -> MatrixPoint,
    ): List<MatrixPoint> =
        workflows.flatMap { wf ->
            workers.flatMap { w ->
                latencies.flatMap { lat ->
                    payloads.flatMap { pay ->
                        extra.map { e -> build(wf, w, lat, pay, e) }
                    }
                }
            }
        }
}
