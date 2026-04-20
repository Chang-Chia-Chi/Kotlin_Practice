package com.workflow.stress

import java.time.Duration

enum class StressScale(
    val workers: Int,
    val fanOutSize: Int,
    val workflowBatchSize: Int,
    val outerTimeout: Duration,
    val innerMargin: Duration,
) {
    MODERATE(
        workers = 10,
        fanOutSize = 50,
        workflowBatchSize = 5,
        outerTimeout = Duration.ofSeconds(30),
        innerMargin = Duration.ofSeconds(5),
    ),
    HIGH(
        workers = 50,
        fanOutSize = 500,
        workflowBatchSize = 20,
        outerTimeout = Duration.ofSeconds(120),
        innerMargin = Duration.ofSeconds(15),
    );

    companion object {
        fun resolve(): StressScale =
            System.getProperty("stress.scale", "MODERATE")
                .uppercase()
                .let { valueOf(it) }
    }
}
