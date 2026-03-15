package com.mapreduce.testinfra

import com.mapreduce.config.FrameworkConfig
import java.time.Duration

/**
 * Factory for creating [FrameworkConfig] instances in tests.
 * Provides sensible defaults; override only what each test needs.
 */
object TestConfig {

    fun create(
        circuitBreakerThreshold: Int = 5,
        pollInterval: Duration = Duration.ofSeconds(1),
        bulkheadSize: Int = 4,
        workerId: String = "test-worker",
        queues: List<String> = listOf("default", "mr"),
        monitorInterval: Duration = Duration.ofSeconds(1),
        heartbeatInterval: Duration = Duration.ofSeconds(30),
        reaperScanInterval: Duration = Duration.ofSeconds(10),
        reaperStaleThreshold: Duration = Duration.ofSeconds(90),
        reaperBatchSize: Int = 50,
        defaultTimeout: Duration = Duration.ofSeconds(30),
        drainTimeout: Duration = Duration.ofSeconds(5),
        alertThreshold: Int = 5,
        alertWindow: Duration = Duration.ofMinutes(5),
    ): FrameworkConfig = object : FrameworkConfig {

        override fun worker() = object : FrameworkConfig.WorkerConfig {
            override fun pollInterval() = pollInterval
            override fun bulkheadSize() = bulkheadSize
            override fun id() = workerId
            override fun queues() = queues
            override fun circuitBreakerThreshold() = circuitBreakerThreshold
        }

        override fun leader() = object : FrameworkConfig.LeaderConfig {
            override fun monitorInterval() = monitorInterval
        }

        override fun leaderElection() = object : FrameworkConfig.LeaderElectionConfig {
            override fun leaseName() = "test-leader"
            override fun namespace() = "default"
            override fun leaseDuration() = Duration.ofSeconds(15)
            override fun renewDeadline() = Duration.ofSeconds(10)
            override fun retryPeriod() = Duration.ofSeconds(2)
        }

        override fun shutdown() = object : FrameworkConfig.ShutdownConfig {
            override fun drainTimeout() = drainTimeout
            override fun leaderTeardownTimeout() = Duration.ofSeconds(1)
            override fun releaseTimeout() = Duration.ofSeconds(1)
            override fun logInterval() = Duration.ofSeconds(1)
        }

        override fun metrics() = object : FrameworkConfig.MetricsConfig {
            override fun queueDepthInterval() = Duration.ofSeconds(15)
        }

        override fun deadLetter() = object : FrameworkConfig.DeadLetterConfig {
            override fun retentionDays() = 30
            override fun cleanupScheduleHours() = 24
            override fun archiveBeforeDelete() = false
            override fun alertDefaultThreshold() = alertThreshold
            override fun alertDefaultWindow() = alertWindow
            override fun slackWebhookUrl() = ""
        }

        override fun health() = object : FrameworkConfig.HealthConfig {
            override fun oracleCheckTimeout() = Duration.ofSeconds(5)
            override fun workerLoopStaleThreshold() = Duration.ofSeconds(6)
            override fun detailEndpointEnabled() = true
            override fun leaderReadinessEnabled() = false
        }

        override fun pipeline() = object : FrameworkConfig.PipelineConfig {
            override fun defaultTimeout() = defaultTimeout
        }

        override fun heartbeat() = object : FrameworkConfig.HeartbeatConfig {
            override fun interval() = heartbeatInterval
        }

        override fun reaper() = object : FrameworkConfig.ReaperConfig {
            override fun scanInterval() = reaperScanInterval
            override fun staleThreshold() = reaperStaleThreshold
            override fun batchSize() = reaperBatchSize
        }
    }
}
