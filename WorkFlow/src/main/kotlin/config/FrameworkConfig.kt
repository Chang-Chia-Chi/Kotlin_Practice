package com.workflow.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import java.time.Duration

@ConfigMapping(prefix = "framework")
interface FrameworkConfig {
    @WithDefault("workflow-engine")
    fun serviceName(): String
    fun worker(): WorkerConfig
    fun leaderElection(): LeaderElectionConfig
    fun shutdown(): ShutdownConfig
    fun sweeper(): SweeperConfig

    interface WorkerConfig {
        @WithDefault("localhost")
        fun id(): String
        @WithDefault("PT1S")
        fun pollInterval(): Duration
        @WithDefault("PT5S")
        fun fallbackPollInterval(): Duration
        @WithDefault("4")
        fun concurrency(): Int
        @WithDefault("1")
        fun batchSize(): Int
        @WithDefault("16")
        fun maxBatchSize(): Int
        @WithDefault("localhost")
        fun podIp(): String
    }

    interface LeaderElectionConfig {
        @WithDefault("default")
        fun namespace(): String
        @WithDefault("workflow-leader")
        fun leaseName(): String
        @WithDefault("PT15S")
        fun leaseDuration(): Duration
        @WithDefault("PT10S")
        fun renewDeadline(): Duration
        @WithDefault("PT2S")
        fun retryPeriod(): Duration
        @WithDefault("PT45S")
        fun healthThreshold(): Duration
    }

    interface ShutdownConfig {
        @WithDefault("PT30S")
        fun globalTimeout(): Duration
        @WithDefault("PT10S")
        fun leaderTeardownTimeout(): Duration
    }

    interface SweeperConfig {
        @WithDefault("PT30S")
        fun interval(): Duration
        @WithDefault("PT2M")
        fun gracePeriod(): Duration
        @WithDefault("PT10M")
        fun staleTaskThreshold(): Duration
    }
}
