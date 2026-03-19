package com.mapreduce.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import io.smallrye.config.WithName
import java.time.Duration

@ConfigMapping(prefix = "taskqueue")
interface FrameworkConfig {

    fun worker(): WorkerConfig

    fun leader(): LeaderConfig

    @WithName("leader-election")
    fun leaderElection(): LeaderElectionConfig

    interface WorkerConfig {
        @WithName("poll-interval")
        @WithDefault("2S")
        fun pollInterval(): Duration

        @WithName("bulkhead-size")
        @WithDefault("4")
        fun bulkheadSize(): Int

        @WithDefault("\${HOSTNAME:local-worker}")
        fun id(): String

        @WithDefault("default,mr")
        fun queues(): List<String>
    }

    interface LeaderConfig {
        @WithName("monitor-interval")
        @WithDefault("3S")
        fun monitorInterval(): Duration
    }

    /** Kubernetes Lease-based leader election with fencing epoch. */
    interface LeaderElectionConfig {
        @WithName("lease-name")
        @WithDefault("taskqueue-leader")
        fun leaseName(): String

        @WithDefault("\${KUBERNETES_NAMESPACE:default}")
        fun namespace(): String

        @WithName("lease-duration")
        @WithDefault("15S")
        fun leaseDuration(): Duration

        @WithName("renew-deadline")
        @WithDefault("10S")
        fun renewDeadline(): Duration

        @WithName("retry-period")
        @WithDefault("2S")
        fun retryPeriod(): Duration
    }

    fun shutdown(): ShutdownConfig

    /** Graceful shutdown phase timeouts. */
    interface ShutdownConfig {
        @WithName("global-timeout")
        @WithDefault("90S")
        fun globalTimeout(): Duration

        @WithName("drain-timeout")
        @WithDefault("60S")
        fun drainTimeout(): Duration

        @WithName("leader-teardown-timeout")
        @WithDefault("5S")
        fun leaderTeardownTimeout(): Duration

        @WithName("release-timeout")
        @WithDefault("5S")
        fun releaseTimeout(): Duration

        @WithName("log-interval")
        @WithDefault("5S")
        fun logInterval(): Duration
    }

    fun health(): HealthConfig

    /** Health probe registry configuration. */
    interface HealthConfig {
        @WithName("oracle-check-timeout")
        @WithDefault("5S")
        fun oracleCheckTimeout(): Duration

        @WithName("worker-loop-stale-threshold")
        @WithDefault("6S")
        fun workerLoopStaleThreshold(): Duration

        @WithName("leader-readiness-enabled")
        @WithDefault("false")
        fun leaderReadinessEnabled(): Boolean
    }

    fun pipeline(): PipelineConfig

    /** Handler execution pipeline configuration. */
    interface PipelineConfig {
        @WithName("default-timeout")
        @WithDefault("2M")
        fun defaultTimeout(): Duration
    }

    fun reaper(): ReaperConfig

    /** Stale task reaper configuration (leader-only). */
    interface ReaperConfig {
        @WithName("scan-interval")
        @WithDefault("30S")
        fun scanInterval(): Duration

        @WithName("stale-threshold")
        @WithDefault("5M")
        fun staleThreshold(): Duration

        @WithName("batch-size")
        @WithDefault("50")
        fun batchSize(): Int
    }

}
