package com.mapreduce.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import io.smallrye.config.WithName
import java.time.Duration

@ConfigMapping(prefix = "mapreduce")
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

        @WithName("stale-threshold")
        @WithDefault("5M")
        fun staleThreshold(): Duration

        @WithDefault("local-worker")
        fun id(): String

        @WithName("shutdown-timeout")
        @WithDefault("30S")
        fun shutdownTimeout(): Duration

        @WithDefault("default,mr")
        fun queues(): List<String>

        @WithName("circuit-breaker-threshold")
        @WithDefault("10")
        fun circuitBreakerThreshold(): Int
    }

    interface LeaderConfig {
        @WithName("monitor-interval")
        @WithDefault("3S")
        fun monitorInterval(): Duration
    }

    /** Kubernetes Lease-based leader election with fencing epoch. */
    interface LeaderElectionConfig {
        @WithName("lease-name")
        @WithDefault("mapreduce-leader")
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

    fun metrics(): MetricsConfig

    /** Autoscaling metrics configuration. */
    interface MetricsConfig {
        @WithName("queue-depth-interval")
        @WithDefault("15S")
        fun queueDepthInterval(): Duration
    }

    @WithName("dead-letter")
    fun deadLetter(): DeadLetterConfig

    fun health(): HealthConfig

    /** Health probe registry configuration. */
    interface HealthConfig {
        @WithName("oracle-check-timeout")
        @WithDefault("5S")
        fun oracleCheckTimeout(): Duration

        @WithName("worker-loop-stale-threshold")
        @WithDefault("6S")
        fun workerLoopStaleThreshold(): Duration

        @WithName("detail-endpoint-enabled")
        @WithDefault("true")
        fun detailEndpointEnabled(): Boolean

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

    fun speculative(): SpeculativeConfig

    /** Dead letter processor configuration. */
    interface DeadLetterConfig {
        @WithName("retention-days")
        @WithDefault("30")
        fun retentionDays(): Int

        @WithName("cleanup-schedule-hours")
        @WithDefault("24")
        fun cleanupScheduleHours(): Int

        @WithName("archive-before-delete")
        @WithDefault("false")
        fun archiveBeforeDelete(): Boolean

        @WithName("alert-default-threshold")
        @WithDefault("10")
        fun alertDefaultThreshold(): Int

        @WithName("alert-default-window")
        @WithDefault("5M")
        fun alertDefaultWindow(): Duration

        @WithName("slack-webhook-url")
        @WithDefault("")
        fun slackWebhookUrl(): String
    }

    interface SpeculativeConfig {
        @WithName("enabled")
        @WithDefault("true")
        fun enabled(): Boolean

        @WithName("median-multiplier")
        @WithDefault("3.0")
        fun medianMultiplier(): Double

        @WithName("min-completed")
        @WithDefault("5")
        fun minCompleted(): Int
    }
}
