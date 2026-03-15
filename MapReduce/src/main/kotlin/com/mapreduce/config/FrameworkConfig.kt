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

    fun speculative(): SpeculativeConfig

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
