package com.mapreduce.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import io.smallrye.config.WithName
import java.time.Duration

@ConfigMapping(prefix = "mapreduce")
interface FrameworkConfig {

    fun worker(): WorkerConfig

    fun leader(): LeaderConfig

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
    }

    interface LeaderConfig {
        @WithName("monitor-interval")
        @WithDefault("3S")
        fun monitorInterval(): Duration
    }
}
