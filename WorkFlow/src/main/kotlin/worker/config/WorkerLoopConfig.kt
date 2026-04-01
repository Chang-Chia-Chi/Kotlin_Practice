package com.workflow.worker.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import java.time.Duration

@ConfigMapping(prefix = "framework.worker")
interface WorkerLoopConfig {
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
    @WithDefault("workflow-engine")
    fun serviceName(): String
}
