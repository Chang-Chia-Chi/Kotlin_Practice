package com.workflow.infrastructure.shutdown

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import java.time.Duration

@ConfigMapping(prefix = "framework.shutdown")
interface ShutdownConfig {
    @WithDefault("PT30S")
    fun globalTimeout(): Duration
    @WithDefault("PT10S")
    fun leaderTeardownTimeout(): Duration
}
