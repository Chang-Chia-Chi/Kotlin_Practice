package com.workflow.workflow.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import java.time.Duration

@ConfigMapping(prefix = "framework.sweeper")
interface SweeperConfig {
    @WithDefault("PT30S")
    fun interval(): Duration
    @WithDefault("PT2M")
    fun gracePeriod(): Duration
    @WithDefault("PT10M")
    fun staleTaskThreshold(): Duration
}
