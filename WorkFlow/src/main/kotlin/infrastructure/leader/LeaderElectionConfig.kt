package com.workflow.infrastructure.leader

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import java.time.Duration

@ConfigMapping(prefix = "framework.leader-election")
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
