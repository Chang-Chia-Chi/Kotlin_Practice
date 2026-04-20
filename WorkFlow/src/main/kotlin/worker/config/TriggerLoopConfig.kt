package com.workflow.worker.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import java.time.Duration

@ConfigMapping(prefix = "framework.trigger")
interface TriggerLoopConfig {
    @WithDefault("PT5S")
    fun sweepInterval(): Duration

    @WithDefault("5")
    fun sqlMaxConcurrent(): Int

    @WithDefault("true")
    fun autoStart(): Boolean
}
