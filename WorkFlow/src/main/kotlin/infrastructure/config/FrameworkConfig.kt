package com.workflow.infrastructure.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault

@ConfigMapping(prefix = "framework")
interface FrameworkConfig {
    @WithDefault("workflow-engine")
    fun serviceName(): String
}
