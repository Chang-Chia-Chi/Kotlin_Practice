package com.workflow.infrastructure.queryexporter.config

import java.time.Duration

data class ScheduleConfig(
    val interval: Duration? = null,
    val cron: String? = null,
)
