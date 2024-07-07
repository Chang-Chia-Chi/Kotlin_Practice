package org.example.mockConstructor.config.model

data class CronTaskModel(
    val runnable: Runnable,
    val crontab: String,
)
