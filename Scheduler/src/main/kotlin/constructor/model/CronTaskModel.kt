package org.example.mockConstructor.config.constructor.model

data class CronTaskModel(
    val runnable: Runnable,
    val crontab: String,
)
