package com.workflow.workflow.model

import java.time.Duration

data class FanOutDefinition(
    val transition: String,
    val retries: Int = 0,
    val deadline: Duration = Duration.ofMinutes(30),
    val backoffBase: Duration = Duration.ofSeconds(1),
    val backoffCap: Duration = Duration.ofSeconds(300),
    val queue: String = "default",
)
