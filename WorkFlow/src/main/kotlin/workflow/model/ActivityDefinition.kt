package com.workflow.workflow.model

import java.time.Duration

data class ActivityDefinition(
    val name: String,
    val transition: String,
    val retries: Int = 0,
    val failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    val deadline: Duration = Duration.ofMinutes(30),
    val fanOut: String? = null,
    val joinPolicy: JoinPolicy = JoinPolicy.All,
    val backoffBase: Duration = Duration.ofSeconds(1),
    val backoffCap: Duration = Duration.ofSeconds(300),
    val queue: String = "default",
    val inputs: Map<String, String> = emptyMap(),
)
