package com.workflow.workflow.model

import com.fasterxml.jackson.annotation.JsonIgnore
import java.time.Duration

data class ActivityDefinition(
    val name: String,
    val transition: String,
    val retries: Int = 0,
    val deadline: Duration = Duration.ofMinutes(30),
    val fanOut: FanOutDefinition? = null,
    val backoffBase: Duration = Duration.ofSeconds(1),
    val backoffCap: Duration = Duration.ofSeconds(300),
    val queue: String = "default",
    val inputs: Map<String, String> = emptyMap(),
    val successors: List<Edge> = emptyList(),
) {
    @get:JsonIgnore
    val isTerminal: Boolean get() = successors.isEmpty() && fanOut == null
}
