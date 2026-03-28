package com.workflow.dsl

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.time.Duration

enum class FailurePolicy { ABORT, BEST_EFFORT }

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(value = JoinPolicy.All::class, name = "ALL"),
    JsonSubTypes.Type(value = JoinPolicy.Threshold::class, name = "THRESHOLD"),
    JsonSubTypes.Type(value = JoinPolicy.Percentage::class, name = "PERCENTAGE"),
)
sealed interface JoinPolicy {
    data object All : JoinPolicy

    data class Threshold(val n: Int) : JoinPolicy {
        init {
            require(n > 0) { "Threshold n must be > 0, got $n" }
        }
    }

    data class Percentage(val pct: Int) : JoinPolicy {
        init {
            require(pct in 1..100) { "Percentage pct must be in 1..100, got $pct" }
        }
    }
}

data class FanOutDefinition(
    val transition: String,
    val retries: Int = 0,
    val failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    val deadline: Duration = Duration.ofMinutes(30),
    val joinPolicy: JoinPolicy = JoinPolicy.All,
    val backoffBase: Duration = Duration.ofSeconds(1),
    val backoffCap: Duration = Duration.ofSeconds(300),
    val queue: String = "default",
)

data class ActivityDefinition(
    val name: String,
    val transition: String,
    val retries: Int = 0,
    val failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    val deadline: Duration = Duration.ofMinutes(30),
    val fanOut: FanOutDefinition? = null,
    val backoffBase: Duration = Duration.ofSeconds(1),
    val backoffCap: Duration = Duration.ofSeconds(300),
    val queue: String = "default",
    val inputs: Map<String, String> = emptyMap(),
)

data class WorkflowDefinition(
    val activities: List<ActivityDefinition>,
    val deadline: Duration = Duration.ofHours(1),
) {
    init {
        require(activities.isNotEmpty()) { "Workflow must have at least one activity" }
        require(deadline > Duration.ZERO) { "Workflow deadline must be positive" }
        val names = activities.map { it.name }
        require(names.size == names.toSet().size) {
            "Activity names must be unique, found duplicates: ${names.groupBy { it }.filter { it.value.size > 1 }.keys}"
        }
    }
}
