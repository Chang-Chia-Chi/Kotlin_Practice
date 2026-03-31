package com.workflow.workflow.model

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

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
