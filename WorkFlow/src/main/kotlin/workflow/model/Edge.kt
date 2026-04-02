package com.workflow.workflow.model

const val DEFAULT_BRANCH = "__default__"

data class Edge(
    val target: String,
    val label: String = DEFAULT_BRANCH,
) {
    init {
        require(target.isNotBlank()) { "Edge target must not be blank" }
    }
}
