package com.workflow.workflow.model

import java.time.Duration

data class WorkflowDefinition(
    val activities: Map<String, ActivityDefinition>,
    val start: String,
    val deadline: Duration = Duration.ofHours(1),
    val staleThreshold: Duration = Duration.ofMinutes(10),
) {
    init {
        require(activities.isNotEmpty()) { "Workflow must have at least one activity" }
        require(deadline > Duration.ZERO) { "Workflow deadline must be positive" }
        require(staleThreshold > Duration.ZERO) { "Stale threshold must be positive" }
        require(start in activities) { "Start activity '$start' not found in activities" }

        for ((name, activity) in activities) {
            for (edge in activity.successors) {
                require(edge.target in activities) {
                    "Activity '$name' has edge to unknown activity '${edge.target}'"
                }
            }
        }

        for ((name, activity) in activities) {
            require(!(activity.fanOut != null &&
                    activity.successors.any { it.label != DEFAULT_BRANCH })) {
                "Activity '$name': fanOut cannot be combined with conditional successors"
            }
        }

        require(activities.values.any { it.isTerminal }) {
            "Workflow must have at least one terminal activity (no successors and no fanOut)"
        }

        // Cycle detection + unreachable check
        val reachable = topologicalSort(this)
        val unreachable = activities.keys - reachable.toSet()
        require(unreachable.isEmpty()) { "Unreachable activities: $unreachable" }
    }
}

internal fun topologicalSort(definition: WorkflowDefinition): List<String> {
    val permanent = mutableSetOf<String>()
    val temporary = mutableSetOf<String>()
    val result = mutableListOf<String>()

    fun visit(name: String) {
        if (name in permanent) return
        require(name !in temporary) { "Cycle detected involving activity '$name'" }
        temporary += name
        val activity = definition.activities[name] ?: return
        for (edge in activity.successors) visit(edge.target)
        temporary -= name
        permanent += name
        result.add(name)
    }

    visit(definition.start)
    return result.asReversed()
}
