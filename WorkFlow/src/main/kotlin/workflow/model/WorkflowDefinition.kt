package com.workflow.workflow.model

import java.time.Duration

data class WorkflowDefinition(
    val activities: Map<String, ActivityDefinition>,
    val start: String? = null,
    val deadline: Duration = Duration.ofHours(1),
    val staleThreshold: Duration = Duration.ofMinutes(10),
) {
    val starts: List<String> = computeRoots(activities)

    init {
        require(activities.isNotEmpty()) { "Workflow must have at least one activity" }
        require(deadline > Duration.ZERO) { "Workflow deadline must be positive" }
        require(staleThreshold > Duration.ZERO) { "Stale threshold must be positive" }

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

        require(starts.isNotEmpty()) {
            "Workflow must have at least one root activity (indegree 0); graph has no entry point"
        }

        if (start != null) {
            require(start in activities) { "Start activity '$start' not found in activities" }
            require(start in starts) {
                "Start activity '$start' is not a root (has incoming edges); auto-detected roots are $starts"
            }
        }

        // Cycle detection + unreachable check, walking from every root
        val reachable = topologicalSort(this)
        val unreachable = activities.keys - reachable.toSet()
        require(unreachable.isEmpty()) { "Unreachable activities: $unreachable" }
    }
}

private fun computeRoots(activities: Map<String, ActivityDefinition>): List<String> {
    val indegree = activities.keys.associateWith { 0 }.toMutableMap()
    for (activity in activities.values) {
        for (edge in activity.successors) {
            indegree.merge(edge.target, 1, Int::plus)
        }
    }
    return activities.keys.filter { indegree[it] == 0 }
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

    for (root in definition.starts) visit(root)
    return result.asReversed()
}
