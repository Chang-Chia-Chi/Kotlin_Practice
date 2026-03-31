package com.workflow.workflow.model

import java.time.Duration

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
        for (activity in activities) {
            val target = activity.fanOut ?: continue
            require(activities.any { it.name == target }) {
                "Activity '${activity.name}' fanOut references unknown activity '$target'"
            }
        }
        for ((i, activity) in activities.withIndex()) {
            val target = activity.fanOut ?: continue
            require(i + 1 < activities.size && activities[i + 1].name == target) {
                "fanOut target '$target' must be the next activity after '${activity.name}'"
            }
        }
        for (activity in activities) {
            val target = activity.fanOut ?: continue
            val targetActivity = activities.first { it.name == target }
            require(targetActivity.fanOut == null) {
                "fanOut target '$target' cannot itself be a fanOut source"
            }
        }
    }
}
