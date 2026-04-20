package com.workflow.workflow.model

data class TaskStatusCounts(
    val total: Int,
    val completed: Int,
    val nonTerminal: Int,
    val failed: Int,
)
