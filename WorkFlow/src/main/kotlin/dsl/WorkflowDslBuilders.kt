package com.workflow.dsl

import java.time.Duration

@DslMarker
annotation class WorkflowDsl

@WorkflowDsl
class JoinBuilder {
    private var policy: JoinPolicy = JoinPolicy.All
    private var transition: String? = null

    fun policy(p: JoinPolicy) { policy = p }
    fun transition(t: String) { transition = t }

    fun build(): JoinDefinition = JoinDefinition(policy = policy, transition = transition)
}

@WorkflowDsl
class FanOutBuilder {
    private var transition: String? = null
    private var retries: Int = 0
    private var failurePolicy: FailurePolicy = FailurePolicy.ABORT
    private var deadline: Duration = Duration.ofMinutes(30)
    private var joinDef: JoinDefinition? = null

    fun transition(t: String) { transition = t }
    fun retries(n: Int) { retries = n }
    fun failurePolicy(p: FailurePolicy) { failurePolicy = p }
    fun deadline(d: Duration) { deadline = d }

    fun join(block: JoinBuilder.() -> Unit) {
        joinDef = JoinBuilder().apply(block).build()
    }

    fun build(): FanOutDefinition {
        requireNotNull(transition) { "FanOut transition is required" }
        requireNotNull(joinDef) { "FanOut join is required" }
        return FanOutDefinition(
            transition = transition!!,
            retries = retries,
            failurePolicy = failurePolicy,
            deadline = deadline,
            join = joinDef!!,
        )
    }
}

@WorkflowDsl
class ActivityBuilder {
    private var transition: String? = null
    private var retries: Int = 0
    private var failurePolicy: FailurePolicy = FailurePolicy.ABORT
    private var deadline: Duration = Duration.ofMinutes(30)
    private var fanOutDef: FanOutDefinition? = null

    fun transition(t: String) { transition = t }
    fun retries(n: Int) { retries = n }
    fun failurePolicy(p: FailurePolicy) { failurePolicy = p }
    fun deadline(d: Duration) { deadline = d }

    fun fanOut(block: FanOutBuilder.() -> Unit) {
        fanOutDef = FanOutBuilder().apply(block).build()
    }

    fun build(name: String): ActivityDefinition {
        requireNotNull(transition) { "Activity '$name' transition is required" }
        return ActivityDefinition(
            name = name,
            transition = transition!!,
            retries = retries,
            failurePolicy = failurePolicy,
            deadline = deadline,
            fanOut = fanOutDef,
        )
    }
}

@WorkflowDsl
class WorkflowBuilder {
    private val activities = mutableListOf<ActivityDefinition>()

    fun activity(name: String, block: ActivityBuilder.() -> Unit) {
        activities += ActivityBuilder().apply(block).build(name)
    }

    fun build(): WorkflowDefinition {
        require(activities.isNotEmpty()) { "Workflow must have at least one activity" }
        return WorkflowDefinition(activities = activities.toList())
    }
}

fun workflow(block: WorkflowBuilder.() -> Unit): WorkflowDefinition =
    WorkflowBuilder().apply(block).build()
