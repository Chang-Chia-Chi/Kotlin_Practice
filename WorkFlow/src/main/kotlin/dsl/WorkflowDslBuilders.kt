package com.workflow.dsl

import java.time.Duration

@DslMarker
annotation class WorkflowDsl

@WorkflowDsl
class FanOutBuilder {
    private var transition: String? = null
    private var retries: Int = 0
    private var failurePolicy: FailurePolicy = FailurePolicy.ABORT
    private var deadline: Duration = Duration.ofMinutes(30)
    private var joinPolicy: JoinPolicy = JoinPolicy.All
    private var backoffBase: Duration = Duration.ofSeconds(1)
    private var backoffCap: Duration = Duration.ofSeconds(300)

    fun transition(t: String) { transition = t }
    fun retries(n: Int) { retries = n }
    fun failurePolicy(p: FailurePolicy) { failurePolicy = p }
    fun deadline(d: Duration) { deadline = d }
    fun joinPolicy(p: JoinPolicy) { joinPolicy = p }
    fun backoffBase(d: Duration) { backoffBase = d }
    fun backoffCap(d: Duration) { backoffCap = d }

    fun build(): FanOutDefinition {
        requireNotNull(transition) { "FanOut transition is required" }
        return FanOutDefinition(
            transition = transition!!,
            retries = retries,
            failurePolicy = failurePolicy,
            deadline = deadline,
            joinPolicy = joinPolicy,
            backoffBase = backoffBase,
            backoffCap = backoffCap,
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
    private var backoffBase: Duration = Duration.ofSeconds(1)
    private var backoffCap: Duration = Duration.ofSeconds(300)

    fun transition(t: String) { transition = t }
    fun retries(n: Int) { retries = n }
    fun failurePolicy(p: FailurePolicy) { failurePolicy = p }
    fun deadline(d: Duration) { deadline = d }
    fun backoffBase(d: Duration) { backoffBase = d }
    fun backoffCap(d: Duration) { backoffCap = d }

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
            backoffBase = backoffBase,
            backoffCap = backoffCap,
        )
    }
}

@WorkflowDsl
class WorkflowBuilder {
    private val activities = mutableListOf<ActivityDefinition>()
    private var deadline: Duration = Duration.ofHours(1)

    fun activity(name: String, block: ActivityBuilder.() -> Unit) {
        activities += ActivityBuilder().apply(block).build(name)
    }

    fun deadline(d: Duration) { deadline = d }

    fun build(): WorkflowDefinition {
        require(activities.isNotEmpty()) { "Workflow must have at least one activity" }
        return WorkflowDefinition(activities = activities.toList(), deadline = deadline)
    }
}

fun workflow(block: WorkflowBuilder.() -> Unit): WorkflowDefinition =
    WorkflowBuilder().apply(block).build()
