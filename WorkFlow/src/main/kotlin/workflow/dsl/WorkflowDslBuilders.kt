package com.workflow.workflow.dsl

import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.WorkflowDefinition
import java.time.Duration

@DslMarker
annotation class WorkflowDsl

@WorkflowDsl
class InputsBuilder {
    private val entries = mutableMapOf<String, String>()

    infix fun String.from(ref: String) {
        entries[this] = ref
    }

    fun build(): Map<String, String> = entries.toMap()
}

@WorkflowDsl
class ActivityBuilder {
    private var transition: String? = null
    private var retries: Int = 0
    private var failurePolicy: FailurePolicy = FailurePolicy.ABORT
    private var deadline: Duration = Duration.ofMinutes(30)
    private var fanOutTarget: String? = null
    private var joinPolicy: JoinPolicy = JoinPolicy.All
    private var backoffBase: Duration = Duration.ofSeconds(1)
    private var backoffCap: Duration = Duration.ofSeconds(300)
    private var queue: String = "default"
    private var inputsDef: Map<String, String> = emptyMap()

    fun transition(t: String) { transition = t }
    fun retries(n: Int) { retries = n }
    fun failurePolicy(p: FailurePolicy) { failurePolicy = p }
    fun deadline(d: Duration) { deadline = d }
    fun backoffBase(d: Duration) { backoffBase = d }
    fun backoffCap(d: Duration) { backoffCap = d }
    fun queue(q: String) { queue = q }

    fun fanOut(target: String) { fanOutTarget = target }
    fun joinPolicy(p: JoinPolicy) { joinPolicy = p }

    fun inputs(block: InputsBuilder.() -> Unit) {
        inputsDef = InputsBuilder().apply(block).build()
    }

    fun build(name: String): ActivityDefinition {
        requireNotNull(transition) { "Activity '$name' transition is required" }
        return ActivityDefinition(
            name = name,
            transition = transition!!,
            retries = retries,
            failurePolicy = failurePolicy,
            deadline = deadline,
            fanOut = fanOutTarget,
            joinPolicy = joinPolicy,
            backoffBase = backoffBase,
            backoffCap = backoffCap,
            queue = queue,
            inputs = inputsDef,
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
