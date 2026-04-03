package com.workflow.workflow.dsl

import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.DEFAULT_BRANCH
import com.workflow.workflow.model.Edge
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.FanOutDefinition
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.WorkflowDefinition
import java.time.Duration

@DslMarker
annotation class WorkflowDsl

@WorkflowDsl
class InputsBuilder {
    private val entries = mutableMapOf<String, String>()

    infix fun String.from(ref: String) { entries[this] = ref }

    fun build(): Map<String, String> = entries.toMap()
}

@WorkflowDsl
class BranchBuilder {
    private val targets = mutableListOf<String>()

    fun next(t: String) { targets += t }

    fun buildEdges(label: String): List<Edge> = targets.map { Edge(it, label) }
}

@WorkflowDsl
class FanOutBuilder {
    private var transition: String? = null
    private var retries: Int = 0
    private var failurePolicy: FailurePolicy = FailurePolicy.ABORT
    private var deadline: Duration = Duration.ofMinutes(30)
    private var joinPolicy: JoinPolicy = JoinPolicy.All
    private var backoffBase: Duration = Duration.ofSeconds(1)
    private var backoffCap: Duration = Duration.ofSeconds(300)
    private var queue: String = "default"

    fun transition(t: String) { transition = t }
    fun retries(n: Int) { retries = n }
    fun failurePolicy(p: FailurePolicy) { failurePolicy = p }
    fun deadline(d: Duration) { deadline = d }
    fun joinPolicy(p: JoinPolicy) { joinPolicy = p }
    fun backoffBase(d: Duration) { backoffBase = d }
    fun backoffCap(d: Duration) { backoffCap = d }
    fun queue(q: String) { queue = q }

    fun build(): FanOutDefinition {
        requireNotNull(transition) { "fanOut transition is required" }
        return FanOutDefinition(
            transition = transition!!,
            retries = retries,
            failurePolicy = failurePolicy,
            deadline = deadline,
            joinPolicy = joinPolicy,
            backoffBase = backoffBase,
            backoffCap = backoffCap,
            queue = queue,
        )
    }
}

@WorkflowDsl
class ActivityBuilder(private val name: String) {
    private var transition: String? = null
    private var retries: Int = 0
    private var failurePolicy: FailurePolicy = FailurePolicy.ABORT
    private var deadline: Duration = Duration.ofMinutes(30)
    private var backoffBase: Duration = Duration.ofSeconds(1)
    private var backoffCap: Duration = Duration.ofSeconds(300)
    private var queue: String = "default"
    private var inputsDef: Map<String, String> = emptyMap()
    private var fanOutDef: FanOutDefinition? = null
    private val successorEdges = mutableListOf<Edge>()
    private var hasConditional = false
    private var hasUnconditional = false

    fun transition(t: String) { transition = t }
    fun retries(n: Int) { retries = n }
    fun failurePolicy(p: FailurePolicy) { failurePolicy = p }
    fun deadline(d: Duration) { deadline = d }
    fun backoffBase(d: Duration) { backoffBase = d }
    fun backoffCap(d: Duration) { backoffCap = d }
    fun queue(q: String) { queue = q }

    fun inputs(block: InputsBuilder.() -> Unit) {
        inputsDef = InputsBuilder().apply(block).build()
    }

    fun next(target: String) {
        require(!hasConditional) {
            "Activity '$name': cannot mix next() and on() — use one or the other"
        }
        hasUnconditional = true
        successorEdges += Edge(target, DEFAULT_BRANCH)
    }

    fun on(label: String, block: BranchBuilder.() -> Unit) {
        require(!hasUnconditional) {
            "Activity '$name': cannot mix next() and on() — use one or the other"
        }
        hasConditional = true
        successorEdges += BranchBuilder().apply(block).buildEdges(label)
    }

    fun fanOut(block: FanOutBuilder.() -> Unit) {
        fanOutDef = FanOutBuilder().apply(block).build()
    }

    fun build(): ActivityDefinition {
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
            queue = queue,
            inputs = inputsDef,
            successors = successorEdges.toList(),
        )
    }
}

@WorkflowDsl
class WorkflowBuilder {
    private val activities = mutableMapOf<String, ActivityDefinition>()
    private var startName: String? = null
    private var deadline: Duration = Duration.ofHours(1)

    fun start(name: String) { startName = name }

    fun activity(name: String, block: ActivityBuilder.() -> Unit) {
        if (startName == null) startName = name  // first activity is default start
        activities[name] = ActivityBuilder(name).apply(block).build()
    }

    fun deadline(d: Duration) { deadline = d }

    fun build(): WorkflowDefinition {
        require(activities.isNotEmpty()) { "Workflow must have at least one activity" }
        return WorkflowDefinition(
            activities = activities.toMap(),
            start = requireNotNull(startName) { "Workflow start activity is required" },
            deadline = deadline,
        )
    }
}

fun workflow(block: WorkflowBuilder.() -> Unit): WorkflowDefinition =
    WorkflowBuilder().apply(block).build()
