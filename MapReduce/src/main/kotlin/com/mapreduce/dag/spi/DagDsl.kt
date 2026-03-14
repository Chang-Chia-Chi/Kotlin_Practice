package com.mapreduce.dag.spi

import com.mapreduce.dag.model.TriggerRule

/**
 * Kotlin DSL for declarative DAG definitions, inspired by Airflow's task dependencies.
 *
 * ## Usage — Airflow-style
 *
 * ```kotlin
 * @ApplicationScoped
 * class OrderPipeline : DagBlueprint {
 *     override val dagId = "order-pipeline"
 *
 *     override fun nodes() = buildDag {
 *         val validate  = node("validate",  "order.validate")
 *         val enrich    = node("enrich",    "data.enrich")
 *         val payment   = node("payment",   "payment.process")
 *         val ship      = node("ship",      "shipping.dispatch", triggerRule = TriggerRule.ALL_DONE)
 *         val notify    = node("notify",    "notification.send")
 *
 *         //  validate ──┬── enrich  ──┬── ship ── notify
 *         //             └── payment ──┘
 *         validate then listOf(enrich, payment)   // fan-out
 *         listOf(enrich, payment) then ship        // fan-in
 *         ship then notify                         // linear
 *     }
 * }
 * ```
 *
 * ## Usage — Linear chain
 *
 * ```kotlin
 * override fun nodes() = buildDag {
 *     val extract   = node("extract",   "etl.extract")
 *     val transform = node("transform", "etl.transform")
 *     val load      = node("load",      "etl.load")
 *
 *     extract then transform then load
 * }
 * ```
 *
 * ## Usage — Dynamic routing
 *
 * A node handler can return `{"__dag_route__": ["branch-a"]}` to prune
 * non-routed downstream branches at runtime (see spec §4).
 *
 * ```kotlin
 * override fun nodes() = buildDag {
 *     val router   = node("router",   "order.route")
 *     val branchA  = node("branch-a", "order.domestic")
 *     val branchB  = node("branch-b", "order.international")
 *     val finalize = node("finalize", "order.finalize", triggerRule = TriggerRule.ONE_SUCCESS)
 *
 *     router then listOf(branchA, branchB)
 *     listOf(branchA, branchB) then finalize
 * }
 * ```
 */
fun buildDag(block: DagBuilder.() -> Unit): List<DagNodeDef> {
    val builder = DagBuilder()
    builder.block()
    return builder.build()
}

/**
 * Convenience factory: create a [DagBlueprint] from a DSL block.
 *
 * Useful for tests or programmatic DAG creation:
 * ```kotlin
 * val blueprint = dag("my-pipeline") {
 *     val a = node("a", "step.one")
 *     val b = node("b", "step.two")
 *     a then b
 * }
 * ```
 */
fun dag(id: String, block: DagBuilder.() -> Unit): DagBlueprint {
    val nodes = buildDag(block)
    return object : DagBlueprint {
        override val dagId = id
        override fun nodes() = nodes
    }
}

@DslMarker
annotation class DagDslMarker

@DagDslMarker
class DagBuilder {
    private val nodes = mutableListOf<NodeData>()
    private val edges = mutableListOf<Pair<String, String>>()

    /**
     * Declare a node in the DAG.
     *
     * @param taskKey Unique logical name within this DAG (e.g. `"validate-order"`).
     * @param nodeType Maps to a [DagNodeHandler.nodeType] (handler = `"dag.{nodeType}"`).
     * @param triggerRule When to fire this node relative to its upstream parents.
     */
    fun node(
        taskKey: String,
        nodeType: String,
        triggerRule: TriggerRule = TriggerRule.ALL_SUCCESS,
    ): NodeRef {
        nodes.add(NodeData(taskKey, nodeType, triggerRule))
        return NodeRef(taskKey, this)
    }

    internal fun addEdge(from: String, to: String) {
        edges.add(from to to)
    }

    fun build(): List<DagNodeDef> {
        val depsMap = mutableMapOf<String, MutableSet<String>>()
        for ((from, to) in edges) {
            depsMap.getOrPut(to) { mutableSetOf() }.add(from)
        }
        return nodes.map { nd ->
            DagNodeDef(
                taskKey = nd.taskKey,
                nodeType = nd.nodeType,
                dependencies = depsMap[nd.taskKey]?.toList() ?: emptyList(),
                triggerRule = nd.triggerRule,
            )
        }
    }

    private data class NodeData(
        val taskKey: String,
        val nodeType: String,
        val triggerRule: TriggerRule,
    )
}

/**
 * A reference to a declared DAG node, used for building edges via [then].
 *
 * Mirrors Airflow's `>>` operator:
 * ```
 * a then b          // a >> b  (linear)
 * a then [b, c]     // a >> [b, c]  (fan-out)
 * [b, c] then d     // [b, c] >> d  (fan-in)
 * ```
 */
class NodeRef internal constructor(
    val taskKey: String,
    @PublishedApi internal val builder: DagBuilder,
) {
    /** Linear: `a then b` — b depends on a. Returns b for chaining. */
    infix fun then(next: NodeRef): NodeRef {
        builder.addEdge(this.taskKey, next.taskKey)
        return next
    }

    /** Fan-out: `a then listOf(b, c)` — b and c both depend on a. */
    infix fun then(nexts: List<NodeRef>): List<NodeRef> {
        nexts.forEach { builder.addEdge(this.taskKey, it.taskKey) }
        return nexts
    }
}

/** Fan-in: `listOf(a, b) then c` — c depends on both a and b. Returns c for chaining. */
infix fun List<NodeRef>.then(next: NodeRef): NodeRef {
    forEach { it then next }
    return next
}

/** Fan-out from multiple: `listOf(a, b) then listOf(c, d)` — cartesian edges. */
infix fun List<NodeRef>.then(nexts: List<NodeRef>): List<NodeRef> {
    forEach { src -> nexts.forEach { dst -> src then dst } }
    return nexts
}
