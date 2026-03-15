package com.mapreduce.dag.template

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

/**
 * Resolves `{{ }}` Mustache-style template expressions in DAG node configs.
 *
 * Supported namespaces:
 * - `{{ inputs.<name> }}` — Run input parameters (from global_context)
 * - `{{ xcom.<task_key>.<field> }}` — Upstream node's output_data field
 * - `{{ xcom.*.<field> }}` — Merged output from all completed upstream parents
 * - `{{ run.run_id }}` — Current Run UUID
 * - `{{ run.dag_id }}` — Blueprint identifier
 *
 * Templates are evaluated lazily at dispatch time by the Leader.
 * Unresolvable references cause the node to fail with a diagnostic error.
 */
class TemplateEngine(private val objectMapper: ObjectMapper) {

    private val templatePattern = Regex("""\{\{\s*(.+?)\s*}}""")

    /**
     * Context for template resolution, built by the orchestrator at dispatch time.
     */
    data class ResolutionContext(
        val runId: String,
        val dagId: String,
        val inputs: JsonNode?,
        val xcom: Map<String, JsonNode>,
    )

    /**
     * Resolve all `{{ }}` expressions in the given string value.
     *
     * @throws UnresolvableTemplateException if a reference cannot be resolved.
     */
    fun resolve(template: String, ctx: ResolutionContext): String {
        return templatePattern.replace(template) { match ->
            val expr = match.groupValues[1].trim()
            resolveExpression(expr, ctx)
                ?: throw UnresolvableTemplateException(expr, template)
        }
    }

    /**
     * Resolve all template expressions within a JSON config map.
     * Returns a new map with all string values resolved.
     */
    fun resolveConfig(
        config: Map<String, Any>,
        ctx: ResolutionContext,
    ): Map<String, Any> {
        return config.mapValues { (_, value) -> resolveValue(value, ctx) }
    }

    @Suppress("UNCHECKED_CAST")
    private fun resolveValue(value: Any, ctx: ResolutionContext): Any {
        return when (value) {
            is String -> resolve(value, ctx)
            is Map<*, *> -> (value as Map<String, Any>).mapValues { (_, v) -> resolveValue(v, ctx) }
            is List<*> -> value.map { resolveValue(it ?: "", ctx) }
            else -> value
        }
    }

    private fun resolveExpression(expr: String, ctx: ResolutionContext): String? {
        val parts = expr.split(".")
        if (parts.isEmpty()) return null

        return when (parts[0]) {
            "inputs" -> resolveInputs(parts.drop(1), ctx.inputs)
            "xcom" -> resolveXcom(parts.drop(1), ctx.xcom)
            "run" -> resolveRun(parts.getOrNull(1), ctx)
            else -> null
        }
    }

    private fun resolveInputs(path: List<String>, inputs: JsonNode?): String? {
        if (inputs == null || path.isEmpty()) return null
        var node: JsonNode? = inputs
        for (key in path) {
            node = node?.get(key) ?: return null
        }
        return nodeToString(node)
    }

    private fun resolveXcom(path: List<String>, xcom: Map<String, JsonNode>): String? {
        if (path.isEmpty()) return null

        val taskKey = path[0]
        val fieldPath = path.drop(1)

        if (taskKey == "*") {
            return resolveWildcardXcom(fieldPath, xcom)
        }

        val taskOutput = xcom[taskKey] ?: return null
        if (fieldPath.isEmpty()) return nodeToString(taskOutput)

        var node: JsonNode? = taskOutput
        for (key in fieldPath) {
            node = node?.get(key) ?: return null
        }
        return nodeToString(node)
    }

    /**
     * `{{ xcom.* }}` or `{{ xcom.*.<field> }}` — merge outputs from all upstream parents.
     * Keys are task_key, conflicts resolved by insertion order.
     */
    private fun resolveWildcardXcom(fieldPath: List<String>, xcom: Map<String, JsonNode>): String? {
        if (fieldPath.isEmpty()) {
            val merged = objectMapper.createObjectNode()
            xcom.forEach { (key, value) -> merged.set<JsonNode>(key, value) }
            return objectMapper.writeValueAsString(merged)
        }

        val fieldName = fieldPath[0]
        for ((_, output) in xcom) {
            val value = output.get(fieldName)
            if (value != null) return nodeToString(value)
        }
        return null
    }

    private fun resolveRun(field: String?, ctx: ResolutionContext): String? {
        return when (field) {
            "run_id" -> ctx.runId
            "dag_id" -> ctx.dagId
            else -> null
        }
    }

    private fun nodeToString(node: JsonNode?): String? {
        if (node == null) return null
        return if (node.isTextual) node.asText() else node.toString()
    }
}

class UnresolvableTemplateException(
    val expression: String,
    val template: String,
) : RuntimeException("Unresolvable template expression '{{ $expression }}' in: $template")
