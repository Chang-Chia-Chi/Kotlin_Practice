package com.mapreduce.dag.template

/**
 * Minimal, sandboxed expression engine for evaluating node conditions.
 *
 * Supports:
 * - Equality: `==`, `!=`
 * - Comparison: `>`, `>=`, `<`, `<=`
 * - Boolean: `&&`, `||`, `!`
 * - Null checks: `== null`, `!= null`
 * - String literals: `'value'` or `"value"`
 * - IN lists: `value IN ('A', 'B', 'C')`
 * - Boolean literals: `true`, `false`
 *
 * Template expressions (`{{ }}`) must be resolved BEFORE passing to this evaluator.
 *
 * No arbitrary code execution — only field access, comparison, and boolean logic.
 */
class ConditionEvaluator {

    /**
     * Evaluate a condition expression to a boolean.
     *
     * @param expression The resolved condition string (templates already substituted).
     * @return true if the condition is satisfied.
     * @throws ConditionEvaluationException if the expression is malformed.
     */
    fun evaluate(expression: String): Boolean {
        val trimmed = expression.trim()
        if (trimmed.isBlank()) return true
        if (trimmed.equals("true", ignoreCase = true)) return true
        if (trimmed.equals("false", ignoreCase = true)) return false

        return try {
            evaluateExpr(trimmed)
        } catch (e: ConditionEvaluationException) {
            throw e
        } catch (e: Exception) {
            throw ConditionEvaluationException(expression, e)
        }
    }

    private fun evaluateExpr(expr: String): Boolean {
        // Handle OR (lowest precedence)
        val orParts = splitOutsideQuotes(expr, "||")
        if (orParts.size > 1) {
            return orParts.any { evaluateExpr(it.trim()) }
        }

        // Handle AND
        val andParts = splitOutsideQuotes(expr, "&&")
        if (andParts.size > 1) {
            return andParts.all { evaluateExpr(it.trim()) }
        }

        // Handle NOT
        val trimmed = expr.trim()
        if (trimmed.startsWith("!") && !trimmed.startsWith("!=")) {
            return !evaluateExpr(trimmed.substring(1).trim())
        }

        // Handle parentheses
        if (trimmed.startsWith("(") && findClosingParen(trimmed, 0) == trimmed.length - 1) {
            return evaluateExpr(trimmed.substring(1, trimmed.length - 1))
        }

        // Handle IN operator
        val inMatch = Regex("""(.+?)\s+IN\s*\((.+)\)""", RegexOption.IGNORE_CASE).matchEntire(trimmed)
        if (inMatch != null) {
            val value = extractValue(inMatch.groupValues[1].trim())
            val items = inMatch.groupValues[2].split(",").map { extractValue(it.trim()) }
            return value in items
        }

        // Handle comparison operators
        return evaluateComparison(trimmed)
    }

    private fun evaluateComparison(expr: String): Boolean {
        // Order matters — check two-char operators before single-char
        for (op in listOf("!=", "==", ">=", "<=", ">", "<")) {
            val parts = splitOnOperator(expr, op)
            if (parts != null) {
                val left = extractValue(parts.first.trim())
                val right = extractValue(parts.second.trim())
                return compareValues(left, right, op)
            }
        }
        throw ConditionEvaluationException(expr, null)
    }

    private fun compareValues(left: String?, right: String?, op: String): Boolean {
        return when (op) {
            "==" -> left == right
            "!=" -> left != right
            ">", ">=", "<", "<=" -> {
                val leftNum = left?.toDoubleOrNull()
                val rightNum = right?.toDoubleOrNull()
                if (leftNum != null && rightNum != null) {
                    when (op) {
                        ">" -> leftNum > rightNum
                        ">=" -> leftNum >= rightNum
                        "<" -> leftNum < rightNum
                        "<=" -> leftNum <= rightNum
                        else -> false
                    }
                } else {
                    // Lexicographic comparison for non-numeric values
                    val cmp = (left ?: "").compareTo(right ?: "")
                    when (op) {
                        ">" -> cmp > 0
                        ">=" -> cmp >= 0
                        "<" -> cmp < 0
                        "<=" -> cmp <= 0
                        else -> false
                    }
                }
            }
            else -> throw ConditionEvaluationException("Unknown operator: $op", null)
        }
    }

    /** Extract the value from a string, removing surrounding quotes. */
    private fun extractValue(raw: String): String? {
        val trimmed = raw.trim()
        if (trimmed.equals("null", ignoreCase = true)) return null
        if (trimmed.startsWith("'") && trimmed.endsWith("'")) return trimmed.substring(1, trimmed.length - 1)
        if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) return trimmed.substring(1, trimmed.length - 1)
        return trimmed
    }

    /** Split on an operator, but not inside quoted strings. */
    private fun splitOnOperator(expr: String, op: String): Pair<String, String>? {
        var inQuote: Char? = null
        var i = 0
        while (i <= expr.length - op.length) {
            val c = expr[i]
            if (c == '\'' || c == '"') {
                inQuote = if (inQuote == c) null else if (inQuote == null) c else inQuote
            }
            if (inQuote == null && expr.substring(i).startsWith(op)) {
                // Avoid matching == when looking for = or >= when looking for >
                return Pair(expr.substring(0, i), expr.substring(i + op.length))
            }
            i++
        }
        return null
    }

    private fun splitOutsideQuotes(expr: String, delimiter: String): List<String> {
        val parts = mutableListOf<String>()
        var inQuote: Char? = null
        var depth = 0
        var start = 0
        var i = 0
        while (i <= expr.length - delimiter.length) {
            val c = expr[i]
            if (c == '\'' || c == '"') {
                inQuote = if (inQuote == c) null else if (inQuote == null) c else inQuote
            }
            if (inQuote == null) {
                if (c == '(') depth++
                if (c == ')') depth--
            }
            if (inQuote == null && depth == 0 && expr.substring(i).startsWith(delimiter)) {
                parts.add(expr.substring(start, i))
                start = i + delimiter.length
                i = start
                continue
            }
            i++
        }
        parts.add(expr.substring(start))
        return parts
    }

    private fun findClosingParen(expr: String, openIndex: Int): Int {
        var depth = 0
        for (i in openIndex until expr.length) {
            when (expr[i]) {
                '(' -> depth++
                ')' -> {
                    depth--
                    if (depth == 0) return i
                }
            }
        }
        return -1
    }
}

class ConditionEvaluationException(
    val expression: String,
    cause: Throwable?,
) : RuntimeException("Failed to evaluate condition expression: $expression", cause)
