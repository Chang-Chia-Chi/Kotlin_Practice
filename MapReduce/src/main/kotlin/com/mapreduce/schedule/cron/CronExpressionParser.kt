package com.mapreduce.schedule.cron

import com.cronutils.model.Cron
import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.regex.Pattern

/**
 * Parses schedule expressions and computes the next fire time.
 *
 * Supports two expression formats:
 * - **Cron** — standard Quartz 7-field cron (`sec min hour dom month dow year`)
 *   or Unix 5-field cron (`min hour dom month dow`).
 * - **Interval** — `"every <N><unit>"` where unit is `s`/`m`/`h`/`d`
 *   (e.g., `"every 30s"`, `"every 5m"`, `"every 1h"`).
 */
object CronExpressionParser {

    private val INTERVAL_PATTERN: Pattern = Pattern.compile(
        "^every\\s+(\\d+)\\s*([smhd])$", Pattern.CASE_INSENSITIVE
    )

    private val quartzParser = CronParser(
        CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ)
    )

    private val unixParser = CronParser(
        CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)
    )

    /**
     * Compute the next fire time after [after] for the given [expression].
     *
     * @return the next fire instant, or `null` if no future fire time exists
     *         (e.g., a cron that will never match again).
     */
    fun nextFireTime(expression: String, after: Instant): Instant? {
        val trimmed = expression.trim()

        // Try interval syntax first
        val intervalMatch = INTERVAL_PATTERN.matcher(trimmed)
        if (intervalMatch.matches()) {
            val amount = intervalMatch.group(1).toLong()
            val unit = intervalMatch.group(2).lowercase()
            val duration = when (unit) {
                "s" -> Duration.ofSeconds(amount)
                "m" -> Duration.ofMinutes(amount)
                "h" -> Duration.ofHours(amount)
                "d" -> Duration.ofDays(amount)
                else -> throw IllegalArgumentException("Unknown interval unit: $unit")
            }
            return after.plus(duration)
        }

        // Try Quartz cron (7-field or 6-field with ? support)
        val cron = parseCron(trimmed)
        val executionTime = ExecutionTime.forCron(cron)
        val zdt = ZonedDateTime.ofInstant(after, ZoneOffset.UTC)
        return executionTime.nextExecution(zdt).orElse(null)?.toInstant()
    }

    /**
     * Validate that [expression] is a parseable schedule expression.
     *
     * @throws IllegalArgumentException if the expression is invalid.
     */
    fun validate(expression: String) {
        val trimmed = expression.trim()
        if (INTERVAL_PATTERN.matcher(trimmed).matches()) return
        parseCron(trimmed) // throws on invalid
    }

    /**
     * Returns `true` if [expression] uses the interval syntax (`every ...`).
     */
    fun isInterval(expression: String): Boolean =
        INTERVAL_PATTERN.matcher(expression.trim()).matches()

    private fun parseCron(expression: String): Cron {
        val fields = expression.trim().split("\\s+".toRegex())
        return try {
            if (fields.size <= 5) unixParser.parse(expression) else quartzParser.parse(expression)
        } catch (e: Exception) {
            throw IllegalArgumentException("Invalid cron expression: '$expression' — ${e.message}", e)
        }
    }
}
