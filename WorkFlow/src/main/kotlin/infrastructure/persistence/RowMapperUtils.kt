package com.workflow.infrastructure.persistence

import java.sql.Clob
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * Wall-clock zone used by the database server. All TIMESTAMP (without time zone) columns
 * in this schema hold values in this zone — either written by Oracle's `SYSTIMESTAMP`
 * (e.g. `task.enqueued_at`, `task.claimed_at`, `task.stale_at`) or by JDBC bindings that
 * convert `Instant` values through this zone. Keeping both paths in the same zone is what
 * makes JVM-side `:now` parameters comparable to `SYSTIMESTAMP`-sourced column values.
 */
val DB_ZONE: ZoneOffset = ZoneOffset.of("+08:00")

fun readClob(value: Any?): String = when (value) {
    is Clob -> value.characterStream.use { it.readText() }
    null -> ""
    else -> value.toString()
}

fun readTimestamp(value: Any?): Instant = when (value) {
    is LocalDateTime -> value.toInstant(DB_ZONE)
    is java.sql.Timestamp -> value.toLocalDateTime().toInstant(DB_ZONE)
    else -> {
        // Oracle JDBC returns oracle.sql.TIMESTAMP — convert via timestampValue()
        val clazz = value?.javaClass
        if (clazz?.name == "oracle.sql.TIMESTAMP") {
            val sqlTs = clazz.getMethod("timestampValue").invoke(value) as java.sql.Timestamp
            sqlTs.toLocalDateTime().toInstant(DB_ZONE)
        } else {
            throw IllegalStateException("Unexpected timestamp type: $clazz")
        }
    }
}

fun readNullableTimestamp(value: Any?): Instant? = when (value) {
    null -> null
    else -> readTimestamp(value)
}

fun caseInsensitive(row: Map<String, Any?>): Map<String, Any?> =
    java.util.TreeMap<String, Any?>(String.CASE_INSENSITIVE_ORDER).apply { putAll(row) }
