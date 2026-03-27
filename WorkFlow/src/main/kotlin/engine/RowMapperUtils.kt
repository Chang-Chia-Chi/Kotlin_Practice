package com.workflow.engine

import java.sql.Clob
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

internal fun readClob(value: Any?): String = when (value) {
    is Clob -> value.characterStream.use { it.readText() }
    null -> ""
    else -> value.toString()
}

internal fun readTimestamp(value: Any?): Instant = when (value) {
    is LocalDateTime -> value.toInstant(ZoneOffset.UTC)
    is java.sql.Timestamp -> value.toLocalDateTime().toInstant(ZoneOffset.UTC)
    else -> {
        // Oracle JDBC returns oracle.sql.TIMESTAMP — convert via timestampValue()
        val clazz = value?.javaClass
        if (clazz?.name == "oracle.sql.TIMESTAMP") {
            val sqlTs = clazz.getMethod("timestampValue").invoke(value) as java.sql.Timestamp
            sqlTs.toLocalDateTime().toInstant(ZoneOffset.UTC)
        } else {
            throw IllegalStateException("Unexpected timestamp type: $clazz")
        }
    }
}

internal fun readNullableTimestamp(value: Any?): Instant? = when (value) {
    null -> null
    else -> readTimestamp(value)
}

internal fun caseInsensitive(row: Map<String, Any?>): Map<String, Any?> =
    java.util.TreeMap<String, Any?>(String.CASE_INSENSITIVE_ORDER).apply { putAll(row) }
