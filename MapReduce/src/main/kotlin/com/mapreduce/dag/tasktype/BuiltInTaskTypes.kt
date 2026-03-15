package com.mapreduce.dag.tasktype

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.dag.spi.TaskOutput
import com.mapreduce.dag.spi.TaskPayload
import com.mapreduce.dag.spi.TaskTypeHandler
import jakarta.enterprise.context.ApplicationScoped

/**
 * Generic handler task type — delegates to a named DagNodeHandler via Layer 1.
 * This is the default task type when no explicit type is specified.
 */
@ApplicationScoped
class GenericHandlerType(private val objectMapper: ObjectMapper) : TaskTypeHandler {
    override val typeId: String = "GENERIC_HANDLER"

    override fun assemblePayload(
        resolvedConfig: Map<String, Any>,
        xcomContext: Map<String, JsonNode>,
        globalContext: JsonNode?,
    ): TaskPayload {
        val payload = mutableMapOf<String, Any?>()
        if (globalContext != null) payload["global_context"] = globalContext
        if (xcomContext.isNotEmpty()) payload["upstream"] = xcomContext
        if (resolvedConfig.isNotEmpty()) payload["config"] = resolvedConfig
        return TaskPayload(payload = objectMapper.writeValueAsString(payload))
    }

    override fun extractOutput(rawResult: JsonNode?): TaskOutput {
        return TaskOutput(data = rawResult?.toString())
    }
}

/**
 * NOOP task type — passthrough useful for join/sync points.
 * Immediately completes with no output.
 */
@ApplicationScoped
class NoopType : TaskTypeHandler {
    override val typeId: String = "NOOP"

    override fun assemblePayload(
        resolvedConfig: Map<String, Any>,
        xcomContext: Map<String, JsonNode>,
        globalContext: JsonNode?,
    ): TaskPayload = TaskPayload(payload = "{}")

    override fun extractOutput(rawResult: JsonNode?): TaskOutput = TaskOutput()
}

/**
 * SQL_QUERY task type — executes parameterized SQL and returns result set.
 * Payload includes datasource, sql, and bind parameters.
 */
@ApplicationScoped
class SqlQueryType(private val objectMapper: ObjectMapper) : TaskTypeHandler {
    override val typeId: String = "SQL_QUERY"

    override fun assemblePayload(
        resolvedConfig: Map<String, Any>,
        xcomContext: Map<String, JsonNode>,
        globalContext: JsonNode?,
    ): TaskPayload {
        val payload = mapOf(
            "task_type" to "SQL_QUERY",
            "datasource" to resolvedConfig["datasource"],
            "sql" to resolvedConfig["sql"],
            "bind" to (resolvedConfig["bind"] ?: emptyMap<String, Any>()),
        )
        return TaskPayload(payload = objectMapper.writeValueAsString(payload))
    }

    override fun extractOutput(rawResult: JsonNode?): TaskOutput {
        return TaskOutput(data = rawResult?.toString())
    }
}

/**
 * SQL_EXECUTE task type — executes DML/DDL and returns affected row count.
 */
@ApplicationScoped
class SqlExecuteType(private val objectMapper: ObjectMapper) : TaskTypeHandler {
    override val typeId: String = "SQL_EXECUTE"

    override fun assemblePayload(
        resolvedConfig: Map<String, Any>,
        xcomContext: Map<String, JsonNode>,
        globalContext: JsonNode?,
    ): TaskPayload {
        val payload = mapOf(
            "task_type" to "SQL_EXECUTE",
            "datasource" to resolvedConfig["datasource"],
            "sql" to resolvedConfig["sql"],
            "bind" to (resolvedConfig["bind"] ?: emptyMap<String, Any>()),
        )
        return TaskPayload(payload = objectMapper.writeValueAsString(payload))
    }

    override fun extractOutput(rawResult: JsonNode?): TaskOutput {
        return TaskOutput(data = rawResult?.toString())
    }
}

/**
 * NOTIFICATION task type — sends alert via configured channel.
 */
@ApplicationScoped
class NotificationType(private val objectMapper: ObjectMapper) : TaskTypeHandler {
    override val typeId: String = "NOTIFICATION"

    override fun assemblePayload(
        resolvedConfig: Map<String, Any>,
        xcomContext: Map<String, JsonNode>,
        globalContext: JsonNode?,
    ): TaskPayload {
        val payload = mapOf(
            "task_type" to "NOTIFICATION",
            "channel" to resolvedConfig["channel"],
            "template" to resolvedConfig["template"],
            "severity" to resolvedConfig["severity"],
        )
        return TaskPayload(payload = objectMapper.writeValueAsString(payload))
    }

    override fun extractOutput(rawResult: JsonNode?): TaskOutput {
        return TaskOutput(data = rawResult?.toString())
    }
}

/**
 * OBJECT_FETCH task type — retrieves object from MinIO/S3 into task workspace.
 */
@ApplicationScoped
class ObjectFetchType(private val objectMapper: ObjectMapper) : TaskTypeHandler {
    override val typeId: String = "OBJECT_FETCH"

    override fun assemblePayload(
        resolvedConfig: Map<String, Any>,
        xcomContext: Map<String, JsonNode>,
        globalContext: JsonNode?,
    ): TaskPayload {
        val payload = mapOf(
            "task_type" to "OBJECT_FETCH",
            "source_uri" to resolvedConfig["source_uri"],
            "target_key" to resolvedConfig["target_key"],
        )
        return TaskPayload(payload = objectMapper.writeValueAsString(payload))
    }

    override fun extractOutput(rawResult: JsonNode?): TaskOutput {
        return TaskOutput(data = rawResult?.toString())
    }
}

/**
 * OBJECT_PUT task type — writes task output to MinIO/S3.
 */
@ApplicationScoped
class ObjectPutType(private val objectMapper: ObjectMapper) : TaskTypeHandler {
    override val typeId: String = "OBJECT_PUT"

    override fun assemblePayload(
        resolvedConfig: Map<String, Any>,
        xcomContext: Map<String, JsonNode>,
        globalContext: JsonNode?,
    ): TaskPayload {
        val payload = mapOf(
            "task_type" to "OBJECT_PUT",
            "target_uri" to resolvedConfig["target_uri"],
            "source_ref" to resolvedConfig["source_ref"],
        )
        return TaskPayload(payload = objectMapper.writeValueAsString(payload))
    }

    override fun extractOutput(rawResult: JsonNode?): TaskOutput {
        return TaskOutput(data = rawResult?.toString())
    }
}
