package com.mapreduce.dag.spi

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode

/**
 * Pluggable execution abstraction for DAG task types — analogous to Airflow Operators
 * or Kestra task plugins.
 *
 * Each type defines config schema validation, payload assembly, and result extraction.
 * Implementations are CDI beans discovered at startup.
 *
 * Built-in types: GENERIC_HANDLER, SQL_QUERY, SQL_EXECUTE, OBJECT_FETCH,
 * OBJECT_PUT, NOTIFICATION, SUB_DAG, NOOP.
 */
interface TaskTypeHandler {
    /** Unique type identifier, matches YAML task_type field. */
    val typeId: String

    /**
     * Assemble the Layer 1 task payload from resolved config + xcom.
     * Called by the Leader at dispatch time.
     *
     * @param resolvedConfig Template-resolved node configuration.
     * @param xcomContext Output data from upstream parents, keyed by task_key.
     * @param globalContext Run input parameters.
     * @return Assembled payload for the Layer 1 task.
     */
    fun assemblePayload(
        resolvedConfig: Map<String, Any>,
        xcomContext: Map<String, JsonNode>,
        globalContext: JsonNode?,
    ): TaskPayload

    /**
     * Extract structured output from the Layer 1 task result.
     * Called by the Leader during Reconcile.
     *
     * @param rawResult The raw output from the handler.
     * @return Structured output for XCom consumption.
     */
    fun extractOutput(rawResult: JsonNode?): TaskOutput
}

/**
 * Assembled payload ready for Layer 1 task enqueue.
 */
data class TaskPayload(
    /** Serialized payload JSON. */
    val payload: String,
    /** Optional handler override (defaults to "dag.{nodeType}"). */
    val handlerOverride: String? = null,
    /** Optional queue override. */
    val queueOverride: String? = null,
)

/**
 * Structured output extracted from a task type handler's result.
 */
data class TaskOutput(
    /** Output data for XCom (will be persisted on dag_task_instance.output_data). */
    val data: String? = null,
    /** Error classification for retry decisions. */
    val errorClass: com.mapreduce.dag.model.ErrorClass? = null,
)
