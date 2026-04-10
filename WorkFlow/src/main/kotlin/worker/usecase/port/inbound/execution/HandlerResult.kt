package com.workflow.worker.usecase.port.inbound.execution

/**
 * The outcome of a transition handler executing a claimed task.
 *
 * [result] is the optional JSON payload written to `task.result_json` and
 * consumed by downstream activity inputs. [fanOutPayloads] is populated only
 * for SCATTER handlers — each string spawns one PARALLEL child task.
 *
 * A handler signals failure by throwing; there is no `Failed` variant.
 */
data class HandlerResult(
    val result: String?,
    val fanOutPayloads: List<String>? = null,
)
