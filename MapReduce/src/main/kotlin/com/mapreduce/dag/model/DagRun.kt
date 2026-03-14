package com.mapreduce.dag.model

import org.jdbi.v3.core.mapper.reflect.ColumnName
import java.time.Instant

enum class DagRunStatus {
    RUNNING, COMPLETED, FAILED
}

/**
 * A specific execution instance of a DAG Blueprint.
 * Acts as the correlation boundary (`group_id`) for underlying Layer 1 tasks.
 */
data class DagRun(
    @ColumnName("run_id") val runId: String,
    @ColumnName("dag_id") val dagId: String,
    val status: DagRunStatus,
    @ColumnName("global_context") val globalContext: String? = null,
    @ColumnName("created_at") val createdAt: Instant? = null,
    @ColumnName("updated_at") val updatedAt: Instant? = null,
)
