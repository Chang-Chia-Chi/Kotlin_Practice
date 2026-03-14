package com.mapreduce.mr.model

import org.jdbi.v3.core.mapper.reflect.ColumnName
import java.time.Instant

/** Intermediate result produced by a map task, consumed by the reduce phase. */
data class OutputRecord(
    @ColumnName("output_id") val outputId: String,
    @ColumnName("job_id") val jobId: String,
    @ColumnName("task_id") val taskId: String,
    @ColumnName("output_data") val outputData: String,
    @ColumnName("created_at") val createdAt: Instant? = null,
)
