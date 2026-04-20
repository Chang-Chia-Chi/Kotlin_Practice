package com.workflow.dispatch.adapter.storage

import com.workflow.dispatch.model.BatchStatus

/**
 * Constructs environment-aware MinIO/S3 paths for dispatch artifacts.
 *
 * Path formats:
 * - CSV:            `env={env}/mode={mode}/dispatch/{batchToken}/simulation/{configId}.csv.gz`
 * - Prod Parquet:   `env={env}/dispatch/result.parquet`
 * - Batch Parquet:  `env={env}/dispatch/{batchToken}/result.parquet`
 */
class DispatchPathBuilder(private val env: String) {

    fun csvPath(mode: BatchStatus, batchToken: String, configId: String): String =
        "env=$env/mode=${mode.name.lowercase()}/dispatch/$batchToken/simulation/$configId.csv.gz"

    fun prodParquetPath(): String =
        "env=$env/dispatch/result.parquet"

    fun batchParquetPath(batchToken: String): String =
        "env=$env/dispatch/$batchToken/result.parquet"
}
