package com.workflow.dispatch.model

import java.time.LocalDateTime

data class DispatchBatch(
    val batchToken: String,
    val status: BatchStatus,
    val createdAt: LocalDateTime,
    val configCount: Int,
)
