package com.workflow.workflow.usecase.port.outbound.persistent

import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import org.jdbi.v3.core.Handle
import java.time.Duration

interface WorkflowRepository {
    suspend fun insert(run: WorkflowRun)
    suspend fun findById(id: String): WorkflowRun?
    suspend fun casVersion(id: String, expectedVersion: Int): Boolean
    suspend fun updateStatus(id: String, newStatus: WorkflowStatus, expectedStatus: WorkflowStatus): Boolean
    suspend fun findStuck(gracePeriod: Duration): List<WorkflowRun>

    fun insertWithHandle(handle: Handle, run: WorkflowRun)
    fun findByIdWithHandle(handle: Handle, id: String): WorkflowRun?
    fun casVersionWithHandle(handle: Handle, id: String, expectedVersion: Int): Boolean
    fun updateStatusWithHandle(handle: Handle, id: String, newStatus: WorkflowStatus, expectedStatus: WorkflowStatus): Boolean
    fun mergeIdempotentWithHandle(handle: Handle, run: WorkflowRun, idempotencyKey: String): Pair<String, Boolean>
    fun expireOverdueWithHandle(handle: Handle, now: java.time.LocalDateTime): Int
}
