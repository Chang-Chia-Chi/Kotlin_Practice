package com.mapreduce.workflow.shuffle

import kotlinx.coroutines.flow.Flow

/**
 * SPI for external shuffle storage.
 *
 * Step handlers stream intermediate outputs to an immutable blob store instead
 * of writing CLOBs to the relational database. The task table stores only
 * the blob URI in the output_uri column.
 *
 * Subsequent steps stream inputs directly from the blob store using URIs,
 * bypassing the database for data movement entirely.
 *
 * Implementations must support concurrent writes from multiple pods and
 * provide read-after-write consistency for the URIs returned by [write].
 */
interface BlobStore {

    /**
     * Stream data to the blob store for a given job/task.
     * Returns the URI pointing to the stored blob.
     *
     * @param jobId The owning job identifier
     * @param taskId The producing task identifier
     * @param partitionHash The partition assignment for sharded steps
     * @param data The serialized output records to store
     */
    suspend fun write(
        jobId: String,
        taskId: String,
        partitionHash: Int,
        data: Flow<String>,
    ): String

    /**
     * Stream data back from a blob URI.
     * Returns a Flow that streams records one at a time.
     */
    suspend fun read(blobUri: String): Flow<String>

    /**
     * Delete all blobs associated with a job (cleanup after completion/failure).
     */
    suspend fun deleteJob(jobId: String)
}
