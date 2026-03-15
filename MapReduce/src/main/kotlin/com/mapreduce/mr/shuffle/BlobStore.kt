package com.mapreduce.mr.shuffle

import kotlinx.coroutines.flow.Flow

/**
 * SPI for external shuffle storage.
 *
 * Map workers stream intermediate outputs to an immutable blob store instead
 * of writing CLOBs to the relational database. The `mr_output` table stores
 * only the routing partition hash and the blob URI.
 *
 * Reduce workers stream inputs directly from the blob store using URIs,
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
     * @param partitionHash The partition assignment for sharded reduce
     * @param data The serialized output records to store
     */
    suspend fun write(
        jobId: String,
        taskId: String,
        partitionHash: Int,
        data: Flow<String>,
    ): String

    /**
     * Stream data back from a blob URI for the reduce phase.
     * Returns a Flow that streams records one at a time.
     */
    suspend fun read(blobUri: String): Flow<String>

    /**
     * Delete all blobs associated with a job (cleanup after completion/failure).
     */
    suspend fun deleteJob(jobId: String)
}
