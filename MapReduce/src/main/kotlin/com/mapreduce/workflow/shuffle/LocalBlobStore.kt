package com.mapreduce.workflow.shuffle

import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.withContext
import org.jboss.logging.Logger
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption

/**
 * Filesystem-backed [BlobStore] for development and single-node deployments.
 *
 * Stores intermediate shuffle data as newline-delimited JSON files under
 * a configurable base directory. Each blob is written atomically (write to
 * temp file, then rename) to prevent partial reads.
 *
 * In production, replace with an S3-compatible implementation.
 */
@ApplicationScoped
class LocalBlobStore : BlobStore {

    private val log = Logger.getLogger(LocalBlobStore::class.java)
    private val baseDir: Path = Path.of(System.getProperty("java.io.tmpdir"), "mapreduce-shuffle")

    init {
        Files.createDirectories(baseDir)
        log.infof("LocalBlobStore initialized at %s", baseDir)
    }

    override suspend fun write(
        jobId: String,
        taskId: String,
        partitionHash: Int,
        data: Flow<String>,
    ): String = withContext(Dispatchers.IO) {
        requireSafeSegment(jobId, "jobId")
        requireSafeSegment(taskId, "taskId")
        val jobDir = baseDir.resolve(jobId)
        Files.createDirectories(jobDir)

        val blobFile = jobDir.resolve("${taskId}_p${partitionHash}.ndjson")
        val tempFile = jobDir.resolve("${taskId}_p${partitionHash}.tmp")

        try {
            Files.newBufferedWriter(tempFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING).use { writer ->
                data.collect { record ->
                    writer.write(record)
                    writer.newLine()
                }
            }
            Files.move(tempFile, blobFile, StandardCopyOption.ATOMIC_MOVE)
        } catch (e: Exception) {
            Files.deleteIfExists(tempFile)
            throw e
        }

        val uri = blobFile.toAbsolutePath().toUri().toString()
        log.debugf("Wrote blob: %s", uri)
        uri
    }

    override suspend fun read(blobUri: String): Flow<String> {
        val path = Path.of(URI.create(blobUri))
        return flow {
            Files.newBufferedReader(path).use { reader ->
                reader.lineSequence().forEach { emit(it) }
            }
        }.flowOn(Dispatchers.IO)
    }

    override suspend fun deleteJob(jobId: String) = withContext(Dispatchers.IO) {
        requireSafeSegment(jobId, "jobId")
        val jobDir = baseDir.resolve(jobId)
        if (Files.exists(jobDir)) {
            Files.walk(jobDir)
                .sorted(Comparator.reverseOrder())
                .forEach { Files.deleteIfExists(it) }
            log.debugf("Deleted blobs for job %s", jobId)
        }
    }

    private fun requireSafeSegment(value: String, name: String) {
        require(value.isNotEmpty() && !value.contains('/') && !value.contains('\\') && value != ".." && value != ".") {
            "$name contains unsafe path characters: $value"
        }
    }
}
