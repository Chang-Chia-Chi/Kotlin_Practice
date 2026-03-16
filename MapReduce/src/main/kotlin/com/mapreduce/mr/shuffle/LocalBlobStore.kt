package com.mapreduce.mr.shuffle

import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.withContext
import org.jboss.logging.Logger
import java.io.BufferedReader
import java.io.BufferedWriter
import java.nio.file.Files
import java.nio.file.Path
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
        val jobDir = baseDir.resolve(jobId)
        Files.createDirectories(jobDir)

        val blobFile = jobDir.resolve("${taskId}_p${partitionHash}.ndjson")
        val tempFile = jobDir.resolve("${taskId}_p${partitionHash}.tmp")

        val writer = Files.newBufferedWriter(
            tempFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
        )
        try {
            data.collect { record ->
                writer.write(record)
                writer.newLine()
            }
            writer.close()
            Files.move(tempFile, blobFile, java.nio.file.StandardCopyOption.ATOMIC_MOVE)
        } catch (e: Exception) {
            writer.close()
            Files.deleteIfExists(tempFile)
            throw e
        }

        val uri = blobFile.toAbsolutePath().toUri().toString()
        log.debugf("Wrote blob: %s", uri)
        uri
    }

    override suspend fun read(blobUri: String): Flow<String> {
        val path = Path.of(java.net.URI.create(blobUri))
        return flow {
            var reader: BufferedReader? = null
            try {
                reader = Files.newBufferedReader(path)
                var line = reader.readLine()
                while (line != null) {
                    emit(line)
                    line = reader.readLine()
                }
            } finally {
                reader?.close()
            }
        }.flowOn(Dispatchers.IO)
    }

    override suspend fun deleteJob(jobId: String) = withContext(Dispatchers.IO) {
        val jobDir = baseDir.resolve(jobId)
        if (Files.exists(jobDir)) {
            Files.walk(jobDir)
                .sorted(Comparator.reverseOrder())
                .forEach { Files.deleteIfExists(it) }
            log.debugf("Deleted blobs for job %s", jobId)
        }
    }
}
