package com.mapreduce.workflow.shuffle

import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.assertThrows

class LocalBlobStoreTest {

    private lateinit var store: LocalBlobStore
    private lateinit var baseDir: Path

    @BeforeEach
    fun setUp() {
        store = LocalBlobStore()
        baseDir = Path.of(System.getProperty("java.io.tmpdir"), "mapreduce-shuffle")
    }

    @AfterEach
    fun tearDown() = runTest {
        listOf("test-job-1", "test-job-2", "test-job-roundtrip", "test-job-del", "test-job-atomic").forEach {
            try { store.deleteJob(it) } catch (_: Exception) { }
        }
    }

    @Test
    fun `write creates file with correct content`() = runTest {
        val data = flowOf("line1", "line2", "line3")

        val uri = store.write("test-job-1", "task-a", 0, data)

        assertTrue(uri.startsWith("file://"))
        val path = Path.of(java.net.URI.create(uri))
        assertTrue(Files.exists(path))

        val lines = Files.readAllLines(path)
        assertEquals(listOf("line1", "line2", "line3"), lines)
    }

    @Test
    fun `read returns all lines from file`() = runTest {
        val data = flowOf("alpha", "beta", "gamma")
        val uri = store.write("test-job-2", "task-b", 0, data)

        val result = store.read(uri).toList()

        assertEquals(listOf("alpha", "beta", "gamma"), result)
    }

    @Test
    fun `write and read round-trip preserves data`() = runTest {
        val original = listOf(
            """{"word":"hello","count":1}""",
            """{"word":"world","count":2}""",
        )
        val uri = store.write("test-job-roundtrip", "task-rt", 5, flowOf(*original.toTypedArray()))

        val restored = store.read(uri).toList()

        assertEquals(original, restored)
    }

    @Test
    fun `deleteJob removes all blobs for job`() = runTest {
        store.write("test-job-del", "task-1", 0, flowOf("a"))
        store.write("test-job-del", "task-2", 1, flowOf("b"))

        val jobDir = baseDir.resolve("test-job-del")
        assertTrue(Files.exists(jobDir))

        store.deleteJob("test-job-del")

        assertFalse(Files.exists(jobDir))
    }

    @Test
    fun `write is atomic -- temp file is cleaned up on success`() = runTest {
        val data = flowOf("data")
        store.write("test-job-atomic", "task-at", 0, data)

        val jobDir = baseDir.resolve("test-job-atomic")
        val tempFile = jobDir.resolve("task-at_p0.tmp")
        val finalFile = jobDir.resolve("task-at_p0.ndjson")

        assertFalse(Files.exists(tempFile))
        assertTrue(Files.exists(finalFile))
    }

    @Test
    fun `write creates file with partition hash in filename`() = runTest {
        val uri = store.write("test-job-1", "task-ph", 7, flowOf("x"))

        assertTrue(uri.contains("task-ph_p7.ndjson"))
    }

    @Test
    fun `write rejects path traversal in jobId`() = runTest {
        assertThrows<IllegalArgumentException> {
            store.write("../escape", "task-1", 0, flowOf("x"))
        }
    }

    @Test
    fun `write rejects path traversal in taskId`() = runTest {
        assertThrows<IllegalArgumentException> {
            store.write("job-1", "../../etc/passwd", 0, flowOf("x"))
        }
    }

    @Test
    fun `deleteJob rejects path traversal`() = runTest {
        assertThrows<IllegalArgumentException> {
            store.deleteJob("../escape")
        }
    }
}
