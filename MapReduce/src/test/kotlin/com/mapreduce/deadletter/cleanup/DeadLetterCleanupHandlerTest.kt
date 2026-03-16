package com.mapreduce.deadletter.cleanup

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.deadletter.DeadLetterMetrics
import com.mapreduce.deadletter.repository.DeadLetterRepository
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Instant

class DeadLetterCleanupHandlerTest {

    private lateinit var config: FrameworkConfig
    private lateinit var dlConfig: FrameworkConfig.DeadLetterConfig
    private lateinit var repository: DeadLetterRepository
    private lateinit var metrics: DeadLetterMetrics
    private lateinit var handler: DeadLetterCleanupHandler

    private val ctx = TaskContext(
        taskId = "cleanup-1",
        payload = "{}",
        groupId = null,
        metadata = null,
        executionGeneration = null,
    )

    @BeforeEach
    fun setUp() {
        config = mock<FrameworkConfig>()
        dlConfig = mock<FrameworkConfig.DeadLetterConfig>()
        repository = mock<DeadLetterRepository>()
        metrics = mock<DeadLetterMetrics>()

        whenever(config.deadLetter()).thenReturn(dlConfig)
        whenever(dlConfig.retentionDays()).thenReturn(30)

        handler = DeadLetterCleanupHandler(config, repository, metrics)
    }

    @Test
    fun `handler name is system dead-letter-cleanup`() {
        assertEquals("system.dead-letter-cleanup", handler.handlerName)
    }

    @Test
    fun `returns Success with deleted count in output`() = runTest {
        whenever(repository.deleteOlderThan(any())).thenReturn(15)

        val result = handler.handle(ctx)

        assertTrue(result is TaskResult.Success)
        assertEquals("""{"deleted":15}""", (result as TaskResult.Success).output)
    }

    @Test
    fun `calls repository deleteOlderThan with correct cutoff`() = runTest {
        val before = Instant.now().minusSeconds(30L * 86400 + 5)
        whenever(repository.deleteOlderThan(any())).thenReturn(0)

        handler.handle(ctx)

        val captor = argumentCaptor<Instant>()
        verify(repository).deleteOlderThan(captor.capture())

        val cutoff = captor.firstValue
        // The cutoff should be approximately 30 days ago (within a few seconds)
        val after = Instant.now().minusSeconds(30L * 86400 - 5)
        assertTrue(cutoff.isAfter(before) && cutoff.isBefore(after),
            "Cutoff $cutoff should be approximately 30 days ago")
    }

    @Test
    fun `records cleaned metrics when tasks deleted`() = runTest {
        whenever(repository.deleteOlderThan(any())).thenReturn(42)

        handler.handle(ctx)

        verify(metrics).recordCleaned(42)
    }

    @Test
    fun `does not record metrics when no tasks deleted`() = runTest {
        whenever(repository.deleteOlderThan(any())).thenReturn(0)

        handler.handle(ctx)

        verify(metrics, never()).recordCleaned(any())
    }
}
