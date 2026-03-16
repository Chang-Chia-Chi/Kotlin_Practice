package com.mapreduce.deadletter

import com.mapreduce.deadletter.api.dto.BulkReplayFilter
import com.mapreduce.deadletter.repository.DeadLetterRepository
import com.mapreduce.deadletter.repository.GroupSummary
import com.mapreduce.deadletter.repository.HandlerSummary
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskStatus
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import java.time.Instant

class DeadLetterServiceTest {

    private lateinit var repository: DeadLetterRepository
    private lateinit var service: DeadLetterService

    @BeforeEach
    fun setUp() {
        repository = mock(DeadLetterRepository::class.java)
        service = DeadLetterService(repository)
    }

    // ── list ────────────────────────────────────────────────────────

    @Test
    fun `list clamps limit below 1 to 1`() {
        `when`(repository.findDeadLetters(null, null, null, null, null, 1, 0))
            .thenReturn(emptyList())

        service.list(null, null, null, null, null, limit = -5, offset = 0)

        verify(repository).findDeadLetters(null, null, null, null, null, 1, 0)
    }

    @Test
    fun `list clamps limit above 200 to 200`() {
        `when`(repository.findDeadLetters(null, null, null, null, null, 200, 0))
            .thenReturn(emptyList())

        service.list(null, null, null, null, null, limit = 999, offset = 0)

        verify(repository).findDeadLetters(null, null, null, null, null, 200, 0)
    }

    @Test
    fun `list preserves limit within valid range`() {
        `when`(repository.findDeadLetters(null, null, null, null, null, 50, 0))
            .thenReturn(emptyList())

        service.list(null, null, null, null, null, limit = 50, offset = 0)

        verify(repository).findDeadLetters(null, null, null, null, null, 50, 0)
    }

    @Test
    fun `list delegates all params to repository and maps results`() {
        val since = Instant.parse("2025-01-01T00:00:00Z")
        val until = Instant.parse("2025-12-31T23:59:59Z")
        val task = deadLetterTask(taskId = "t-1", handler = "email.send", queue = "default")

        `when`(repository.findDeadLetters("email.send", "g-1", since, until, "%timeout%", 25, 10))
            .thenReturn(listOf(task))

        val result = service.list("email.send", "g-1", since, until, "%timeout%", 25, 10)

        assertEquals(1, result.size)
        val item = result.first()
        assertEquals("t-1", item.taskId)
        assertEquals("email.send", item.handler)
        assertEquals("default", item.queue)
    }

    // ── getDetail ───────────────────────────────────────────────────

    @Test
    fun `getDetail returns null when repository returns null`() {
        `when`(repository.findDeadLetterById("missing")).thenReturn(null)

        assertNull(service.getDetail("missing"))
    }

    @Test
    fun `getDetail maps Task to DeadLetterDetail`() {
        val created = Instant.parse("2025-06-01T12:00:00Z")
        val claimed = Instant.parse("2025-06-01T12:01:00Z")
        val task = deadLetterTask(
            taskId = "t-2",
            handler = "process.order",
            queue = "high",
            payload = """{"orderId":42}""",
            groupId = "job-1",
            metadata = """{"source":"api"}""",
            retryCount = 3,
            maxRetries = 5,
            errorMessage = "Connection refused",
            createdAt = created,
            claimedBy = "worker-1",
            claimedAt = claimed,
        )
        `when`(repository.findDeadLetterById("t-2")).thenReturn(task)

        val detail = service.getDetail("t-2")!!

        assertEquals("t-2", detail.taskId)
        assertEquals("process.order", detail.handler)
        assertEquals("high", detail.queue)
        assertEquals("""{"orderId":42}""", detail.payload)
        assertEquals("job-1", detail.groupId)
        assertEquals("""{"source":"api"}""", detail.metadata)
        assertEquals(3, detail.retryCount)
        assertEquals(5, detail.maxRetries)
        assertEquals("Connection refused", detail.errorMessage)
        assertEquals(created, detail.createdAt)
        assertEquals("worker-1", detail.claimedBy)
        assertEquals(claimed, detail.claimedAt)
    }

    // ── summary ─────────────────────────────────────────────────────

    @Test
    fun `summary aggregates by handler and group`() {
        val since = Instant.parse("2025-01-01T00:00:00Z")
        val earliest = Instant.parse("2025-06-01T10:00:00Z")
        val latest = Instant.parse("2025-06-01T12:00:00Z")

        `when`(repository.summaryByHandler(since)).thenReturn(
            listOf(
                HandlerSummary("h1", 5, "err-a", earliest, latest),
                HandlerSummary("h2", 3, "err-b", earliest, latest),
            ),
        )
        `when`(repository.summaryByGroupId(since)).thenReturn(
            listOf(GroupSummary("g1", "h1", 2, "err-a", earliest, latest)),
        )

        val response = service.summary(since)

        assertEquals(8, response.totalCount)
        assertEquals(2, response.byHandler.size)
        assertEquals(1, response.byGroup.size)
        assertEquals("h1", response.byHandler[0].handler)
        assertEquals(5, response.byHandler[0].count)
        assertEquals("g1", response.byGroup[0].groupId)
    }

    // ── replaySingle ────────────────────────────────────────────────

    @Test
    fun `replaySingle returns 1 on success`() {
        `when`(repository.replaySingle("t-1", null, null)).thenReturn(true)

        assertEquals(1, service.replaySingle("t-1", null, null))
    }

    @Test
    fun `replaySingle returns null when repository returns false`() {
        `when`(repository.replaySingle("t-1", null, null)).thenReturn(false)

        assertNull(service.replaySingle("t-1", null, null))
    }

    // ── replayByFilter ──────────────────────────────────────────────

    @Test
    fun `replayByFilter delegates to repository`() {
        val filter = BulkReplayFilter(handler = "email.send", groupId = "g-1", since = null, errorPattern = null)
        val scheduledAt = Instant.parse("2025-07-01T00:00:00Z")
        `when`(repository.replayByFilter("email.send", "g-1", null, null, 5, scheduledAt))
            .thenReturn(12)

        val result = service.replayByFilter(filter, 5, scheduledAt)

        assertEquals(12, result)
        verify(repository).replayByFilter("email.send", "g-1", null, null, 5, scheduledAt)
    }

    // ── replayJob ───────────────────────────────────────────────────

    @Test
    fun `replayJob returns -1 for rejected COMPLETED job`() {
        `when`(repository.replayJob("job-1", false)).thenReturn(-1)

        assertEquals(-1, service.replayJob("job-1", force = false))
    }

    @Test
    fun `replayJob returns 0 when no tasks to replay`() {
        `when`(repository.replayJob("job-2", false)).thenReturn(0)

        assertEquals(0, service.replayJob("job-2"))
    }

    @Test
    fun `replayJob returns count of replayed tasks`() {
        `when`(repository.replayJob("job-3", false)).thenReturn(7)

        assertEquals(7, service.replayJob("job-3"))
    }

    // ── helpers ─────────────────────────────────────────────────────

    private fun deadLetterTask(
        taskId: String = "t-1",
        handler: String = "test-handler",
        queue: String = "default",
        payload: String = "{}",
        groupId: String? = null,
        metadata: String? = null,
        retryCount: Int = 3,
        maxRetries: Int = 3,
        errorMessage: String? = "some error",
        createdAt: Instant? = Instant.now(),
        claimedBy: String? = null,
        claimedAt: Instant? = null,
    ) = Task(
        taskId = taskId,
        handler = handler,
        queue = queue,
        payload = payload,
        status = TaskStatus.DEAD_LETTER,
        groupId = groupId,
        metadata = metadata,
        retryCount = retryCount,
        maxRetries = maxRetries,
        errorMessage = errorMessage,
        createdAt = createdAt,
        claimedBy = claimedBy,
        claimedAt = claimedAt,
    )
}
