package com.workflow.worker.usecase.service

import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.usecase.port.inbound.orchestration.PhaseGate
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.stub
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class TaskSettlerTest {

    private lateinit var taskRepo: TaskRepository
    private lateinit var phaseGate: PhaseGate
    private lateinit var settler: TaskSettler

    @BeforeEach
    fun setUp() {
        taskRepo = mock()
        phaseGate = mock()
        settler = TaskSettler(taskRepo, phaseGate)
    }

    // ── Step 1: settle() delegation ─────────────────────────────────────

    @Test
    fun `settle delegates to phaseGate onTaskCompleted`() = runTest {
        val someInstant = Instant.parse("2026-01-01T00:00:00Z")

        settler.settle("t-1", "wf-1", 1, TaskStatus.COMPLETED, """{"ok":true}""", "worker-1", someInstant)

        verify(phaseGate).onTaskCompleted(
            eq("t-1"), eq("wf-1"), eq(1),
            eq(TaskStatus.COMPLETED), eq("""{"ok":true}"""),
            eq("worker-1"), eq(someInstant), eq(null),
        )
    }

    @Test
    fun `settle with null claimedBy and claimedAt defaults`() = runTest {
        settler.settle("t-1", "wf-1", 1, TaskStatus.TIMED_OUT, null)

        verify(phaseGate).onTaskCompleted(
            eq("t-1"), eq("wf-1"), eq(1),
            eq(TaskStatus.TIMED_OUT), eq(null),
            eq(null), eq(null), eq(null),
        )
    }

    @Test
    fun `settle propagates phaseGate exception`() = runTest {
        phaseGate.stub {
            onBlocking {
                onTaskCompleted(
                    eq("t-1"), eq("wf-1"), eq(1),
                    eq(TaskStatus.COMPLETED), eq(null), eq(null), eq(null), eq(null),
                )
            } doThrow RuntimeException("db error")
        }

        assertFailsWith<RuntimeException> {
            settler.settle("t-1", "wf-1", 1, TaskStatus.COMPLETED, null)
        }
    }

    @Test
    fun `settle passes non-null itemsJson to phaseGate`() = runTest {
        settler.settle("t-1", "wf-1", 1, TaskStatus.COMPLETED, """{"ok":true}""", itemsJson = """["item1"]""")

        verify(phaseGate).onTaskCompleted(
            eq("t-1"), eq("wf-1"), eq(1),
            eq(TaskStatus.COMPLETED), eq("""{"ok":true}"""),
            eq(null), eq(null), eq("""["item1"]"""),
        )
    }

    // ── Step 2: retryOrFail() — retry path ──────────────────────────────

    @Test
    fun `retryOrFail with retries remaining - resets and returns Retried`() = runTest {
        whenever(taskRepo.resetForRetry(eq("t-1"), eq(1), eq(null), eq(null))).thenReturn(true)

        val outcome = settler.retryOrFail("t-1", "wf-1", 1, retryCount = 0, maxRetries = 3)

        verify(taskRepo).resetForRetry(eq("t-1"), eq(1), eq(null), eq(null))
        verifyNoInteractions(phaseGate)
        assertEquals(RetryOutcome.Retried, outcome)
    }

    @Test
    fun `retryOrFail at boundary (retryCount = maxRetries - 1) - still retries`() = runTest {
        whenever(taskRepo.resetForRetry(eq("t-1"), eq(3), eq(null), eq(null))).thenReturn(true)

        val outcome = settler.retryOrFail("t-1", "wf-1", 1, retryCount = 2, maxRetries = 3)

        verify(taskRepo).resetForRetry(eq("t-1"), eq(3), eq(null), eq(null))
        assertEquals(RetryOutcome.Retried, outcome)
    }

    // ── Step 3: retryOrFail() — exhausted path ─────────────────────────

    @Test
    fun `retryOrFail with retries exhausted - settles FAILED and returns Failed`() = runTest {
        val outcome = settler.retryOrFail("t-1", "wf-1", 1, retryCount = 3, maxRetries = 3)

        verify(taskRepo, never()).resetForRetry(any(), any(), anyOrNull(), anyOrNull())
        verify(phaseGate).onTaskCompleted(
            eq("t-1"), eq("wf-1"), eq(1),
            eq(TaskStatus.FAILED), eq(null),
            eq(null), eq(null), eq(null),
        )
        assertEquals(RetryOutcome.Failed, outcome)
    }

    @Test
    fun `retryOrFail with zero maxRetries - settles FAILED immediately`() = runTest {
        val outcome = settler.retryOrFail("t-1", "wf-1", 1, retryCount = 0, maxRetries = 0)

        verify(taskRepo, never()).resetForRetry(any(), any(), anyOrNull(), anyOrNull())
        assertEquals(RetryOutcome.Failed, outcome)
    }

    // ── Step 4: retryOrFail() — resetForRetry false fallback (Bug 1 fix) ─

    @Test
    fun `retryOrFail when resetForRetry returns false (task already terminal) - settles FAILED`() = runTest {
        whenever(taskRepo.resetForRetry(eq("t-1"), eq(1), eq(null), eq(null))).thenReturn(false)

        val outcome = settler.retryOrFail("t-1", "wf-1", 1, retryCount = 0, maxRetries = 3)

        verify(taskRepo).resetForRetry(eq("t-1"), eq(1), eq(null), eq(null))
        verify(phaseGate).onTaskCompleted(
            eq("t-1"), eq("wf-1"), eq(1),
            eq(TaskStatus.FAILED), eq(null),
            eq(null), eq(null), eq(null),
        )
        assertEquals(RetryOutcome.Failed, outcome)
    }

    // ── Step 5: retryOrFail() — resetForRetry exception fallback ────────

    @Test
    fun `retryOrFail when resetForRetry throws - falls through to FAILED`() = runTest {
        whenever(taskRepo.resetForRetry(eq("t-1"), eq(1), eq(null), eq(null)))
            .thenThrow(RuntimeException("DB error"))

        val outcome = settler.retryOrFail("t-1", "wf-1", 1, retryCount = 0, maxRetries = 3)

        verify(taskRepo).resetForRetry(eq("t-1"), eq(1), eq(null), eq(null))
        verify(phaseGate).onTaskCompleted(
            eq("t-1"), eq("wf-1"), eq(1),
            eq(TaskStatus.FAILED), eq(null),
            eq(null), eq(null), eq(null),
        )
        assertEquals(RetryOutcome.Failed, outcome)
    }

    @Test
    fun `retryOrFail when resetForRetry throws AND phaseGate throws - propagates phaseGate exception`() = runTest {
        whenever(taskRepo.resetForRetry(eq("t-1"), eq(1), eq(null), eq(null)))
            .thenThrow(RuntimeException("DB error"))
        phaseGate.stub {
            onBlocking {
                onTaskCompleted(
                    eq("t-1"), eq("wf-1"), eq(1),
                    eq(TaskStatus.FAILED), eq(null), eq(null), eq(null), eq(null),
                )
            } doThrow RuntimeException("phaseGate error")
        }

        val ex = assertFailsWith<RuntimeException> {
            settler.retryOrFail("t-1", "wf-1", 1, retryCount = 0, maxRetries = 3)
        }
        assertEquals("phaseGate error", ex.message)
    }

    // ── Step 6: retryOrFail() — fencing token passthrough (Bug 1 fix) ───

    @Test
    fun `retryOrFail passes claimedBy and claimedAt to resetForRetry`() = runTest {
        val instant = Instant.parse("2026-03-15T12:00:00Z")
        whenever(taskRepo.resetForRetry(eq("t-1"), eq(1), eq("worker-1"), eq(instant))).thenReturn(true)

        val outcome = settler.retryOrFail(
            "t-1", "wf-1", 1,
            retryCount = 0, maxRetries = 3,
            claimedBy = "worker-1", claimedAt = instant,
        )

        verify(taskRepo).resetForRetry(eq("t-1"), eq(1), eq("worker-1"), eq(instant))
        verifyNoInteractions(phaseGate)
        assertEquals(RetryOutcome.Retried, outcome)
    }

    @Test
    fun `retryOrFail passes claimedBy and claimedAt to settle on failure`() = runTest {
        val instant = Instant.parse("2026-03-15T12:00:00Z")

        settler.retryOrFail("t-1", "wf-1", 1, 3, 3, "worker-1", instant)

        verify(phaseGate).onTaskCompleted(
            eq("t-1"), eq("wf-1"), eq(1),
            eq(TaskStatus.FAILED), eq(null),
            eq("worker-1"), eq(instant), eq(null),
        )
    }
}
