package com.mapreduce.deadletter

import com.mapreduce.deadletter.repository.DeadLetterRepository
import com.mapreduce.event.TaskDeadLettered
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import java.time.Instant

class DeadLetterMetricsTest {

    private lateinit var registry: SimpleMeterRegistry
    private lateinit var repository: DeadLetterRepository
    private lateinit var metrics: DeadLetterMetrics

    @BeforeEach
    fun setUp() {
        registry = SimpleMeterRegistry()
        repository = mock(DeadLetterRepository::class.java)
        metrics = DeadLetterMetrics(registry, repository)
    }

    private fun event(handler: String = "email.send") = TaskDeadLettered(
        taskId = "t-1",
        handler = handler,
        queue = "default",
        groupId = null,
        retryCount = 3,
        lastError = "timeout",
        createdAt = Instant.now(),
    )

    // ── onDeadLetter gauge ──────────────────────────────────────────

    @Test
    fun `onDeadLetter increments gauge for handler`() {
        metrics.onDeadLetter(event("email.send"))

        val gauge = registry.find("taskqueue.deadletter.total")
            .tag("handler", "email.send")
            .gauge()

        assertEquals(1.0, gauge?.value())
    }

    @Test
    fun `multiple events for same handler accumulate`() {
        repeat(5) { metrics.onDeadLetter(event("email.send")) }

        val gauge = registry.find("taskqueue.deadletter.total")
            .tag("handler", "email.send")
            .gauge()

        assertEquals(5.0, gauge?.value())
    }

    @Test
    fun `different handlers get separate gauges`() {
        metrics.onDeadLetter(event("email.send"))
        metrics.onDeadLetter(event("email.send"))
        metrics.onDeadLetter(event("order.process"))

        val emailGauge = registry.find("taskqueue.deadletter.total")
            .tag("handler", "email.send")
            .gauge()
        val orderGauge = registry.find("taskqueue.deadletter.total")
            .tag("handler", "order.process")
            .gauge()

        assertEquals(2.0, emailGauge?.value())
        assertEquals(1.0, orderGauge?.value())
    }

    // ── recordReplayed ──────────────────────────────────────────────

    @Test
    fun `recordReplayed increments counter and decrements gauge`() {
        repeat(5) { metrics.onDeadLetter(event("email.send")) }

        metrics.recordReplayed("email.send", 3)

        val counter = registry.find("taskqueue.deadletter.replayed")
            .tag("handler", "email.send")
            .counter()
        val gauge = registry.find("taskqueue.deadletter.total")
            .tag("handler", "email.send")
            .gauge()

        assertEquals(3.0, counter?.count())
        assertEquals(2.0, gauge?.value())
    }

    // ── recordCleaned ───────────────────────────────────────────────

    @Test
    fun `recordCleaned increments counter`() {
        metrics.recordCleaned(42)

        val counter = registry.find("taskqueue.deadletter.cleaned").counter()

        assertEquals(42.0, counter?.count())
    }
}
