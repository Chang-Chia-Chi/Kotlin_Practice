package com.mapreduce.deadletter.alerting

import com.mapreduce.config.FrameworkConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import java.time.Duration
import java.time.Instant

class DeadLetterAlertEvaluatorTest {

    private lateinit var config: FrameworkConfig
    private lateinit var dlConfig: FrameworkConfig.DeadLetterConfig
    private lateinit var delivered: MutableList<DeadLetterAlert>
    private lateinit var evaluator: DeadLetterAlertEvaluator

    private val threshold = 3
    private val window = Duration.ofSeconds(60)

    @BeforeEach
    fun setUp() {
        config = mock(FrameworkConfig::class.java)
        dlConfig = mock(FrameworkConfig.DeadLetterConfig::class.java)
        `when`(config.deadLetter()).thenReturn(dlConfig)
        `when`(dlConfig.alertDefaultThreshold()).thenReturn(threshold)
        `when`(dlConfig.alertDefaultWindow()).thenReturn(window)

        delivered = mutableListOf()
        val sink = CapturingSink(delivered)
        evaluator = DeadLetterAlertEvaluator(config, listOf(sink))
    }

    // ── threshold behavior ──────────────────────────────────────────

    @Test
    fun `events below threshold do not fire alert`() {
        val now = Instant.now()
        repeat(threshold - 1) { i ->
            evaluator.evaluate("email.send", now.plusMillis(i.toLong()))
        }

        assertEquals(0, delivered.size)
    }

    @Test
    fun `reaching threshold fires alert`() {
        val now = Instant.now()
        repeat(threshold) { i ->
            evaluator.evaluate("email.send", now.plusMillis(i.toLong()))
        }

        assertEquals(1, delivered.size)
        val alert = delivered.first()
        assertEquals("email.send", alert.handler)
        assertEquals(threshold, alert.count)
        assertEquals(threshold, alert.threshold)
        assertEquals(window.seconds, alert.windowSeconds)
        assertEquals(AlertSeverity.WARNING, alert.severity)
    }

    @Test
    fun `multiple events in quick succession reach threshold`() {
        val now = Instant.now()
        repeat(threshold) { evaluator.evaluate("fast.handler", now) }

        assertEquals(1, delivered.size)
        assertEquals("fast.handler", delivered.first().handler)
    }

    // ── window reset ────────────────────────────────────────────────

    @Test
    fun `window resets after alert fires`() {
        val now = Instant.now()
        // Fire first alert
        repeat(threshold) { i ->
            evaluator.evaluate("email.send", now.plusMillis(i.toLong()))
        }
        assertEquals(1, delivered.size)

        // Need another full threshold to fire again
        repeat(threshold - 1) { i ->
            evaluator.evaluate("email.send", now.plusMillis((100 + i).toLong()))
        }
        assertEquals(1, delivered.size) // still only the first alert

        evaluator.evaluate("email.send", now.plusMillis(200))
        assertEquals(2, delivered.size)
    }

    // ── eviction ────────────────────────────────────────────────────

    @Test
    fun `events outside window are evicted`() {
        val now = Instant.now()
        // Add events that will be outside the window when the final event arrives
        repeat(threshold - 1) { i ->
            evaluator.evaluate("email.send", now.plusMillis(i.toLong()))
        }

        // Jump far past the window so previous events are evicted
        val future = now.plus(window).plusSeconds(10)
        evaluator.evaluate("email.send", future)

        // Only 1 event remains in window (the future one), not enough for threshold
        assertEquals(0, delivered.size)
    }

    // ── wildcard rule ───────────────────────────────────────────────

    @Test
    fun `wildcard rule matches all handlers`() {
        val now = Instant.now()
        repeat(threshold) { i ->
            evaluator.evaluate("any.handler", now.plusMillis(i.toLong()))
        }

        assertEquals(1, delivered.size)
        assertEquals("any.handler", delivered.first().handler)
    }

    // ── sink failure isolation ───────────────────────────────────────

    @Test
    fun `sink failure does not prevent other sinks from receiving alert`() {
        val secondDelivered = mutableListOf<DeadLetterAlert>()
        val failingSink = object : AlertSink {
            override fun deliver(alert: DeadLetterAlert) {
                throw RuntimeException("sink exploded")
            }
        }
        val capturingSink = CapturingSink(secondDelivered)

        val evaluatorWithFailingSink = DeadLetterAlertEvaluator(config, listOf(failingSink, capturingSink))

        val now = Instant.now()
        repeat(threshold) { i ->
            evaluatorWithFailingSink.evaluate("email.send", now.plusMillis(i.toLong()))
        }

        assertEquals(1, secondDelivered.size)
    }

    // ── all sinks receive alert ─────────────────────────────────────

    @Test
    fun `reaching threshold delivers alert to all sinks`() {
        val sink1Alerts = mutableListOf<DeadLetterAlert>()
        val sink2Alerts = mutableListOf<DeadLetterAlert>()
        val multiSinkEvaluator = DeadLetterAlertEvaluator(
            config,
            listOf(CapturingSink(sink1Alerts), CapturingSink(sink2Alerts)),
        )

        val now = Instant.now()
        repeat(threshold) { i ->
            multiSinkEvaluator.evaluate("email.send", now.plusMillis(i.toLong()))
        }

        assertEquals(1, sink1Alerts.size)
        assertEquals(1, sink2Alerts.size)
    }

    // ── helpers ─────────────────────────────────────────────────────

    private class CapturingSink(private val store: MutableList<DeadLetterAlert>) : AlertSink {
        override fun deliver(alert: DeadLetterAlert) {
            store.add(alert)
        }
    }
}
