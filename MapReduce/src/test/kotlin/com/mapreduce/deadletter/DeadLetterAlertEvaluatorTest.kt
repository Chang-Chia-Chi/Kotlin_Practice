package com.mapreduce.deadletter

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.deadletter.alerting.AlertSeverity
import com.mapreduce.deadletter.alerting.AlertSink
import com.mapreduce.deadletter.alerting.DeadLetterAlert
import com.mapreduce.deadletter.alerting.DeadLetterAlertEvaluator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

class DeadLetterAlertEvaluatorTest {

    private lateinit var evaluator: DeadLetterAlertEvaluator
    private val firedAlerts = CopyOnWriteArrayList<DeadLetterAlert>()

    @BeforeEach
    fun setup() {
        firedAlerts.clear()

        val sink = object : AlertSink {
            override fun deliver(alert: DeadLetterAlert) {
                firedAlerts.add(alert)
            }
        }

        val config = object : FrameworkConfig {
            override fun worker() = throw UnsupportedOperationException()
            override fun leader() = throw UnsupportedOperationException()
            override fun leaderElection() = throw UnsupportedOperationException()
            override fun shutdown() = throw UnsupportedOperationException()
            override fun metrics() = throw UnsupportedOperationException()
            override fun health() = throw UnsupportedOperationException()
            override fun pipeline() = throw UnsupportedOperationException()
            override fun heartbeat() = throw UnsupportedOperationException()
            override fun reaper() = throw UnsupportedOperationException()
            override fun schedule() = throw UnsupportedOperationException()
            override fun deadLetter() = object : FrameworkConfig.DeadLetterConfig {
                override fun retentionDays() = 30
                override fun cleanupScheduleHours() = 24
                override fun archiveBeforeDelete() = false
                override fun alertDefaultThreshold() = 5
                override fun alertDefaultWindow() = Duration.ofMinutes(5)
                override fun slackWebhookUrl() = ""
            }
        }

        evaluator = DeadLetterAlertEvaluator(config, listOf(sink))
    }

    @Test
    fun `fires alert when threshold reached within window`() {
        val now = Instant.now()
        repeat(5) { i ->
            evaluator.evaluate("test.handler", now.plusMillis(i * 100L))
        }
        assertEquals(1, firedAlerts.size)
        assertEquals("test.handler", firedAlerts[0].handler)
        assertEquals(5, firedAlerts[0].count)
        assertEquals(AlertSeverity.WARNING, firedAlerts[0].severity)
    }

    @Test
    fun `does not fire alert below threshold`() {
        val now = Instant.now()
        repeat(4) { i ->
            evaluator.evaluate("test.handler", now.plusMillis(i * 100L))
        }
        assertEquals(0, firedAlerts.size)
    }

    @Test
    fun `counter resets after alert fires`() {
        val now = Instant.now()
        // Fire first alert
        repeat(5) { i ->
            evaluator.evaluate("test.handler", now.plusMillis(i * 100L))
        }
        assertEquals(1, firedAlerts.size)

        // Need 5 more to fire again (counter was reset)
        repeat(4) { i ->
            evaluator.evaluate("test.handler", now.plusMillis((i + 5) * 100L))
        }
        assertEquals(1, firedAlerts.size) // still 1

        evaluator.evaluate("test.handler", now.plusMillis(900L))
        assertEquals(2, firedAlerts.size) // now 2
    }

    @Test
    fun `events outside window are evicted`() {
        val now = Instant.now()
        // Add 4 events far in the past (outside 5-minute window)
        val past = now.minus(Duration.ofMinutes(10))
        repeat(4) { i ->
            evaluator.evaluate("test.handler", past.plusMillis(i * 100L))
        }

        // 1 more event in the present — should not trigger (old ones evicted)
        evaluator.evaluate("test.handler", now)
        assertEquals(0, firedAlerts.size)
    }

    @Test
    fun `separate handlers have independent counters`() {
        val now = Instant.now()
        repeat(3) { evaluator.evaluate("handler.a", now.plusMillis(it * 10L)) }
        repeat(5) { evaluator.evaluate("handler.b", now.plusMillis(it * 10L)) }

        // Only handler.b should have triggered (threshold=5)
        assertEquals(1, firedAlerts.size)
        assertEquals("handler.b", firedAlerts[0].handler)
    }
}
