package com.mapreduce.deadletter.alerting

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.TaskDeadLettered
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.ObservesAsync
import jakarta.enterprise.inject.Instance
import org.jboss.logging.Logger
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque

/**
 * Sliding window counter that evaluates dead-letter thresholds per handler.
 *
 * Subscribes to [TaskDeadLettered] via `@ObservesAsync` — failures here
 * never propagate to the task completion path.
 *
 * Each handler maintains an in-memory [ConcurrentLinkedDeque] of timestamps.
 * On each event:
 * 1. Evict entries outside the sliding window
 * 2. Add the new event
 * 3. If count exceeds threshold, fire an alert and reset the counter
 *
 * The counter resets on pod restart — acceptable for alerting (§5.3).
 */
@ApplicationScoped
class DeadLetterAlertEvaluator(
    private val config: FrameworkConfig,
    private val sinks: Instance<AlertSink>,
) {

    private val log = Logger.getLogger(DeadLetterAlertEvaluator::class.java)

    /** Per-handler sliding window: timestamps of recent dead-letter events. */
    private val windows = ConcurrentHashMap<String, ConcurrentLinkedDeque<Instant>>()

    /** Configured alert rules — loaded lazily from config on first event. */
    private val rules: List<AlertRule> by lazy { loadRules() }

    /** Resolved sinks iterable — supports both CDI Instance and test-injected lists. */
    private val resolvedSinks: Iterable<AlertSink> get() = testSinks ?: sinks

    @Volatile
    private var testSinks: List<AlertSink>? = null

    /** Test-only constructor. */
    internal constructor(
        config: FrameworkConfig,
        sinkList: List<AlertSink>,
    ) : this(config, CDI_PLACEHOLDER) {
        testSinks = sinkList
    }

    companion object {
        /** Placeholder — never used when testSinks is set. */
        @Suppress("UNCHECKED_CAST")
        private val CDI_PLACEHOLDER: Instance<AlertSink> =
            java.lang.reflect.Proxy.newProxyInstance(
                Instance::class.java.classLoader,
                arrayOf(Instance::class.java),
            ) { _, _, _ -> throw UnsupportedOperationException() } as Instance<AlertSink>
    }

    fun onDeadLetter(@ObservesAsync event: TaskDeadLettered) {
        try {
            evaluate(event.handler, event.deadLetteredAt)
        } catch (e: Exception) {
            log.warnf(e, "Error evaluating dead-letter alert for handler=%s", event.handler)
        }
    }

    internal fun evaluate(handler: String, timestamp: Instant) {
        val matchingRules = rules.filter { it.handler == handler || it.handler == "*" }
        if (matchingRules.isEmpty()) return

        for (rule in matchingRules) {
            val key = "${rule.handler}:${rule.threshold}:${rule.window.seconds}"
            val window = windows.computeIfAbsent(key) { ConcurrentLinkedDeque() }

            // Evict entries outside the sliding window
            val cutoff = timestamp.minus(rule.window)
            while (window.peekFirst()?.isBefore(cutoff) == true) {
                window.pollFirst()
            }

            window.addLast(timestamp)

            if (window.size >= rule.threshold) {
                fireAlert(handler, rule, window.size)
                window.clear() // Reset to prevent alert storms (§5.3)
            }
        }
    }

    private fun fireAlert(handler: String, rule: AlertRule, count: Int) {
        val alert = DeadLetterAlert(
            handler = handler,
            count = count,
            threshold = rule.threshold,
            windowSeconds = rule.window.seconds,
            severity = rule.severity,
        )

        log.warnf("Dead-letter alert: handler=%s count=%d threshold=%d window=%ds severity=%s",
            handler, count, rule.threshold, rule.window.seconds, rule.severity)

        for (sink in resolvedSinks) {
            try {
                sink.deliver(alert)
            } catch (e: Exception) {
                log.warnf(e, "Alert sink %s failed", sink.javaClass.simpleName)
            }
        }
    }

    private fun loadRules(): List<AlertRule> {
        val dlConfig = config.deadLetter()
        // Default catch-all rule from config
        return listOf(
            AlertRule(
                handler = "*",
                threshold = dlConfig.alertDefaultThreshold(),
                window = dlConfig.alertDefaultWindow(),
                severity = AlertSeverity.WARNING,
            ),
        )
    }
}

/** A single alert rule definition. */
data class AlertRule(
    val handler: String,
    val threshold: Int,
    val window: Duration,
    val severity: AlertSeverity,
)
