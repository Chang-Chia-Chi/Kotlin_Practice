package com.mapreduce.deadletter.alerting

import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger
import java.time.Instant

/** Severity levels for dead-letter alerts. */
enum class AlertSeverity { INFO, WARNING, CRITICAL }

/** Immutable alert payload delivered to all sinks. */
data class DeadLetterAlert(
    val handler: String,
    val count: Int,
    val threshold: Int,
    val windowSeconds: Long,
    val severity: AlertSeverity,
    val firedAt: Instant = Instant.now(),
)

/** Abstraction for alert delivery — multiple implementations run simultaneously. */
interface AlertSink {
    fun deliver(alert: DeadLetterAlert)
}

/**
 * Prometheus sink — increments a labeled counter.
 * Alertmanager rules evaluate downstream.
 */
@ApplicationScoped
class PrometheusAlertSink(private val meterRegistry: MeterRegistry) : AlertSink {

    override fun deliver(alert: DeadLetterAlert) {
        meterRegistry.counter(
            "taskqueue.deadletter.alerts_fired",
            "handler", alert.handler,
            "severity", alert.severity.name,
        ).increment()
    }
}

/**
 * Structured log sink — logs at WARN/ERROR with structured fields.
 * Relies on log aggregation (ELK, Loki) for visibility.
 */
@ApplicationScoped
class LogAlertSink : AlertSink {

    private val log = Logger.getLogger("deadletter.alerts")

    override fun deliver(alert: DeadLetterAlert) {
        val msg = """{"event":"DEAD_LETTER_ALERT","handler":"%s","count":%d,"threshold":%d,"window_seconds":%d,"severity":"%s","fired_at":"%s"}"""
        when (alert.severity) {
            AlertSeverity.CRITICAL -> log.errorf(msg,
                alert.handler, alert.count, alert.threshold, alert.windowSeconds, alert.severity, alert.firedAt)
            AlertSeverity.WARNING -> log.warnf(msg,
                alert.handler, alert.count, alert.threshold, alert.windowSeconds, alert.severity, alert.firedAt)
            AlertSeverity.INFO -> log.infof(msg,
                alert.handler, alert.count, alert.threshold, alert.windowSeconds, alert.severity, alert.firedAt)
        }
    }
}
