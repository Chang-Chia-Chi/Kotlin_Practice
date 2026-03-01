package com.exporter.config

/**
 * Supported Prometheus metric types.
 */
enum class MetricType {
    GAUGE,
    COUNTER,
    HISTOGRAM,
    SUMMARY,
    ENUM
}
