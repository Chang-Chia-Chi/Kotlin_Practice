package com.exporter.validation

/**
 * Thrown when configuration validation fails at startup.
 * Contains all collected errors for a single fail-fast report.
 */
class ConfigValidationException(
    val errors: List<String>,
) : RuntimeException(
    "Configuration validation failed with ${errors.size} error(s):\n" +
        errors.joinToString("\n") { "  - $it" }
)
