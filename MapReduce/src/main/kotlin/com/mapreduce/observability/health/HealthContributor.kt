package com.mapreduce.observability.health

/**
 * Health status for a single dimension (liveness or readiness).
 *
 * - **UP:** fully operational.
 * - **DEGRADED:** functioning with reduced capacity (treated as UP for aggregation).
 * - **DOWN:** non-functional — triggers pod restart (liveness) or endpoint removal (readiness).
 */
enum class HealthStatus { UP, DEGRADED, DOWN }

/**
 * Result of a single health probe dimension.
 *
 * @property status the health status.
 * @property details human-readable context for dashboards / runbooks.
 */
data class ProbeResult(
    val status: HealthStatus,
    val details: Map<String, Any> = emptyMap(),
)

/**
 * Unified health contributor interface.
 *
 * Every subsystem implements this contract and is discovered via CDI.
 * The aggregator iterates all beans, calls [liveness]/[readiness],
 * and combines results per the aggregation rules (§4 of the design doc).
 *
 * Returning `null` means "I have no opinion on this dimension."
 * The aggregator skips null contributors.
 */
interface HealthContributor {

    /** Unique identifier for this contributor (e.g., `"worker-loop"`, `"oracle"`). */
    val name: String

    /** Liveness probe result, or `null` if this contributor has no liveness opinion. */
    fun liveness(): ProbeResult?

    /** Readiness probe result, or `null` if this contributor has no readiness opinion. */
    fun readiness(): ProbeResult?
}
