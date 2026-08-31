package infra.snapshotcache.api

import java.nio.file.Path
import java.time.Duration

/**
 * Every configuration knob, with its documented default. The two path settings have no
 * default because they are required.
 */
data class SnapshotCacheConfig(
    /** `storage.path` - directory holding the generation files. */
    val storagePath: Path,
    /** `duckdb.tempDirectory` - spill directory; without real space behind it, spilling becomes an OOM. */
    val tempDirectory: Path,
    /** `refresh.interval` - gap measured from the end of the previous round, not a cron. */
    val refreshInterval: Duration = Duration.ofMinutes(10),
    /** `refresh.allowOverlap` - two concurrent builds would double memory and disk. */
    val allowOverlap: Boolean = false,
    /** `generation.maxLive` (K) - above this, refresh pauses and alerts rather than going stale silently. */
    val maxLiveGenerations: Int = 3,
    /** `acquire.defaultWaitBudget` - upper bound, not a sleep; overridable per call. */
    val defaultWaitBudget: Duration = Duration.ofSeconds(30),
    /** `lease.deadline` - diagnostic threshold; nothing is force-reclaimed when it passes. */
    val leaseDeadline: Duration = Duration.ofMinutes(5),
    val verify: VerifyConfig = VerifyConfig(),
    /** `jdbc.fetchSize` - read by the caller's [GenerationSource]; the framework itself opens no source connection. */
    val jdbcFetchSize: Int = 2000,
    /** `duckdb.serving.memoryLimit` - must stay well under the pod limit. */
    val servingMemoryLimit: String = "3GB",
    /**
     * `duckdb.consumer.memoryLimit` - the host's one shared consumer instance.
     *
     * **Inert when the consumer is SimpleEtl** (as amended 2026-08-30). A `cacheCopy` step
     * passes its own per-run scratch instance's write connection as `CopyOutSpec.targetConnection`,
     * so the copy is bounded by `EtlWiring.scratchMemoryLimitMb` and never by this. Kept rather
     * than removed: a host that really does own one shared consumer instance reads it, and the
     * pod budget for a consumer that cannot share one is
     * `N_concurrent x <per-instance limit> + servingMemoryLimit`.
     */
    val consumerMemoryLimit: String = "1GB",
    /**
     * `duckdb.serving.threads` - caps the serving instance's DuckDB thread pool; null =
     * engine default. Matters on CPU-limited pods where the default equals hardware
     * concurrency.
     */
    val servingThreads: Int? = null,
    /** `startup.clearStaleFiles` - leftover files are unowned because the pointer is not persisted. */
    val clearStaleFilesOnStartup: Boolean = true,
    /** `shutdown.leaseDrainTimeout` - keep `terminationGracePeriodSeconds` above this plus headroom. */
    val leaseDrainTimeout: Duration = Duration.ofSeconds(30),
) {
    init {
        require(maxLiveGenerations >= 1) { "maxLiveGenerations must be at least 1, was $maxLiveGenerations" }
        require(!defaultWaitBudget.isNegative) { "defaultWaitBudget must not be negative, was $defaultWaitBudget" }
        require(!refreshInterval.isNegative) { "refreshInterval must not be negative, was $refreshInterval" }
        require(jdbcFetchSize >= 1) { "jdbcFetchSize must be at least 1, was $jdbcFetchSize" }
        require(servingThreads == null || servingThreads >= 1) {
            "servingThreads must be at least 1 when set, was $servingThreads"
        }
    }
}

/**
 * Settings for the verify gate.
 *
 * `nonEmpty` and `readable` are absent from the constructor on purpose: they are
 * non-disableable, so there is no setting to get wrong. They are exposed as constants
 * only so the gate can be read off the config.
 */
data class VerifyConfig(
    /** `verify.keyUnique` - id unique within its own table. */
    val keyUnique: Boolean = true,
    /** `verify.requiredNonNull` - columns whose NULL means broken data; empty until a deployment names them. */
    val requiredNonNull: List<String> = emptyList(),
    val rowCountDelta: RowCountDeltaConfig = RowCountDeltaConfig(),
    /** `verify.consecutiveFailureThreshold` - failures before escalating to critical. */
    val consecutiveFailureThreshold: Int = 3,
) {
    /** `verify.nonEmpty` - always on; publishing an empty dataset is the most expensive failure. */
    val nonEmpty: Boolean get() = true

    /** `verify.readable` - always on; the candidate is reopened and queried before publish. */
    val readable: Boolean get() = true

    init {
        require(consecutiveFailureThreshold >= 1) {
            "consecutiveFailureThreshold must be at least 1, was $consecutiveFailureThreshold"
        }
    }
}

/** Row-count movement gate. Off until enough history exists to pick thresholds. */
data class RowCountDeltaConfig(
    val enabled: Boolean = false,
    val maxDecreaseRatio: Double = 0.20,
    val maxIncreaseRatio: Double = 1.00,
)
