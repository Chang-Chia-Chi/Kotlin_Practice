package infra.snapshotcache.api

import java.nio.file.Path
import java.time.Duration

/**
 * Every knob of spec 13, with its documented default. The two path settings have no
 * default because spec 13 marks them required.
 */
data class SnapshotCacheConfig(
    /** `storage.path` - directory holding the generation files. */
    val storagePath: Path,
    /** `duckdb.tempDirectory` - spill directory; without real space behind it, spilling becomes an OOM (spec 11.1). */
    val tempDirectory: Path,
    /** `refresh.interval` - gap measured from the end of the previous round, not a cron (spec 4.4). */
    val refreshInterval: Duration = Duration.ofMinutes(10),
    /** `refresh.allowOverlap` - two concurrent builds would double memory and disk (spec 4.4). */
    val allowOverlap: Boolean = false,
    /** `generation.maxLive` (K) - above this, refresh pauses and alerts rather than going stale silently (spec 6.1, D7). */
    val maxLiveGenerations: Int = 3,
    /** `acquire.defaultWaitBudget` - upper bound, not a sleep; overridable per call (spec 5.1, D21/D22). */
    val defaultWaitBudget: Duration = Duration.ofSeconds(30),
    /** `lease.deadline` - diagnostic threshold; nothing is force-reclaimed when it passes (spec 6.2, D8). */
    val leaseDeadline: Duration = Duration.ofMinutes(5),
    val verify: VerifyConfig = VerifyConfig(),
    /** `jdbc.fetchSize` - read by the caller's [GenerationSource]; the framework itself opens no source connection (spec 7.2). */
    val jdbcFetchSize: Int = 2000,
    /** `duckdb.serving.memoryLimit` - must stay well under the pod limit (spec 11.1). */
    val servingMemoryLimit: String = "3GB",
    /** `duckdb.consumer.memoryLimit` - the one shared consumer instance (spec 6.5, 11.1). */
    val consumerMemoryLimit: String = "1GB",
    /** `startup.clearStaleFiles` - leftover files are unowned because the pointer is not persisted (spec 10.1, D10). */
    val clearStaleFilesOnStartup: Boolean = true,
    /** `shutdown.leaseDrainTimeout` - keep `terminationGracePeriodSeconds` above this plus headroom (spec 10.2, 11.3). */
    val leaseDrainTimeout: Duration = Duration.ofSeconds(30),
) {
    init {
        require(maxLiveGenerations >= 1) { "maxLiveGenerations must be at least 1, was $maxLiveGenerations" }
        require(!defaultWaitBudget.isNegative) { "defaultWaitBudget must not be negative, was $defaultWaitBudget" }
        require(!refreshInterval.isNegative) { "refreshInterval must not be negative, was $refreshInterval" }
        require(jdbcFetchSize >= 1) { "jdbcFetchSize must be at least 1, was $jdbcFetchSize" }
    }
}

/**
 * The verify gate of spec 8.
 *
 * `nonEmpty` and `readable` are absent from the constructor on purpose: spec 8.1 marks
 * them non-disableable, so there is no setting to get wrong. They are exposed as constants
 * only so the gate can be read off the config.
 */
data class VerifyConfig(
    /** `verify.keyUnique` - id unique within its own table (spec 8.1). */
    val keyUnique: Boolean = true,
    /** `verify.requiredNonNull` - columns whose NULL means broken data; empty until spec 16.2 fills it in. */
    val requiredNonNull: List<String> = emptyList(),
    val rowCountDelta: RowCountDeltaConfig = RowCountDeltaConfig(),
    /** `verify.consecutiveFailureThreshold` - failures before escalating to critical (spec 8.5, D15). */
    val consecutiveFailureThreshold: Int = 3,
) {
    /** `verify.nonEmpty` - always on; publishing an empty dataset is the most expensive failure (spec 8.2, D13). */
    val nonEmpty: Boolean get() = true

    /** `verify.readable` - always on; the candidate is reopened and queried before publish (spec 4.2, 8.1). */
    val readable: Boolean get() = true

    init {
        require(consecutiveFailureThreshold >= 1) {
            "consecutiveFailureThreshold must be at least 1, was $consecutiveFailureThreshold"
        }
    }
}

/** Row-count movement gate. Off until enough history exists to pick thresholds (spec 8.3, D14). */
data class RowCountDeltaConfig(
    val enabled: Boolean = false,
    val maxDecreaseRatio: Double = 0.20,
    val maxIncreaseRatio: Double = 1.00,
)
