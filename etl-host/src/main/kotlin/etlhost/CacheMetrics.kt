package etlhost

import infra.snapshotcache.api.AcquireUnavailableReason
import infra.snapshotcache.api.CacheAdmin
import infra.snapshotcache.api.CacheEvents
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.LeaseInfo
import infra.snapshotcache.api.RefreshPhase
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.api.SnapshotCache
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.time.Clock
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Supplier
import java.util.concurrent.atomic.AtomicLong

/**
 * Snapshot-cache spec 12, bound to Micrometer. Names and label sets verbatim - they are a FIXED
 * contract of plan P9 and an alert rule is written against the string, not against this class.
 *
 * Two halves, because spec 12 has two kinds of metric and the framework only ships one of them:
 *
 * - **Occurrences** arrive through [CacheEvents], which the framework calls. That is this class's
 *   interface half.
 * - **Gauges are not events** and the framework says so: they are polled from
 *   [SnapshotCache.currentInfo] and [CacheAdmin.liveGenerations]. [bind] is where the host takes
 *   that job on, and it is called once the cache exists - which is why it is a method rather than
 *   constructor injection. A `CacheEvents` the cache producer needs cannot itself need the cache.
 *
 * **No `generation` label anywhere** (spec 12.5): it increases monotonically and would grow the
 * series count without bound. Generation-level detail is the admin endpoint's and the log's.
 *
 * Every gauge is registered through Micrometer's `Supplier<Number>` builder, which holds the
 * supplier **strongly**. The sibling `MicrometerTaskMetrics` records the measurement behind that:
 * the `(obj, valueFunction)` form holds its referent weakly and reads `NaN` after a collection,
 * and re-registering the same id is ignored with a warning rather than replacing it.
 */
@Singleton
class CacheMetrics @Inject constructor(
    private val registry: MeterRegistry,
    private val clock: Clock,
) : CacheEvents {

    private val lastSuccess = ConcurrentHashMap<String, AtomicLong>()
    private val rowGauges = ConcurrentHashMap<String, Gauge>()

    @Volatile
    private var bound: Bound? = null

    private class Bound(val cache: SnapshotCache, val admin: CacheAdmin)

    /**
     * Registers spec 12's polled gauges for [groups]. Called by the host's startup sequence once
     * the cache is open; idempotent per group in the sense Micrometer is - re-registering an id is
     * ignored.
     *
     * `snapshot_rows` is deliberately absent here: its `table` label set is not known until a
     * generation has published, so those gauges appear on the first successful refresh instead.
     */
    fun bind(groups: Collection<GroupId>, cache: SnapshotCache, admin: CacheAdmin) {
        bound = Bound(cache, admin)
        groups.forEach { group ->
            val tag = group.value
            gauge("snapshot_current_generation", tag) {
                cache.currentInfo(group)?.generation?.toDouble() ?: Double.NaN
            }
            gauge("snapshot_data_as_of_seconds", tag) {
                // Absolute Unix seconds, never an age. Spec 12.1: alert rules compute
                // `time() - snapshot_data_as_of_seconds > X`, so the threshold is editable
                // without a deploy. An age here would move that arithmetic into this file.
                cache.currentInfo(group)?.dataAsOf?.epochSecond?.toDouble() ?: Double.NaN
            }
            gauge("snapshot_published_at_seconds", tag) {
                cache.currentInfo(group)?.publishedAt?.epochSecond?.toDouble() ?: Double.NaN
            }
            gauge("snapshot_last_success_seconds", tag) { successHolder(tag).get().toDouble() }
            gauge("snapshot_live_generations", tag) { admin.liveGenerations(group).size.toDouble() }
            gauge("snapshot_active_leases", tag) {
                admin.liveGenerations(group).sumOf { it.leases.size }.toDouble()
            }
            gauge("snapshot_db_file_bytes", tag) {
                admin.liveGenerations(group).sumOf { it.fileBytes }.toDouble()
            }
        }
    }

    // ---- occurrences (spec 12.2, 12.3, 12.4) ----

    override fun refreshFinished(group: GroupId, result: RefreshResult, generation: Long?) {
        registry.counter("snapshot_refresh_total", "group", group.value, "result", result.label()).increment()
        if (result != RefreshResult.SUCCESS) return
        successHolder(group.value).set(clock.instant().epochSecond)
        bindRowGauges(group)
    }

    override fun refreshPhase(group: GroupId, phase: RefreshPhase, elapsed: Duration) {
        registry.timer("snapshot_refresh_duration_seconds", "group", group.value, "phase", phase.label())
            .record(elapsed)
    }

    override fun verifyFailed(group: GroupId, rule: String, detail: String) {
        registry.counter("snapshot_verify_failed_total", "group", group.value, "rule", rule).increment()
    }

    override fun leaseReleased(group: GroupId, lease: LeaseInfo, heldFor: Duration) {
        registry.timer("snapshot_lease_duration_seconds", "group", group.value).record(heldFor)
    }

    override fun leaseExpired(group: GroupId, lease: LeaseInfo, heldFor: Duration) {
        registry.counter("snapshot_lease_expired_total", "group", group.value).increment()
    }

    /** Any non-zero value is a bug (spec 12.3), which is why it is counted rather than logged. */
    override fun leaseOrphaned(group: GroupId, lease: LeaseInfo) {
        registry.counter("snapshot_lease_orphaned_total", "group", group.value).increment()
    }

    override fun acquireWaited(group: GroupId, waited: Duration) {
        registry.timer("snapshot_acquire_waited_seconds", "group", group.value).record(waited)
    }

    override fun acquireUnavailable(group: GroupId, reason: AcquireUnavailableReason) {
        registry.counter(
            "snapshot_acquire_unavailable_total", "group", group.value, "reason", reason.label(),
        ).increment()
    }

    override fun generationReclaimed(group: GroupId, generation: Long) {
        registry.counter("snapshot_gc_deleted_total", "group", group.value).increment()
    }

    // ---- helpers ----

    /**
     * `snapshot_rows{group,table}`, registered the first time a generation publishes because that
     * is the first moment the table names exist. Each gauge then re-reads `currentInfo` on every
     * scrape, so it follows the current generation rather than freezing on the one that created it.
     */
    private fun bindRowGauges(group: GroupId) {
        val cache = bound?.cache ?: return
        cache.currentInfo(group)?.rowCounts?.keys?.forEach { table ->
            rowGauges.computeIfAbsent(group.value + "/" + table) {
                Gauge.builder(
                    "snapshot_rows",
                    Supplier<Number> { cache.currentInfo(group)?.rowCounts?.get(table) ?: 0L },
                ).tags("group", group.value, "table", table).strongReference(true).register(registry)
            }
        }
    }

    private fun successHolder(group: String) = lastSuccess.computeIfAbsent(group) { AtomicLong() }

    private fun gauge(name: String, group: String, value: () -> Double) {
        Gauge.builder(name, Supplier<Number> { value() })
            .tags("group", group)
            .strongReference(true)
            .register(registry)
    }
}

/**
 * `name.lowercase()` is the label verbatim, which each of the three enums states in its own KDoc.
 * Written once here so a rename in the framework is a compile error in one place rather than four
 * silently diverging label sets.
 */
private fun Enum<*>.label(): String = name.lowercase()
