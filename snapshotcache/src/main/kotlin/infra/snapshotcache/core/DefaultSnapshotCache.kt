package infra.snapshotcache.core

import infra.snapshotcache.api.CacheAdmin
import infra.snapshotcache.api.CopyOutResult
import infra.snapshotcache.api.CopyOutSpec
import infra.snapshotcache.api.GcOutcome
import infra.snapshotcache.api.GenerationInfo
import infra.snapshotcache.api.GenerationState
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.RefreshOutcome
import infra.snapshotcache.api.Snapshot
import infra.snapshotcache.api.SnapshotCache
import infra.snapshotcache.api.SnapshotCacheConfig
import java.time.Duration

/**
 * Shell. P3 fills in the consumer surface; P4 wires the admin surface to [RefreshCycle].
 *
 * One class implements both interfaces on purpose (plan 2.3): callers that should not see
 * the admin surface are handed the [SnapshotCache] type, not a second object.
 */
internal class DefaultSnapshotCache(
    private val config: SnapshotCacheConfig,
) : SnapshotCache, CacheAdmin {

    override val defaultWaitBudget: Duration get() = config.defaultWaitBudget

    override fun <T> withSnapshot(group: GroupId, waitBudget: Duration, block: (Snapshot) -> T): T = TODO("P3")

    override fun copyOut(group: GroupId, spec: CopyOutSpec, waitBudget: Duration): CopyOutResult = TODO("P3")

    override fun acquire(group: GroupId, waitBudget: Duration): Snapshot = TODO("P3")

    override fun currentInfo(group: GroupId): GenerationInfo? = TODO("P3")

    override fun triggerRefresh(group: GroupId): RefreshOutcome = TODO("P4")

    override fun gc(group: GroupId): GcOutcome = TODO("P4")

    override fun liveGenerations(group: GroupId): List<GenerationState> = TODO("P4")
}
