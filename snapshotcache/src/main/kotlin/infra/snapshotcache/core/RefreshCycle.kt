package infra.snapshotcache.core

import infra.snapshotcache.api.BuildContext
import infra.snapshotcache.api.CacheEvents
import infra.snapshotcache.api.GcOutcome
import infra.snapshotcache.api.GenerationCheck
import infra.snapshotcache.api.GenerationInfo
import infra.snapshotcache.api.GenerationSource
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.Hook
import infra.snapshotcache.api.HookRunner
import infra.snapshotcache.api.LeaseInfo
import infra.snapshotcache.api.NoOpCacheEvents
import infra.snapshotcache.api.NoOpHooks
import infra.snapshotcache.api.RefreshOutcome
import infra.snapshotcache.api.RefreshPhase
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.api.SnapshotCacheConfig
import infra.snapshotcache.spi.Candidate
import infra.snapshotcache.spi.GateOutcome
import infra.snapshotcache.spi.GenerationStore
import infra.snapshotcache.spi.OpenGeneration
import infra.snapshotcache.spi.VerifyGate
import infra.snapshotcache.spi.describe
import org.jboss.logging.Logger
import java.time.Clock
import java.time.Duration

private val log = Logger.getLogger(RefreshCycle::class.java)

/**
 * One refresh round, end to end: the refresh state machine driven over the seams. The
 * registry decides - every mutable-state question is answered under its single lock - and
 * this class sequences the I/O outside it. Failure at any stage leaves the current pointer
 * untouched, deletes the candidate, and puts the registry record in GONE, so the cache
 * returns to a usable state and the next round starts from a clean slate.
 *
 * Failure classification:
 * - [GenerationStore] operations throwing  -> [RefreshResult.DISK_ERROR], plus an emergency GC
 * - [GenerationSource.refresh] throwing    -> [RefreshResult.SOURCE_ERROR]
 * - [InterruptedException] anywhere, or the registry's shutting-down flag observed at a
 *   stage boundary                          -> [RefreshResult.SHUTDOWN_ABORTED]
 * - a failing verify rule                   -> [RefreshResult.VERIFY_FAILED]
 *
 * Shutdown abort is cooperative: the source may throw [InterruptedException], or the flag
 * is observed between stages. Delivering an actual thread interrupt is P9 wiring.
 *
 * Verify runs after promote+open, on a connection from the attached generation: the frozen
 * [GenerationStore] has no reopen-candidate-read-only seam, and the `readable` verify rule
 * ("reopened and queried") is exactly what [GenerationStore.open] does. I1 is unaffected -
 * the pointer swap still happens only after the gate passes - and a crash in the window
 * leaves an unattached, unverified file that the startup wipe removes.
 */
internal class RefreshCycle(
    private val group: GroupId,
    private val registry: GenerationRegistry,
    private val store: GenerationStore,
    private val source: GenerationSource,
    private val config: SnapshotCacheConfig,
    private val events: CacheEvents = NoOpCacheEvents,
    private val checks: List<GenerationCheck> = emptyList(),
    private val clock: Clock = Clock.systemUTC(),
    private val hooks: HookRunner = NoOpHooks,
) {
    private val gate = VerifyGate(config.verify, checks)

    /** Classified mid-round failure; [round] raises it, [abort] turns it into cleanup + outcome. */
    private class RoundAbort(val result: RefreshResult, val reason: String) : RuntimeException(reason)

    /**
     * One full round including the overlap guard, the K guard, and GC after publish.
     * Overlapping rounds are forbidden: if one is already running, this trigger is skipped
     * and counted, nothing else.
     */
    fun runOnce(): RefreshOutcome {
        if (!registry.tryBeginRound()) {
            return finish(RefreshResult.SKIPPED_OVERLAP, detail = "a refresh round is already running")
        }
        try {
            return round()
        } finally {
            registry.endRound()
        }
    }

    private fun round(): RefreshOutcome {
        if (registry.blockedByK() != null) {
            // Generations already eligible for reclaim (non-current, refcount 0) must not
            // keep refresh paused: reclaim them first, then re-evaluate the guard, so a
            // released lease auto-resumes the next round.
            reclaimPass()
            registry.blockedByK()?.let { holders -> return blockedByK(holders) }
        }
        if (registry.isShuttingDown()) {
            return finish(RefreshResult.SHUTDOWN_ABORTED, detail = "shutdown observed before the round started")
        }

        val dataAsOf = clock.instant()
        val gen = registry.beginBuild()
        var candidate: Candidate? = null
        var opened: OpenGeneration? = null
        try {
            // ACQUIRING + BUILDING: the source streams into the candidate file.
            candidate = storeOp { store.createCandidate(gen) }
            // The candidate is a store product: its write connection failing is a disk/DB
            // problem (ruling 5), not a source problem, so it classifies outside the try.
            val target = storeOp { candidate.connection() }
            try {
                source.refresh(BuildContext(group, gen, target, dataAsOf))
            } catch (interrupted: InterruptedException) {
                throw interrupted
            } catch (failure: Exception) {
                // An interrupted JDBC call surfaces as a driver exception (SQLException),
                // not as InterruptedException, so shutdown has to be re-checked here or a
                // correctly ordered shutdown - flag set, then interrupt - still counts
                // source_error and the two stop being distinguishable.
                if (registry.isShuttingDown()) {
                    throw RoundAbort(
                        RefreshResult.SHUTDOWN_ABORTED,
                        "shutdown observed while the source was running: ${failure.describe()}",
                    )
                }
                throw RoundAbort(RefreshResult.SOURCE_ERROR, failure.describe())
            }

            // CHECKPOINT: close() folds the WAL into the file. The adapter never throws
            // here (P0 note) but the contract belongs to the store, so a third-party one
            // that does throw classifies as a disk error rather than escaping the round.
            val checkpointStart = clock.instant()
            storeOp { candidate.close() }
            emit(group) { events.refreshPhase(group, RefreshPhase.CHECKPOINT, Duration.between(checkpointStart, clock.instant())) }

            if (registry.isShuttingDown()) {
                throw RoundAbort(RefreshResult.SHUTDOWN_ABORTED, "shutdown observed before promote; candidate never promoted")
            }

            // PUBLISHING, first half: promote + attach. The registry holds the gen in
            // OPENING so it stays invisible to acquire until the gate has passed (I1).
            registry.beginPublish(gen)
            val attachStart = clock.instant()
            storeOp { store.promote(gen) }
            opened = storeOp { store.open(gen) }
            val attachElapsed = Duration.between(attachStart, clock.instant())

            // VERIFYING, on a connection from the attached generation.
            val verifyStart = clock.instant()
            val verdict = gate.verify(opened, registry.currentInfo())
            emit(group) { events.refreshPhase(group, RefreshPhase.VERIFY, Duration.between(verifyStart, clock.instant())) }
            hooks.at(Hook.AFTER_VERIFY)

            when (verdict) {
                is GateOutcome.Failed -> {
                    log.warnf(
                        "Verify rejected candidate generation %d of group %s: rule=%s detail=%s",
                        gen, group, verdict.rule, verdict.detail,
                    )
                    emit(group) { events.verifyFailed(group, verdict.rule, verdict.detail) }
                    val failures = registry.recordVerifyFailure()
                    if (failures == config.verify.consecutiveFailureThreshold) {
                        log.errorf(
                            "Group %s has failed verification %d times in a row; escalating to critical (spec 8.5)",
                            group, failures,
                        )
                        emit(group) { events.verifyFailureEscalated(group, failures) }
                    }
                    throw RoundAbort(RefreshResult.VERIFY_FAILED, "${verdict.rule}: ${verdict.detail}")
                }
                is GateOutcome.Passed -> {
                    registry.resetVerifyFailures()
                    // Verify runs full-table scans and takes seconds; a shutdown that began
                    // meanwhile must not still swap the pointer and reclaim mid-drain; the
                    // current pointer stays untouched instead.
                    if (registry.isShuttingDown()) {
                        throw RoundAbort(
                            RefreshResult.SHUTDOWN_ABORTED,
                            "shutdown observed after verify; candidate never published",
                        )
                    }
                    // PUBLISHING, second half: the pointer swap.
                    hooks.at(Hook.BEFORE_POINTER_SWAP)
                    val publishedAt = clock.instant()
                    registry.publish(gen, opened, GenerationInfo(gen, dataAsOf, publishedAt, verdict.rowCounts))
                    emit(group) {
                        events.refreshPhase(
                            group,
                            RefreshPhase.PUBLISH,
                            attachElapsed + Duration.between(publishedAt, clock.instant()),
                        )
                    }
                }
            }

            // GC: reclaim what the publish just made non-current.
            reclaimPass()
            return finish(RefreshResult.SUCCESS, generation = gen)
        } catch (interrupted: InterruptedException) {
            Thread.currentThread().interrupt()
            return abort(gen, candidate, opened, RefreshResult.SHUTDOWN_ABORTED, "build interrupted: ${interrupted.describe()}")
        } catch (aborted: RoundAbort) {
            return abort(gen, candidate, opened, aborted.result, aborted.reason)
        }
    }

    /**
     * Failure epilogue, identical for every classified failure that reaches a candidate: record
     * GONE first (the generation can never be handed out), then best-effort I/O cleanup so
     * a failing delete cannot mask the original failure. Current is untouched throughout (I7).
     */
    private fun abort(
        gen: Long,
        candidate: Candidate?,
        opened: OpenGeneration?,
        result: RefreshResult,
        detail: String,
    ): RefreshOutcome {
        runCatching { candidate?.close() }
            .onFailure { log.warnf("Close of aborted candidate %d of group %s failed: %s", gen, group, it.message) }
        registry.discardBuild(gen)
        if (result == RefreshResult.DISK_ERROR) {
            log.warnf("Disk error while building generation %d of group %s; running emergency GC: %s", gen, group, detail)
            reclaimPass()
        }
        // Same protocol as reclaimPass: detach first, delete only once detached. Deleting a
        // file still attached to the serving instance would leave a dangling alias, so a
        // failed detach leaves the file for the startup wipe instead. The delete runs
        // even when no candidate object exists - createCandidate creates the .tmp file
        // before it can fail, and delete is deleteIfExists-safe on both paths.
        val detached = opened == null || runCatching { store.close(gen) }
            .onFailure { log.warnf("Detach of aborted generation %d of group %s failed: %s", gen, group, it.message) }
            .isSuccess
        if (detached) {
            runCatching { store.delete(gen) }
                .onFailure { log.warnf("Delete of aborted generation %d of group %s failed: %s", gen, group, it.message) }
        }
        return finish(result, detail = detail)
    }

    /**
     * One reclaim pass, the round's GC stage: the registry marks eligible generations
     * RECLAIMING under its lock, detach + delete run out here. A generation whose detach
     * fails - a connection still in use (A4) - or whose delete fails is deferred to the
     * next pass with a warning, never blocking the rest of the pass. The retry re-runs
     * both calls, which is why [GenerationStore.close] is contractually idempotent: a
     * close-succeeded-then-delete-failed generation must not wedge in a permanent defer.
     */
    fun reclaimPass(): GcOutcome {
        val marked = registry.beginReclaim()
        if (marked.isEmpty()) return GcOutcome()
        val reclaimed = mutableListOf<Long>()
        val deferred = mutableListOf<Long>()
        for (gen in marked) {
            try {
                store.close(gen)
                store.delete(gen)
            } catch (failure: Exception) {
                log.warnf(
                    "Reclaim of generation %d of group %s failed (%s); deferred to the next pass (spec 9.2)",
                    gen, group, failure.message,
                )
                registry.deferReclaim(gen)
                deferred += gen
                continue
            }
            registry.reclaimed(gen)
            emit(group) { events.generationReclaimed(group, gen) }
            reclaimed += gen
        }
        return GcOutcome(reclaimed, deferred)
    }

    /**
     * Blocked-by-K: refresh pauses without allocating a generation number, and every
     * holding lease is logged with its owner and hold duration so the stuck job is
     * identifiable. Auto-resume needs no machinery - the next round proceeds
     * once the leases release.
     */
    private fun blockedByK(holders: List<LeaseInfo>): RefreshOutcome {
        val now = clock.instant()
        log.warnf(
            "Refresh of group %s is blocked: live generations exceed K=%d; pausing until leases release (spec 6.1)",
            group, registry.maxLive,
        )
        for (lease in holders) {
            log.warnf(
                "Blocking lease on group %s: owner=%s acquiredAt=%s heldFor=%s",
                group, lease.owner, lease.acquiredAt, Duration.between(lease.acquiredAt, now),
            )
        }
        return finish(
            RefreshResult.BLOCKED_BY_K,
            detail = "live generations exceed K=${registry.maxLive}; ${holders.size} lease(s) outstanding",
        )
    }

    /** Every round exit funnels through here: one refreshFinished event per round, [generation] only on success. */
    private fun finish(result: RefreshResult, generation: Long? = null, detail: String? = null): RefreshOutcome {
        emit(group) { events.refreshFinished(group, result, generation) }
        return RefreshOutcome(result, generation, detail)
    }

    /** A [GenerationStore] operation throwing classifies as a disk problem, not a source one. */
    private inline fun <T> storeOp(op: () -> T): T = try {
        op()
    } catch (interrupted: InterruptedException) {
        throw interrupted
    } catch (failure: Exception) {
        throw RoundAbort(RefreshResult.DISK_ERROR, failure.describe())
    }

}
