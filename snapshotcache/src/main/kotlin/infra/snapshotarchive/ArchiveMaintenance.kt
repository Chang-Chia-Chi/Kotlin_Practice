package infra.snapshotarchive

import infra.snapshotcache.api.GroupId
import org.jboss.logging.Logger
import java.time.Clock
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * The archive layer's maintenance pass (spec 18.3 watchdog, 18.5 retention; plan P13).
 *
 * Two jobs, one thread, no state. The watchdog drags every PENDING row to a terminal status,
 * so a crashed or interrupted run is repaired without anyone intervening; the purge holds
 * storage inside a fixed window. Both are idempotent, which is the whole design: a pass that
 * dies halfway is repaired by the next one rather than by remembering where it stopped.
 *
 * What is deliberately absent is an orphan sweep. D33 commits a manifest row carrying the
 * complete inventory before the first object is uploaded, so an object without a covering row
 * cannot come into being, and everything here reads the inventory rather than the bucket.
 * Scanning for a state that is impossible by construction would be worse than useless - it
 * would quietly become the thing the ordering is trusted to instead of the ordering itself.
 * [ObjectStore] therefore has no `list` and `ArchitectureTest` fails the build if MinIO's
 * listing API is ever named in this package.
 */
class ArchiveMaintenance(
    private val manifest: ManifestDao,
    private val objects: ObjectStore,
    private val groups: Collection<GroupId>,
    private val clock: Clock,
    private val watchdogTimeout: Duration = DEFAULT_WATCHDOG_TIMEOUT,
    private val retention: Duration = Duration.ofHours(48),
    private val stalenessThreshold: Duration = Duration.ofHours(3),
) : AutoCloseable {

    private val scheduler = Executors.newSingleThreadScheduledExecutor { runnable ->
        Thread(runnable, "archive-maintenance")
    }

    /** Schedules [sweep] at [interval]. Scheduling stops in [close]. */
    fun start(interval: Duration) {
        val millis = interval.toMillis()
        scheduler.scheduleAtFixedRate({ sweep() }, millis, millis, TimeUnit.MILLISECONDS)
    }

    /**
     * One pass over every configured group: resolve, reclaim, then report staleness.
     *
     * A group that throws does not take the others - or the schedule - down with it. A
     * `scheduleAtFixedRate` task that escapes with an exception is silently never run again,
     * which for a self-healing pass would mean the healing stops at the first bad hour and
     * nothing says so.
     */
    fun sweep() {
        for (group in groups) {
            try {
                watchdog(group)
                purge(group)
                staleness(group)
            } catch (e: Exception) {
                log.errorf(e, "archive maintenance pass for group '%s' failed", group)
            }
        }
    }

    /**
     * Resolves every PENDING row older than the watchdog timeout against its own inventory
     * (spec 18.3, D33): COMPLETE when the bucket holds every object at the recorded size,
     * FAILED when it does not.
     *
     * An uploader still working while this runs is not a hazard to design around. Both sides
     * go through the ticket-02 conditional transitions, so exactly one moves the row and the
     * other is told it moved nothing - and if the uploader has in fact finished every object,
     * the verdict this pass reaches is the same one the uploader was about to reach anyway.
     *
     * Verification is presence plus size, not checksum. An object store publishes an object
     * whole or not at all, so a truncated upload is a missing key rather than a short one,
     * and the alternative - downloading every object to re-hash it - would make the repair
     * path cost more than the run it repairs. The SHA-256 is in the inventory for anyone who
     * later wants a stronger verdict.
     */
    fun watchdog(group: GroupId) {
        val cutoff = clock.instant().minus(watchdogTimeout)
        for (entry in manifest.byStatus(group.value, ArchiveStatus.PENDING, cutoff)) {
            val inventory = Inventory.decode(entry.inventory)
            val missing = inventory.filter { objects.sizeOf(keyOf(entry, it)) != it.bytes }
            val moved =
                if (missing.isEmpty()) manifest.markComplete(group.value, entry.version)
                else manifest.markFailed(group.value, entry.version)

            when {
                !moved -> log.infof(
                    "archive version %d for group '%s' was resolved by its own uploader while " +
                        "the watchdog was verifying it; this pass changed nothing",
                    entry.version, group,
                )

                missing.isEmpty() -> log.infof(
                    "watchdog completed archive version %d for group '%s': all %d objects " +
                        "match the inventory, so the run had uploaded everything before it died",
                    entry.version, group, inventory.size,
                )

                else -> log.warnf(
                    "ALERT: watchdog failed archive version %d for group '%s': %d of %d objects " +
                        "are missing or the wrong size (%s). Consumers fall back to a full " +
                        "compare, which is correct but wasteful - check why the run did not finish",
                    entry.version, group, missing.size, inventory.size, missing.map { it.objectKey },
                )
            }
        }
    }

    /**
     * Enforces retention (spec 18.5, D34): everything past the window goes, except the newest
     * COMPLETE version, which never does.
     *
     * Keep-newest is unconditional and is the point of the rule. An archiver that has stopped
     * publishing would otherwise have its last good baseline aged out from under the
     * consumers - turning a broken archiver into a full compare on every run, forever, at the
     * moment nobody is watching. The window is not "keep latest only" for the mirror-image
     * reason: an ETL slower than the archive cadence needs a baseline that is older than the
     * newest one.
     *
     * PENDING rows are never reclaimed here. They belong to [watchdog], which reaches a
     * verdict on them first, so a version becomes collectable one pass after it becomes
     * resolvable and never while an uploader might still be writing into it. FAILED rows are
     * reclaimed on their own clock rather than the retention window, since a broken uploader
     * would otherwise pile up a window's worth of garbage nothing can ever read - but only
     * once they have been FAILED for longer than the watchdog timeout, which is by definition
     * longer than an upload can take, so nothing is uploading into one when its objects go.
     */
    fun purge(group: GroupId) {
        val now = clock.instant()
        val newest = manifest.newestComplete(group.value)?.version
        val expired = manifest.expired(group.value, now.minus(retention))
            .filter { it.status == ArchiveStatus.COMPLETE }
        val failed = manifest.byStatus(group.value, ArchiveStatus.FAILED, now.minus(watchdogTimeout))

        (expired + failed)
            .distinctBy { it.version }
            .filter { it.version != newest }
            .forEach { reclaim(group, it) }
    }

    /**
     * Reclaims one version: mark, delete objects per its inventory, delete the row.
     *
     * The order is the contract. Objects go before the row so that a crash in the middle
     * leaves objects a row still covers - recoverable by simply running again - rather than
     * objects nothing points at, which no amount of later bookkeeping could find without the
     * bucket scan D33 exists to make unnecessary. The mark protects the same property one
     * level up: without it, dying between the objects and the row would leave a COMPLETE row
     * whose inventory the bucket can no longer honour, and COMPLETE is what readers trust.
     */
    private fun reclaim(group: GroupId, entry: ManifestEntry) {
        if (entry.status == ArchiveStatus.COMPLETE) manifest.retire(group.value, entry.version)
        Inventory.decode(entry.inventory).forEach { objects.delete(keyOf(entry, it)) }
        manifest.delete(group.value, entry.version)
        log.infof(
            "reclaimed archive version %d for group '%s' (was %s, data_as_of %s)",
            entry.version, group, entry.status, entry.dataAsOf,
        )
    }

    /**
     * Spec 18.5's staleness alert: how old the newest COMPLETE checkpoint's data is, and a
     * warning when that exceeds the threshold. Null when the group has never published one,
     * which alerts too - never having published is the stalest state there is.
     *
     * Purely operational. A stale baseline does not make a diff wrong; it makes it
     * over-report, which idempotent consumers absorb by design (D25). What it does mean is
     * that the archiver has quietly stopped doing its job, and the only symptom otherwise is
     * ETLs getting slower.
     */
    fun staleness(group: GroupId): Duration? {
        val newest = manifest.newestComplete(group.value)
        if (newest == null) {
            log.warnf(
                "ALERT: group '%s' has no COMPLETE archive version at all; every consuming ETL " +
                    "is doing a full compare on every run",
                group,
            )
            return null
        }
        val age = Duration.between(newest.dataAsOf, clock.instant())
        if (age > stalenessThreshold) {
            log.warnf(
                "ALERT: the newest COMPLETE archive version for group '%s' (v%d) is %s old, past " +
                    "the %s threshold; the archiver has probably stopped publishing. Diffs stay " +
                    "correct and only over-report, so this is a cost problem, not a data problem",
                group, newest.version, age, stalenessThreshold,
            )
        }
        return age
    }

    /** Stops scheduling. In-flight passes are idempotent, so nothing needs to be waited for. */
    override fun close() {
        scheduler.shutdownNow()
    }

    private fun keyOf(entry: ManifestEntry, obj: ArchivedObject): String =
        entry.uriPrefix.removePrefix("${objects.bucket}/") + obj.objectKey

    companion object {

        /**
         * How long a PENDING row is left alone before the watchdog rules on it.
         *
         * **Spec 18.6 item 3 is still open and this number does not close it.** T is supposed
         * to come from the worst-case upload time on the real MinIO link, and there is no such
         * link on this machine - only a container on loopback, which sizes nothing. What the
         * value is derived from instead is the shape of the cost function, the same argument
         * D22 makes for `waitBudget`:
         *
         * - Too low costs real work. A version failed while its uploader was still running is
         *   a checkpoint thrown away, and every ETL that would have used it as a baseline does
         *   a full compare instead. Correct (D34), but wasteful, and invisible.
         * - Too high costs only latency of repair. A crashed run's row sits PENDING a while
         *   longer; readers already ignore anything that is not COMPLETE, so nothing downstream
         *   can tell the difference except that the repair is late.
         *
         * One-sided like that, the number should be given headroom rather than estimated. 15
         * minutes is four times under the hourly archive cadence - so a stale row is always
         * resolved before the next run for its group - and, against the ~14 MB per million
         * rows that ticket 01 measured, orders of magnitude above any upload a link one would
         * actually deploy on could take. Set it from a measurement when there is a real link to
         * measure; until then the margin, not the estimate, is what makes it safe.
         */
        val DEFAULT_WATCHDOG_TIMEOUT: Duration = Duration.ofMinutes(15)

        private val log: Logger = Logger.getLogger(ArchiveMaintenance::class.java)
    }
}
