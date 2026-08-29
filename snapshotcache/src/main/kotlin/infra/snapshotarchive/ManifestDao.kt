package infra.snapshotarchive

import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.statement.Query
import java.time.Clock
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

/**
 * Durable state for the archive & diff layer (spec 18.2, D31).
 *
 * The framework itself persists nothing and renumbers generations from 1 on every boot
 * (D10, spec 4.3), so a pod restart destroys every in-process notion of "which snapshot was
 * that". This table is the only thing that survives it, and `data_as_of` is the sole join
 * key between the ephemeral world and the durable one - generation numbers are recorded for
 * diagnostics and are never a key.
 */
enum class ArchiveStatus { PENDING, COMPLETE, FAILED }

/** One manifest row. `inventory` is the json array described in spec 18.2. */
data class ManifestEntry(
    val group: String,
    val version: Long,
    val dataAsOf: Instant,
    val createdAt: Instant,
    val uriPrefix: String,
    val inventory: String,
    val status: ArchiveStatus,
    val generation: Long,
    val updatedAt: Instant,
)

/**
 * Thrown by [ManifestDao.insertPending] when `data_as_of` is not strictly greater than the
 * newest COMPLETE version's (spec 18.3 step 2, D31).
 *
 * This is the one place a timestamp is load-bearing, so the regression is raised rather
 * than returned: the archiver must skip and alert, and a caller that ignores a return value
 * would instead publish a checkpoint that silently moves the diff baseline backwards.
 */
class DataAsOfRegression(
    val group: String,
    val offered: Instant,
    val newestComplete: Instant,
) : RuntimeException(
    "refusing to archive group '$group': data_as_of $offered is not newer than the " +
        "newest COMPLETE version's $newestComplete",
)

/**
 * DDL for the manifest. Applied by the DBA in production; the contract tests apply it to a
 * throwaway container.
 *
 * Two deliberate departures from the illustrative DDL in spec 18.2, both recorded in
 * progress.md. Timestamps are `TIMESTAMP WITH TIME ZONE` rather than bare `TIMESTAMP`: the
 * column is read back by a different process than wrote it, and a bare TIMESTAMP round-trips
 * through whatever zone each JVM happens to have, which would silently shift every
 * `data_as_of` comparison the watermark predicate depends on. And the status column carries
 * a CHECK constraint, so a typo cannot invent a fourth state that readers would treat as
 * "not COMPLETE" forever.
 */
object ManifestSchema {

    const val TABLE: String = "SNAPSHOT_ARCHIVE_MANIFEST"
    const val SEQUENCE: String = "SNAPSHOT_ARCHIVE_VERSION_SEQ"

    val DDL: List<String> = listOf(
        """
        CREATE TABLE $TABLE (
          group_id   VARCHAR2(128)            NOT NULL,
          version    NUMBER(19)               NOT NULL,
          data_as_of TIMESTAMP WITH TIME ZONE NOT NULL,
          created_at TIMESTAMP WITH TIME ZONE NOT NULL,
          uri_prefix VARCHAR2(512)            NOT NULL,
          inventory  CLOB                     NOT NULL,
          status     VARCHAR2(16)             NOT NULL,
          generation NUMBER(19)               NOT NULL,
          updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
          CONSTRAINT snapshot_archive_manifest_pk PRIMARY KEY (group_id, version),
          CONSTRAINT snapshot_archive_status_ck
            CHECK (status IN ('PENDING', 'COMPLETE', 'FAILED'))
        )
        """.trimIndent(),
        // The watermark query is the hot read: newest COMPLETE at or before an instant.
        "CREATE INDEX snapshot_archive_watermark_ix ON $TABLE (group_id, status, data_as_of)",
        // NOCACHE because one version per group per hour makes sequence-cache throughput
        // irrelevant, while a cache is what lets numbers come back out of order after a
        // restart - and MAX(version) is how a watermark is chosen.
        "CREATE SEQUENCE $SEQUENCE START WITH 1 INCREMENT BY 1 NOCACHE",
    )
}

/**
 * The durable half of the archive layer (plan P11).
 *
 * Every status transition is conditional on the row still being PENDING, and reports whether
 * it actually moved. That is not defensiveness: it is the entire mechanism by which an
 * uploader racing the watchdog (ticket 04) resolves to exactly one winner, and there is no
 * second line of defence behind it.
 *
 * [bucket] is the object-store bucket the archiver writes into; the DAO derives `uri_prefix`
 * from it so the layout in spec 18.2 is defined in one place rather than at each call site.
 */
class ManifestDao(
    private val jdbi: Jdbi,
    private val bucket: String,
    private val clock: Clock,
) {

    /**
     * Allocates a version and records the intent to publish it (spec 18.3 step 3).
     *
     * The row lands before a single object is uploaded, carrying the complete inventory. That
     * ordering is what makes a ghost object impossible and is why this layer owns no
     * LIST-based orphan sweep (D33).
     *
     * @throws DataAsOfRegression if [dataAsOf] is not strictly newer than the newest COMPLETE.
     */
    fun insertPending(
        group: String,
        dataAsOf: Instant,
        inventory: String,
        generation: Long,
    ): ManifestEntry = jdbi.inTransaction<ManifestEntry, RuntimeException> { handle ->
        // Guard and insert share one transaction so a concurrent publisher cannot slip a
        // newer COMPLETE row in between. Runs for one group are serialized anyway (spec
        // 18.2), so this closes the window rather than being the only thing holding it shut.
        val newest = newestComplete(handle, group)
        if (newest != null && !dataAsOf.isAfter(newest.dataAsOf)) {
            throw DataAsOfRegression(group, dataAsOf, newest.dataAsOf)
        }

        val version = handle.createQuery("SELECT ${ManifestSchema.SEQUENCE}.NEXTVAL FROM dual")
            .mapTo(Long::class.java)
            .one()
        val now = clock.instant()
        val entry = ManifestEntry(
            group = group,
            version = version,
            dataAsOf = dataAsOf,
            createdAt = now,
            uriPrefix = uriPrefix(group, version),
            inventory = inventory,
            status = ArchiveStatus.PENDING,
            generation = generation,
            updatedAt = now,
        )

        handle.createUpdate(
            """
            INSERT INTO ${ManifestSchema.TABLE}
              (group_id, version, data_as_of, created_at, uri_prefix, inventory, status, generation, updated_at)
            VALUES
              (:group, :version, :dataAsOf, :createdAt, :uriPrefix, :inventory, :status, :generation, :updatedAt)
            """.trimIndent(),
        )
            .bind("group", entry.group)
            .bind("version", entry.version)
            .bind("dataAsOf", entry.dataAsOf.utc())
            .bind("createdAt", entry.createdAt.utc())
            .bind("uriPrefix", entry.uriPrefix)
            .bind("inventory", entry.inventory)
            .bind("status", entry.status.name)
            .bind("generation", entry.generation)
            .bind("updatedAt", entry.updatedAt.utc())
            .execute()

        entry
    }

    /**
     * PENDING -> COMPLETE (spec 18.3 step 5). Returns false, without throwing, when the row
     * was not PENDING - already resolved, or never existed. The caller learns it lost the
     * race instead of assuming it won.
     */
    fun markComplete(group: String, version: Long): Boolean =
        transition(group, version, ArchiveStatus.COMPLETE)

    /** PENDING -> FAILED, for the watchdog's verdict on a stale intent (D33). Same contract as [markComplete]. */
    fun markFailed(group: String, version: Long): Boolean =
        transition(group, version, ArchiveStatus.FAILED)

    /**
     * COMPLETE -> FAILED: the purge's mark step (spec 18.5, plan P13), and the only
     * transition that does not start from PENDING.
     *
     * A version being reclaimed is a version whose objects are about to stop existing, which
     * is precisely what FAILED already means to every reader - "do not trust this one". The
     * mark is what keeps that true across a crash: purge deletes objects before the row, so
     * dying in between would otherwise leave a COMPLETE row whose inventory the bucket can no
     * longer honour. There is no fourth status to invent for it; the CHECK constraint says so
     * and readers would have to learn it.
     */
    fun retire(group: String, version: Long): Boolean =
        transition(group, version, ArchiveStatus.FAILED, from = ArchiveStatus.COMPLETE)

    /**
     * Deletes one row, the last step of reclaiming a version. Returns whether it was there.
     *
     * Always called after its objects are gone, never before: the ordering is what makes an
     * object without a covering manifest row impossible, which is why this layer owns no
     * LIST-based orphan sweep (D33, D34).
     */
    fun delete(group: String, version: Long): Boolean =
        jdbi.withHandle<Boolean, RuntimeException> { handle ->
            handle.createUpdate(
                "DELETE FROM ${ManifestSchema.TABLE} WHERE group_id = :group AND version = :version",
            )
                .bind("group", group)
                .bind("version", version)
                .execute() == 1
        }

    /** The newest COMPLETE version, or null when the group has never published one. */
    fun newestComplete(group: String): ManifestEntry? =
        jdbi.withHandle<ManifestEntry?, RuntimeException> { newestComplete(it, group) }

    /**
     * The watermark predicate of D35, verbatim: `max(version) WHERE status='COMPLETE' AND
     * data_as_of <= at`.
     *
     * The `<= at` half is the whole correctness argument. A checkpoint published while an
     * ETL was running describes state that ETL never processed; adopting it as the next
     * baseline would silently drop every change in the gap. Erring toward an older version
     * only over-reports, which idempotent consumers absorb (D25), so the predicate is
     * allowed to be conservative and is.
     */
    fun watermark(group: String, at: Instant): Long? =
        jdbi.withHandle<Long?, RuntimeException> { handle ->
            handle.createQuery(
                """
                SELECT MAX(version) FROM ${ManifestSchema.TABLE}
                 WHERE group_id = :group AND status = 'COMPLETE' AND data_as_of <= :at
                """.trimIndent(),
            )
                .bind("group", group)
                .bind("at", at.utc())
                .mapTo(Long::class.javaObjectType)
                .findOne()
                .orElse(null)
        }

    /**
     * Versions whose `data_as_of` predates [olderThan], oldest first - the raw retention
     * query.
     *
     * Deliberately dumb: it applies no keep-newest-COMPLETE rule and no status filter,
     * because retention policy is ticket 04's and mixing it in here would put the one rule
     * that protects the last good baseline (D34) somewhere nobody looks for it. Aged by
     * `data_as_of` rather than `created_at`: what makes a checkpoint useful to an ETL is how
     * stale its data is, and that is the same clock the watermark predicate reads.
     */
    fun expired(group: String, olderThan: Instant): List<ManifestEntry> =
        jdbi.withHandle<List<ManifestEntry>, RuntimeException> { handle ->
            handle.createQuery(
                """
                ${SELECT_ALL}
                 WHERE group_id = :group AND data_as_of < :olderThan
                 ORDER BY version
                """.trimIndent(),
            )
                .bind("group", group)
                .bind("olderThan", olderThan.utc())
                .toEntries()
        }

    /**
     * Versions in [status] whose last transition predates [unchangedSince], oldest first.
     *
     * As dumb as [expired], and for the same reason: it answers both of ticket 04's
     * questions - PENDING rows older than the watchdog timeout, and FAILED rows that have
     * settled long enough to reclaim - while the policy that picks those instants stays where
     * the decision is made. Aged by `updated_at`, which an insert sets alongside `created_at`,
     * so the one column means "how long has this row been in this state" for every status.
     */
    fun byStatus(group: String, status: ArchiveStatus, unchangedSince: Instant): List<ManifestEntry> =
        jdbi.withHandle<List<ManifestEntry>, RuntimeException> { handle ->
            handle.createQuery(
                """
                $SELECT_ALL
                 WHERE group_id = :group AND status = :status AND updated_at < :unchangedSince
                 ORDER BY version
                """.trimIndent(),
            )
                .bind("group", group)
                .bind("status", status.name)
                .bind("unchangedSince", unchangedSince.utc())
                .toEntries()
        }

    /** Reads one row regardless of status. Diagnostics and tests; the protocol never needs it. */
    fun find(group: String, version: Long): ManifestEntry? =
        jdbi.withHandle<ManifestEntry?, RuntimeException> { handle ->
            handle.createQuery("$SELECT_ALL WHERE group_id = :group AND version = :version")
                .bind("group", group)
                .bind("version", version)
                .toEntries()
                .firstOrNull()
        }

    private fun transition(
        group: String,
        version: Long,
        to: ArchiveStatus,
        from: ArchiveStatus = ArchiveStatus.PENDING,
    ): Boolean =
        jdbi.withHandle<Boolean, RuntimeException> { handle ->
            val moved = handle.createUpdate(
                """
                UPDATE ${ManifestSchema.TABLE}
                   SET status = :to, updated_at = :now
                 WHERE group_id = :group AND version = :version AND status = :from
                """.trimIndent(),
            )
                .bind("to", to.name)
                .bind("from", from.name)
                .bind("now", clock.instant().utc())
                .bind("group", group)
                .bind("version", version)
                .execute()
            moved == 1
        }

    private fun newestComplete(handle: Handle, group: String): ManifestEntry? =
        handle.createQuery(
            """
            $SELECT_ALL
             WHERE group_id = :group AND status = 'COMPLETE'
             ORDER BY version DESC
             FETCH FIRST 1 ROWS ONLY
            """.trimIndent(),
        )
            .bind("group", group)
            .toEntries()
            .firstOrNull()

    private fun uriPrefix(group: String, version: Long): String =
        "$bucket/snapshots/$group/v$version/"

    private companion object {

        const val SELECT_ALL: String =
            "SELECT group_id, version, data_as_of, created_at, uri_prefix, inventory, " +
                "status, generation, updated_at FROM ${ManifestSchema.TABLE}"

        /** Bound and read as UTC everywhere, so no JVM's default zone can shift a stored instant. */
        fun Instant.utc(): OffsetDateTime = atOffset(ZoneOffset.UTC)

        fun Query.toEntries(): List<ManifestEntry> = map { rs, _ ->
            ManifestEntry(
                group = rs.getString("group_id"),
                version = rs.getLong("version"),
                dataAsOf = rs.getObject("data_as_of", OffsetDateTime::class.java).toInstant(),
                createdAt = rs.getObject("created_at", OffsetDateTime::class.java).toInstant(),
                uriPrefix = rs.getString("uri_prefix"),
                inventory = rs.getString("inventory"),
                status = ArchiveStatus.valueOf(rs.getString("status")),
                generation = rs.getLong("generation"),
                updatedAt = rs.getObject("updated_at", OffsetDateTime::class.java).toInstant(),
            )
        }.list()
    }
}
