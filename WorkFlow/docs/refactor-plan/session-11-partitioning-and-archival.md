# Session 11 — Table Partitioning, Archival & Metric Cardinality

**Tier:** 4 (architecture improvements)
**Prerequisites:** Session 5 (enqueued_at column exists)
**Estimated scope:** Schema migration + archive job + metric fix + tests

---

## Items

### R4.5 — Add task table partitioning

**Problem:** Completed and failed tasks accumulate in the hot `task` table indefinitely. Over months, the `(status, enqueued_at)` index grows, SKIP LOCKED scans slow down, and maintenance operations (DELETE, ANALYZE) become expensive. Oracle range partitioning allows fast partition-level operations.

**Schema change (new migration `V6__partitioning_and_archive.sql`):**

Note: Oracle does not support `ALTER TABLE ... PARTITION BY` on an existing table with data. The migration must recreate the table. For a live system, use `DBMS_REDEFINITION` or create a new partitioned table and swap.

For a greenfield deployment or dev/test:
```sql
-- Create partitioned table (for new deployments)
CREATE TABLE task_partitioned (
    -- same columns as task
    -- ...
) PARTITION BY RANGE (enqueued_at) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(
    PARTITION p_initial VALUES LESS THAN (TO_TIMESTAMP('2025-01-01', 'YYYY-MM-DD'))
);

-- For existing deployments, use online redefinition:
-- EXEC DBMS_REDEFINITION.START_REDEF_TABLE(...)
-- This is an operational procedure, not a Flyway migration.
```

**Recommendation:** For the initial implementation, add partitioning to the DDL for new deployments (`V1__create_workflow_tables.sql` or a conditional V6). For existing deployments, provide an operational runbook using `DBMS_REDEFINITION`.

**Files to modify:**
- `src/main/resources/db/migration/V6__partitioning_and_archive.sql` — new table + archive table DDL
- Document the `DBMS_REDEFINITION` procedure in `docs/operations/partitioning.md`

---

### R4.6 — Add archive table and purge job

**Problem:** Terminal tasks (COMPLETED, FAILED, DEAD_LETTER, CANCELLED) stay in the hot table forever. They are never queried by the claiming or reaper logic but degrade index performance.

**Schema change (same migration `V6__partitioning_and_archive.sql`):**
```sql
CREATE TABLE task_archive AS SELECT * FROM task WHERE 1=0;
-- Add same indexes as task table for historical queries
CREATE INDEX idx_task_archive_wf ON task_archive (workflow_id, sequence_number);
CREATE INDEX idx_task_archive_status ON task_archive (status, completed_at);
```

**New scheduled job:**
```kotlin
@Singleton
class TaskArchiver(
    private val jdbi: Jdbi,
    private val config: FrameworkConfig,
) {
    @Scheduled(every = "PT1H", skipExecutionIf = NotLeader::class)
    suspend fun archiveCompletedTasks() {
        val cutoff = Instant.now().minus(config.archiver().retentionPeriod()) // e.g., 7 days
        jdbi.inTransactionSuspend { handle ->
            // Move terminal tasks older than cutoff to archive
            val archived = handle.createUpdate("""
                INSERT INTO task_archive
                SELECT * FROM task
                WHERE status IN ('COMPLETED', 'FAILED', 'DEAD_LETTER', 'CANCELLED')
                  AND completed_at < :cutoff
            """).bind("cutoff", cutoff).execute()

            handle.createUpdate("""
                DELETE FROM task
                WHERE status IN ('COMPLETED', 'FAILED', 'DEAD_LETTER', 'CANCELLED')
                  AND completed_at < :cutoff
            """).bind("cutoff", cutoff).execute()

            if (archived > 0) log.infof("Archived %d tasks older than %s", archived, cutoff)
        }
    }
}
```

**Config addition:**
```kotlin
// In FrameworkConfig:
interface ArchiverConfig {
    @WithDefault("P7D")
    fun retentionPeriod(): Duration
}
fun archiver(): ArchiverConfig
```

```properties
# application.properties
framework.archiver.retention-period=P7D
```

**Test:**
1. Create tasks with `completed_at` 8 days ago and 1 day ago
2. Run archiver
3. Assert old tasks moved to `task_archive`, recent tasks remain in `task`
4. Assert archived tasks are queryable from `task_archive`

---

### R4.8 — Remove `task_backlog_depth` high-cardinality `workflow_id` tag

**Problem:** `task_backlog_depth` in `query-exporter.yaml` uses `workflow_id` as a tag column. With thousands of concurrent workflows, this creates thousands of unique time series, causing Micrometer registry OOM and Prometheus scrape timeouts.

**Files to modify:**
- `src/main/resources/query-exporter.yaml` — `task_backlog_depth` query

**Option A: Remove the metric entirely** (simplest — the information is available via direct DB query)

**Option B: Aggregate without `workflow_id`:**
```yaml
task_backlog_depth:
  sql: |
    SELECT COUNT(*) AS depth
    FROM task
    WHERE status = 'PENDING'
  schedule:
    interval: PT30S
  metrics:
    - name: task_backlog_depth
      type: GAUGE
      valueColumn: depth
```

**Option C: Top-N only** (show only the 10 deepest backlogs):
```yaml
task_backlog_depth:
  sql: |
    SELECT workflow_id, depth FROM (
      SELECT workflow_id, COUNT(*) AS depth
      FROM task
      WHERE status = 'PENDING'
      GROUP BY workflow_id
      ORDER BY COUNT(*) DESC
    ) FETCH FIRST 10 ROWS ONLY
  schedule:
    interval: PT60S
  metrics:
    - name: task_backlog_depth
      type: GAUGE
      valueColumn: depth
      tagColumns: [workflow_id]
```

**Recommendation:** Option B for the default metric (safe cardinality). Add Option C as a separate metric (`task_backlog_depth_top10`) for debugging if needed.

**Test:** Verify the updated YAML loads without validation errors. Check `/q/metrics` output for the metric.

---

## Verification

1. `mvn test` passes
2. Migration V6 applies cleanly on Oracle container
3. Archiver test with real Oracle container
4. Metric cardinality verified via `/q/metrics` endpoint (no workflow_id explosion)
