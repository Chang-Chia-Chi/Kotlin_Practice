# Session 5 — Schema & Query Performance

**Tier:** 2 (performance and scalability blockers)
**Prerequisites:** Session 2 (migration V2 must exist)
**Estimated scope:** Schema migration + index rebuild + query changes + tests

---

## Items

### R2.1 — Add `enqueued_at` column for FIFO ordering

**Problem:** The claim query orders by `claimed_at NULLS FIRST, id`. For PENDING tasks, `claimed_at` is always NULL, so the tiebreaker is `id` (UUID v4 = random). Tasks are not claimed in FIFO order — under load, recently enqueued tasks can be claimed before older ones, causing starvation.

**Schema change (new migration `V4__enqueued_at_and_indexes.sql`):**
```sql
-- Add enqueued_at with server-time default
ALTER TABLE task ADD enqueued_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL;

-- Backfill existing rows (use created_at if it exists, otherwise claim time or current)
-- For existing tasks, approximate enqueued_at from the earliest known timestamp:
UPDATE task SET enqueued_at = COALESCE(claimed_at, SYSTIMESTAMP) WHERE enqueued_at IS NULL;
```

**Files to modify:**
- `src/main/kotlin/engine/WorkflowModels.kt` — add `enqueuedAt: LocalDateTime` to `Task`
- `src/main/kotlin/engine/TaskRepository.kt`:
  - `claimNext` inner query: change `ORDER BY claimed_at NULLS FIRST, id` to `ORDER BY enqueued_at ASC, id`
  - `insertBatchWithHandle`: do NOT bind `enqueued_at` — let Oracle's DEFAULT SYSTIMESTAMP handle it
- `src/main/kotlin/engine/RowMapperUtils.kt` — map `enqueued_at` column

**Test:** Insert 3 tasks with known ordering, claim them, assert they come back in enqueue order.

---

### R2.2 — Fix index strategy

**Problem:** The current indexes are:
- `idx_task_status_claimed (status, claimed_at)` — useless for PENDING rows (claimed_at is NULL)
- `idx_task_status_deadline (status, deadline_at)` — serves `findExpired` but not `findStale`
- `idx_task_wf_seq_status (workflow_id, sequence_number, status)` — serves barrier counting

Missing: index for claim query ORDER BY, index for reaper's `claimed_at` filter.

**Schema change (same migration `V4__enqueued_at_and_indexes.sql`):**
```sql
-- Drop the ineffective index
DROP INDEX idx_task_status_claimed;

-- Claim query: WHERE status = 'PENDING' ORDER BY enqueued_at ASC
CREATE INDEX idx_task_pending_enqueued ON task (status, enqueued_at, id);

-- Reaper query: WHERE status = 'PROCESSING' AND claimed_at < :threshold
CREATE INDEX idx_task_processing_claimed ON task (status, claimed_at);

-- Barrier fan-in counting: WHERE workflow_id = :wfId AND sequence_number = :seq AND status NOT IN (...)
-- idx_task_wf_seq_status already covers this — keep it
```

**Test:** Run the claim and reaper queries in `RepositoryTest` with `EXPLAIN PLAN` assertions if feasible, or at minimum verify query execution does not degrade with 1000+ rows.

---

### R2.3 — Use DB SYSTIMESTAMP for `claimed_at`

**Problem:** `claimNext` sets `claimed_at = LocalDateTime.now(ZoneOffset.UTC)` from application code. On multi-pod deployments, each pod has its own clock. The reaper threshold is also computed from application time. Clock skew between pods causes incorrect stale detection — a 1-2 second skew with a 10-minute threshold is normally safe, but under NTP drift it becomes dangerous.

**Files to modify:**
- `src/main/kotlin/engine/TaskRepository.kt` — `claimNext` UPDATE statement

**Fix:**
```sql
-- Change:
UPDATE task SET status = 'PROCESSING', claimed_by = :workerId, claimed_at = :claimedAt
-- To:
UPDATE task SET status = 'PROCESSING', claimed_by = :workerId, claimed_at = SYSTIMESTAMP
```

Remove the `claimedAt` parameter binding. The DB server clock is the single source of truth.

Also update `resetStaleTasks` and `findStale` to compare against DB time:
```sql
-- Change:
WHERE ... AND claimed_at < :threshold
-- To:
WHERE ... AND claimed_at < (SYSTIMESTAMP - INTERVAL :thresholdSeconds SECOND)
```

Or continue passing the threshold as a parameter but document that it must be computed from a value that tolerates clock skew (which it does when comparing DB-written `claimed_at` against DB-evaluated SYSTIMESTAMP).

**Simpler approach:** Just fix the write side (`claimed_at = SYSTIMESTAMP`). The read side threshold comparison is now DB-time vs DB-time, eliminating skew. The threshold parameter from application code only controls the duration, not the reference point.

**Test:** Claim a task, read back `claimed_at`, verify it is within a few seconds of `SYSTIMESTAMP` (not tied to application JVM clock).

---

## Verification

1. `mvn test` passes
2. Migration V4 applies cleanly on Oracle container
3. `RepositoryTest` verifies FIFO ordering, index usage, and SYSTIMESTAMP-based claiming
4. Existing `WorkflowIntegrationTest` still passes (no behavioral regression)
