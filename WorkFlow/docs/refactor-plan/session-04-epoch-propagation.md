# Session 4 — Epoch Propagation to DB Writes

**Tier:** 1 (correctness — fencing token is currently decorative)
**Prerequisites:** Session 3 (epoch fallback fix)
**Estimated scope:** Schema migration + repository changes across all write paths + tests

---

## Item

### R1.6 — Wire fencing epoch into all leader-gated DB writes

**Problem:** `LeaderElection.token` (the fencing epoch from K8s `leaseTransitions`) is read on acquisition but never checked at any DB write boundary. The epoch is decorative. An old leader (epoch N) can write to the DB after a new leader (epoch N+1) has already advanced the workflow. The CAS `version` column prevents double-advance within a single term but not cross-term races where both leaders hold different versions of the same workflow.

**Scope:** The epoch must be checked on every **leader-gated** write — i.e., writes that only the leader should perform. Consumer writes (task claiming, task completion) happen on all pods and are not leader-gated; they rely on `claim_id` fencing (Session 2) instead.

Leader-gated writes are exclusively in `Sweeper`:
1. `taskRepo.resetStaleTasks()` — bulk UPDATE PROCESSING → PENDING
2. `taskRepo.deadLetterExhaustedTasks()` — bulk UPDATE PROCESSING → DEAD_LETTER (added in Session 2)
3. `barrierService.onTaskCompleted()` called from `failExpiredTasks` — UPDATE task status + CAS workflow advance
4. `barrierService.recoverStuckWorkflow()` — CAS workflow advance

**Schema change (new migration `V3__add_epoch_column.sql`):**
```sql
ALTER TABLE workflow ADD leader_epoch NUMBER(19) DEFAULT 0 NOT NULL;
```

Note: the epoch is on the `workflow` table only. Task-level writes from the sweeper either:
- Reset tasks to PENDING (no correctness issue if an old leader does this — the task re-enters the pool)
- Mark tasks DEAD_LETTER (idempotent — reaper retry will find the same result)

The critical write is `casAdvanceWithHandle` on the `workflow` table, which must not be performed by a stale leader.

**Files to modify:**

1. `src/main/resources/db/migration/V3__add_epoch_column.sql` — new file

2. `src/main/kotlin/engine/WorkflowModels.kt` — add `leaderEpoch: Long` to `WorkflowRun`

3. `src/main/kotlin/engine/WorkflowRepository.kt`:
   - `casAdvanceWithHandle`: add `AND leader_epoch <= :epoch` to WHERE, set `leader_epoch = :epoch` in SET
   ```sql
   UPDATE workflow
   SET current_sequence = :nextSequence, version = version + 1, leader_epoch = :epoch
   WHERE id = :id AND current_sequence = :expectedSequence
     AND version = :expectedVersion AND leader_epoch <= :epoch
   ```
   - `updateStatusWithHandle`: add `leader_epoch = :epoch` in SET
   - `insertWithHandle`: set `leader_epoch = :epoch` on initial insert
   - Row mapper: map `leader_epoch` column

4. `src/main/kotlin/engine/BarrierService.kt`:
   - Accept `epoch: Long` parameter (from `LeaderElection.token` for sweeper calls, or 0 for worker calls)
   - Pass epoch to `casAdvanceWithHandle`
   - For worker-initiated barrier calls (from `WorkerLoop`): pass `epoch = 0` — the `<= :epoch` guard with epoch=0 will fail, so change the guard to `leader_epoch <= :epoch OR :epoch = 0`:
   ```sql
   AND (leader_epoch <= :epoch OR :epoch = 0)
   ```
   This means: if the caller provides an epoch (leader), enforce it; if epoch is 0 (worker), skip the check.

5. `src/main/kotlin/engine/Sweeper.kt`:
   - Inject `LeaderElection`
   - Pass `leaderElection.token` to all barrier calls

6. `src/main/kotlin/worker/WorkerLoop.kt`:
   - Pass `epoch = 0` to barrier calls (workers are not leader-gated)

**Design rationale:**
- `leader_epoch <= :epoch` (not `== :epoch`) because the epoch can advance between sweeper cycles without the workflow row being touched. The check ensures no stale leader writes over a newer leader's work.
- Workers pass `epoch = 0` to opt out of the check. This is safe because worker writes are fenced by `claim_id` (Session 2), not by epoch.

**Test:**
1. Set workflow `leader_epoch = 10`
2. Attempt `casAdvanceWithHandle` with `epoch = 9` — assert 0 rows, CAS fails (stale leader)
3. Attempt with `epoch = 10` — assert success
4. Attempt with `epoch = 11` — assert success, `leader_epoch` updated to 11
5. Attempt with `epoch = 0` (worker path) — assert success (bypass)

---

## Verification

1. `mvn test` passes
2. `RepositoryTest` extended with epoch-guarded CAS scenarios
3. `SweeperTest` verifies epoch is passed through to barrier
4. `WorkflowIntegrationTest` verifies worker-initiated barriers still work with epoch=0
