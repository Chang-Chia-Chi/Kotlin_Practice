# Phase 2: SQL Query Optimizations

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Optimize SQL queries that do unnecessary full-table scans or triple-correlated subqueries.

**Architecture:** SQL string changes only — no new port methods, no API changes. All existing tests must pass unchanged since query semantics are preserved.

**Tech Stack:** Kotlin, JDBI, Oracle SQL, JUnit 5

---

## Task 1: SyncRepository — replace NOT IN with NOT EXISTS

**Files:**
- Modify: `src/main/kotlin/dispatch/adapter/persistence/SyncRepository.kt:51-53`
- Test: `src/test/kotlin/dispatch/adapter/persistence/SyncRepositoryTest.kt` (existing, must still pass)

- [ ] **Step 1: Replace NOT IN subquery with NOT EXISTS**

In `src/main/kotlin/dispatch/adapter/persistence/SyncRepository.kt`, replace step 2 (the orphan cleanup SQL):

Find:
```kotlin
            // 2. Delete orphaned stg batches (no remaining events)
            h.createUpdate("""
                DELETE FROM dispatch_batch_stg
                WHERE batch_token NOT IN (SELECT DISTINCT batch_token FROM dispatch_event_stg)
            """).execute()
```

Replace with:
```kotlin
            // 2. Delete orphaned stg batches (no remaining events)
            h.createUpdate("""
                DELETE FROM dispatch_batch_stg b
                WHERE NOT EXISTS (
                    SELECT 1 FROM dispatch_event_stg e WHERE e.batch_token = b.batch_token
                )
            """).execute()
```

Why: `NOT EXISTS` uses a correlated semi-anti-join which Oracle can evaluate with an index on `dispatch_event_stg.batch_token` without materializing the full subquery result set. The original `NOT IN` required scanning all distinct batch_tokens from `dispatch_event_stg` first.

- [ ] **Step 2: Run SyncRepository tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="SyncRepositoryTest"`
Expected: All PASS — same semantics, better execution plan.

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/dispatch/adapter/persistence/SyncRepository.kt
git commit -m "perf(dispatch): SyncRepository uses NOT EXISTS instead of NOT IN for orphan cleanup"
```

---

## Task 2: findStuck — replace triple correlated subquery with CTE

**Files:**
- Modify: `src/main/kotlin/workflow/adapter/persistent/JdbiWorkflowRepository.kt:40-61`
- Test: `src/test/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdogTest.kt` (existing, must still pass)

- [ ] **Step 1: Rewrite findStuck SQL with a CTE for max_seq**

In `src/main/kotlin/workflow/adapter/persistent/JdbiWorkflowRepository.kt`, replace the `findStuck` method:

```kotlin
    override suspend fun findStuck(gracePeriod: Duration): List<WorkflowRun> =
        jdbi.withHandleSuspend<List<WorkflowRun>, Exception> { h: Handle ->
            val cutoff = LocalDateTime.ofInstant(Instant.now().minus(gracePeriod), ZoneOffset.UTC)
            h.createQuery(
                """
                WITH max_seq AS (
                    SELECT workflow_id, MAX(sequence_number) AS max_seq_num
                    FROM task
                    GROUP BY workflow_id
                )
                SELECT w.*
                FROM workflow w
                JOIN max_seq ms ON ms.workflow_id = w.id
                WHERE w.status = 'RUNNING'
                  AND w.updated_at < :cutoff
                  AND NOT EXISTS (
                    SELECT 1 FROM task t
                    WHERE t.workflow_id = w.id
                      AND t.sequence_number = ms.max_seq_num
                      AND t.status NOT IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED', 'SKIPPED')
                  )
                """,
            )
                .bind("cutoff", cutoff)
                .mapToMap()
                .list()
                .map(::mapWorkflowRow)
        }
```

Changes:
- The CTE `max_seq` computes `MAX(sequence_number)` per workflow once (single GROUP BY scan of the task table)
- `JOIN max_seq` replaces both `EXISTS (SELECT 1 FROM task ...)` and the inner `SELECT MAX(...)` correlated subquery
- The NOT EXISTS is now simple: just checks tasks at `ms.max_seq_num`, no nested correlated subquery
- Net effect: 3 correlated subqueries per workflow row → 1 CTE scan + 1 simple NOT EXISTS

- [ ] **Step 2: Run Watchdog tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="WorkflowWatchdogTest"`
Expected: All PASS

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/workflow/adapter/persistent/JdbiWorkflowRepository.kt
git commit -m "perf(workflow): findStuck uses CTE to avoid triple correlated subquery"
```
