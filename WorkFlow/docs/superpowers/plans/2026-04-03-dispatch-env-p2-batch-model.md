# Dispatch Env P2: BatchStatus Model + SimulationResultStore Port Extension

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `BatchStatus` enum and `DispatchBatch` data class to the domain model. Extend `SimulationResultStore` port with batch-level operations (`createBatch`, `findBatchStatus`).

**Architecture:** Pure domain types in `dispatch/model/`. Port extension is additive — existing `saveDecisions` and `findByBatchToken` methods unchanged. New methods support batch lifecycle tracking needed by handlers.

**Tech Stack:** Kotlin

---

### Task 1: Add BatchStatus enum

**Files:**
- Create: `src/main/kotlin/dispatch/model/BatchStatus.kt`
- Test: `src/test/kotlin/dispatch/model/DispatchModelsTest.kt`

- [ ] **Step 1: Write the failing test**

In `src/test/kotlin/dispatch/model/DispatchModelsTest.kt`, add:

```kotlin
@Test
fun `BatchStatus has NORMAL and DRYRUN values`() {
    assertEquals(BatchStatus.NORMAL, BatchStatus.valueOf("NORMAL"))
    assertEquals(BatchStatus.DRYRUN, BatchStatus.valueOf("DRYRUN"))
    assertEquals(2, BatchStatus.entries.size)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchModelsTest" -pl WorkFlow`
Expected: FAIL — `BatchStatus` does not exist.

- [ ] **Step 3: Create BatchStatus enum**

Create `src/main/kotlin/dispatch/model/BatchStatus.kt`:

```kotlin
package com.workflow.dispatch.model

enum class BatchStatus {
    NORMAL,
    DRYRUN,
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchModelsTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/dispatch/model/BatchStatus.kt src/test/kotlin/dispatch/model/DispatchModelsTest.kt
git commit -m "feat(dispatch): add BatchStatus enum"
```

---

### Task 2: Add DispatchBatch data class

**Files:**
- Create: `src/main/kotlin/dispatch/model/DispatchBatch.kt`
- Modify: `src/test/kotlin/dispatch/model/DispatchModelsTest.kt`

- [ ] **Step 1: Write the failing test**

In `src/test/kotlin/dispatch/model/DispatchModelsTest.kt`, add:

```kotlin
@Test
fun `DispatchBatch holds batch metadata`() {
    val now = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS)
    val batch = DispatchBatch(
        batchToken = "20260403060000",
        status = BatchStatus.NORMAL,
        createdAt = now,
        configCount = 3,
    )
    assertEquals("20260403060000", batch.batchToken)
    assertEquals(BatchStatus.NORMAL, batch.status)
    assertEquals(now, batch.createdAt)
    assertEquals(3, batch.configCount)
}
```

Add required imports: `java.time.LocalDateTime`, `java.time.temporal.ChronoUnit`.

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchModelsTest" -pl WorkFlow`
Expected: FAIL — `DispatchBatch` does not exist.

- [ ] **Step 3: Create DispatchBatch data class**

Create `src/main/kotlin/dispatch/model/DispatchBatch.kt`:

```kotlin
package com.workflow.dispatch.model

import java.time.LocalDateTime

data class DispatchBatch(
    val batchToken: String,
    val status: BatchStatus,
    val createdAt: LocalDateTime,
    val configCount: Int,
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchModelsTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/dispatch/model/DispatchBatch.kt src/test/kotlin/dispatch/model/DispatchModelsTest.kt
git commit -m "feat(dispatch): add DispatchBatch data class"
```

---

### Task 3: Extend SimulationResultStore port with batch operations

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/port/outbound/persistence/SimulationResultStore.kt`

- [ ] **Step 1: Add batch methods to the port interface**

In `src/main/kotlin/dispatch/usecase/port/outbound/persistence/SimulationResultStore.kt`, add the new methods:

```kotlin
package com.workflow.dispatch.usecase.port.outbound.persistence

import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.model.DispatchDecision

interface SimulationResultStore {
    suspend fun createBatch(batchToken: String, status: BatchStatus, configCount: Int)
    suspend fun findBatchStatus(batchToken: String): BatchStatus
    suspend fun saveDecisions(batchToken: String, configId: String, decisions: List<DispatchDecision>)
    suspend fun findByBatchToken(batchToken: String): List<DispatchDecision>
}
```

- [ ] **Step 2: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl WorkFlow`
Expected: Compilation may fail if there are mock implementations — that's expected and will be fixed in P3.

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/dispatch/usecase/port/outbound/persistence/SimulationResultStore.kt
git commit -m "feat(dispatch): extend SimulationResultStore port with batch operations"
```
