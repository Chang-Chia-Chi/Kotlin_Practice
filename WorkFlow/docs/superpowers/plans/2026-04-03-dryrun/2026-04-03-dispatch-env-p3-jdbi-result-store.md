# Dispatch Env P3: JdbiSimulationResultStore Adapter + CDI Producer

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `JdbiSimulationResultStore` — a single JDBI adapter parameterized by table names — and a CDI producer that wires it based on `dispatch.env` config.

**Architecture:** The adapter takes `batchTable` and `eventTable` as constructor parameters. All SQL uses these names via string interpolation (safe — table names are from config, not user input). CDI producer reads `dispatch.env` property and creates the adapter with the correct table names.

**Tech Stack:** Kotlin, JDBI, Oracle, Quarkus CDI

**Important patterns from existing code:**
- Use `jdbi.inTransactionSuspend` / `jdbi.withHandleSuspend` from `infrastructure/persistence/JdbiExtension.kt`
- Use `.bindNull("col", Types.VARCHAR)` for nullable Oracle columns (CLAUDE.md guardrail)
- Truncate `LocalDateTime.now()` to `ChronoUnit.MICROS` for Oracle TIMESTAMP precision
- Follow `JdbiTaskRepository` style: `@ApplicationScoped`, `mapToMap()` + manual row mapping

---

### Task 1: Write tests for JdbiSimulationResultStore

**Files:**
- Create: `src/test/kotlin/dispatch/adapter/persistence/JdbiSimulationResultStoreTest.kt`

- [ ] **Step 1: Write the test class**

Create `src/test/kotlin/dispatch/adapter/persistence/JdbiSimulationResultStoreTest.kt`:

```kotlin
package com.workflow.dispatch.adapter.persistence

import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.model.DispatchDecision
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals

class JdbiSimulationResultStoreTest {

    private lateinit var jdbi: Jdbi
    private lateinit var store: JdbiSimulationResultStore

    @BeforeEach
    fun setUp() {
        jdbi = Jdbi.create("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MODE=ORACLE")
        jdbi.useHandle<Exception> { h ->
            h.execute("""
                CREATE TABLE IF NOT EXISTS dispatch_batch (
                    batch_token  VARCHAR(64)  NOT NULL PRIMARY KEY,
                    status       VARCHAR(16)  NOT NULL,
                    created_at   TIMESTAMP    NOT NULL,
                    config_count INT
                )
            """)
            h.execute("""
                CREATE TABLE IF NOT EXISTS dispatch_event (
                    id             INT AUTO_INCREMENT PRIMARY KEY,
                    batch_token    VARCHAR(64)  NOT NULL,
                    config_id      VARCHAR(64)  NOT NULL,
                    dispatch_order INT          NOT NULL,
                    product_id     VARCHAR(64)  NOT NULL,
                    source_bom_id  VARCHAR(64)  NOT NULL,
                    qty            INT          NOT NULL,
                    target_site_id VARCHAR(64)  NOT NULL,
                    target_bom_id  VARCHAR(64),
                    site_gap       DECIMAL      NOT NULL,
                    bom_gap        DECIMAL
                )
            """)
        }
        store = JdbiSimulationResultStore(jdbi, "dispatch_batch", "dispatch_event")
    }

    @Test
    fun `createBatch inserts batch record`() = runTest {
        store.createBatch("batch1", BatchStatus.NORMAL, 3)

        val status = store.findBatchStatus("batch1")
        assertEquals(BatchStatus.NORMAL, status)
    }

    @Test
    fun `createBatch with DRYRUN status`() = runTest {
        store.createBatch("dryrun1", BatchStatus.DRYRUN, 1)

        val status = store.findBatchStatus("dryrun1")
        assertEquals(BatchStatus.DRYRUN, status)
    }

    @Test
    fun `saveDecisions and findByBatchToken round-trips decisions`() = runTest {
        store.createBatch("batch1", BatchStatus.NORMAL, 1)

        val decisions = listOf(
            DispatchDecision(1, "P1", "BOM-A", 10, "SITE-X", null, BigDecimal("5.0"), null),
            DispatchDecision(2, "P2", "BOM-B", 5, "SITE-Y", "TGT-1", BigDecimal("3.0"), BigDecimal("1.5")),
        )
        store.saveDecisions("batch1", "cfg1", decisions)

        val found = store.findByBatchToken("batch1")
        assertEquals(2, found.size)
        assertEquals("P1", found[0].productId)
        assertEquals(10, found[0].qty)
        assertEquals(null, found[0].targetBomId)
        assertEquals(null, found[0].bomGap)
        assertEquals("P2", found[1].productId)
        assertEquals("TGT-1", found[1].targetBomId)
        assertEquals(BigDecimal("1.5"), found[1].bomGap)
    }

    @Test
    fun `findByBatchToken returns empty list for unknown token`() = runTest {
        val found = store.findByBatchToken("nonexistent")
        assertEquals(emptyList(), found)
    }

    @Test
    fun `findByBatchToken returns decisions across configs`() = runTest {
        store.createBatch("batch1", BatchStatus.NORMAL, 2)

        store.saveDecisions("batch1", "cfg1", listOf(
            DispatchDecision(1, "P1", "BOM-A", 10, "SITE-X", null, BigDecimal("5.0"), null),
        ))
        store.saveDecisions("batch1", "cfg2", listOf(
            DispatchDecision(1, "P2", "BOM-B", 8, "SITE-Y", null, BigDecimal("3.0"), null),
        ))

        val found = store.findByBatchToken("batch1")
        assertEquals(2, found.size)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="JdbiSimulationResultStoreTest" -pl WorkFlow`
Expected: FAIL — `JdbiSimulationResultStore` does not exist.

- [ ] **Step 3: Commit failing tests**

```bash
git add src/test/kotlin/dispatch/adapter/persistence/JdbiSimulationResultStoreTest.kt
git commit -m "test(dispatch): add JdbiSimulationResultStore tests"
```

---

### Task 2: Implement JdbiSimulationResultStore

**Files:**
- Create: `src/main/kotlin/dispatch/adapter/persistence/JdbiSimulationResultStore.kt`

- [ ] **Step 1: Create the adapter**

Create `src/main/kotlin/dispatch/adapter/persistence/JdbiSimulationResultStore.kt`:

```kotlin
package com.workflow.dispatch.adapter.persistence

import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.model.DispatchDecision
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.infrastructure.persistence.inTransactionSuspend
import com.workflow.infrastructure.persistence.withHandleSuspend
import org.jdbi.v3.core.Jdbi
import java.math.BigDecimal
import java.sql.Types
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

class JdbiSimulationResultStore(
    private val jdbi: Jdbi,
    private val batchTable: String,
    private val eventTable: String,
) : SimulationResultStore {

    override suspend fun createBatch(batchToken: String, status: BatchStatus, configCount: Int) {
        val now = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS)
        jdbi.inTransactionSuspend<Unit, Exception> { h ->
            h.createUpdate("INSERT INTO $batchTable (batch_token, status, created_at, config_count) VALUES (:token, :status, :createdAt, :count)")
                .bind("token", batchToken)
                .bind("status", status.name)
                .bind("createdAt", now)
                .bind("count", configCount)
                .execute()
        }
    }

    override suspend fun findBatchStatus(batchToken: String): BatchStatus {
        return jdbi.withHandleSuspend<BatchStatus, Exception> { h ->
            val status = h.createQuery("SELECT status FROM $batchTable WHERE batch_token = :token")
                .bind("token", batchToken)
                .mapTo(String::class.java)
                .one()
            BatchStatus.valueOf(status)
        }
    }

    override suspend fun saveDecisions(batchToken: String, configId: String, decisions: List<DispatchDecision>) {
        if (decisions.isEmpty()) return
        jdbi.inTransactionSuspend<Unit, Exception> { h ->
            val batch = h.prepareBatch(
                """INSERT INTO $eventTable
                   (batch_token, config_id, dispatch_order, product_id, source_bom_id,
                    qty, target_site_id, target_bom_id, site_gap, bom_gap)
                   VALUES (:batchToken, :configId, :dispatchOrder, :productId, :sourceBomId,
                           :qty, :targetSiteId, :targetBomId, :siteGap, :bomGap)"""
            )
            for (d in decisions) {
                batch.bind("batchToken", batchToken)
                    .bind("configId", configId)
                    .bind("dispatchOrder", d.dispatchOrder)
                    .bind("productId", d.productId)
                    .bind("sourceBomId", d.sourceBomId)
                    .bind("qty", d.qty)
                    .bind("targetSiteId", d.targetSiteId)
                    .bind("siteGap", d.siteGap)
                if (d.targetBomId != null) batch.bind("targetBomId", d.targetBomId)
                else batch.bindNull("targetBomId", Types.VARCHAR)
                if (d.bomGap != null) batch.bind("bomGap", d.bomGap)
                else batch.bindNull("bomGap", Types.DECIMAL)
                batch.add()
            }
            batch.execute()
        }
    }

    override suspend fun findByBatchToken(batchToken: String): List<DispatchDecision> {
        return jdbi.withHandleSuspend<List<DispatchDecision>, Exception> { h ->
            h.createQuery(
                """SELECT dispatch_order, product_id, source_bom_id, qty,
                          target_site_id, target_bom_id, site_gap, bom_gap
                   FROM $eventTable WHERE batch_token = :token
                   ORDER BY config_id, dispatch_order"""
            )
                .bind("token", batchToken)
                .mapToMap()
                .list()
                .map { row ->
                    DispatchDecision(
                        dispatchOrder = (row["dispatch_order"] as Number).toInt(),
                        productId = row["product_id"] as String,
                        sourceBomId = row["source_bom_id"] as String,
                        qty = (row["qty"] as Number).toInt(),
                        targetSiteId = row["target_site_id"] as String,
                        targetBomId = row["target_bom_id"] as String?,
                        siteGap = row["site_gap"] as BigDecimal,
                        bomGap = row["bom_gap"] as BigDecimal?,
                    )
                }
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="JdbiSimulationResultStoreTest" -pl WorkFlow`
Expected: All 5 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/dispatch/adapter/persistence/JdbiSimulationResultStore.kt
git commit -m "feat(dispatch): implement JdbiSimulationResultStore with parameterized table names"
```

---

### Task 3: Add CDI producer for SimulationResultStore

**Files:**
- Create: `src/main/kotlin/dispatch/adapter/persistence/DispatchPersistenceProducer.kt`

- [ ] **Step 1: Create the CDI producer**

Create `src/main/kotlin/dispatch/adapter/persistence/DispatchPersistenceProducer.kt`:

```kotlin
package com.workflow.dispatch.adapter.persistence

import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jdbi.v3.core.Jdbi

@ApplicationScoped
class DispatchPersistenceProducer {

    @Produces
    @ApplicationScoped
    fun simulationResultStore(
        @ConfigProperty(name = "dispatch.env", defaultValue = "prod") env: String,
        jdbi: Jdbi,
    ): SimulationResultStore {
        val (batchTable, eventTable) = when (env) {
            "prod" -> "dispatch_batch" to "dispatch_event"
            "stg" -> "dispatch_batch_stg" to "dispatch_event_stg"
            else -> throw IllegalArgumentException("Unknown dispatch.env: $env")
        }
        return JdbiSimulationResultStore(jdbi, batchTable, eventTable)
    }
}
```

- [ ] **Step 2: Add dispatch.env to application.properties**

In `src/main/resources/application.properties`, add at the end of the file under a new section:

```properties
# =============================================================================
# Dispatch Environment
# =============================================================================
dispatch.env=${DISPATCH_ENV:prod}
```

- [ ] **Step 3: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl WorkFlow`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/dispatch/adapter/persistence/DispatchPersistenceProducer.kt src/main/resources/application.properties
git commit -m "feat(dispatch): add CDI producer for env-aware SimulationResultStore"
```

---

### Task 4: Update existing handler tests to mock new port methods

**Files:**
- Modify: `src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt`

- [ ] **Step 1: Update mocks to include new methods**

The existing `DispatchHandlersTest` uses `mock<SimulationResultStore>()`. Since Mockito mocks return defaults for un-stubbed methods (null for objects, 0 for numbers), the existing tests should still pass without changes. Verify this.

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchHandlersTest" -pl WorkFlow`
Expected: All existing tests PASS (new methods on the interface are un-stubbed but unused by current handler code).

- [ ] **Step 2: Commit (if any fixup was needed)**

Only commit if changes were required. If tests passed without changes, skip this step.
