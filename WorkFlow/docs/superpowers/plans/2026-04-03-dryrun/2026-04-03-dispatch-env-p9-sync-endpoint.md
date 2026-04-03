# Dispatch Env P9: Sync Endpoint (Stg Only)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `POST /dispatch/sync` REST endpoint gated to stg profile. It performs a selective replace of dispatch events from prod tables to stg tables in a single transaction.

**Architecture:** A JAX-RS resource annotated with `@IfBuildProfile("stg")` backed by a `SyncRepository` that reads from prod tables (read-only) and writes to stg tables. The sync operation runs in a single JDBI transaction: delete stg events for the specified configs, clean orphaned stg batches, copy matching prod batches + events into stg.

**Tech Stack:** Kotlin, JDBI, Quarkus JAX-RS, Oracle SQL

**Key constraint:** The stg deployment's DB credentials must have read-only access to `dispatch_batch` and `dispatch_event` (prod tables) plus read-write access to `dispatch_batch_stg` and `dispatch_event_stg`.

---

### Task 1: Write tests for SyncRepository

**Files:**
- Create: `src/test/kotlin/dispatch/adapter/persistence/SyncRepositoryTest.kt`

- [ ] **Step 1: Write the test class**

Create `src/test/kotlin/dispatch/adapter/persistence/SyncRepositoryTest.kt`:

```kotlin
package com.workflow.dispatch.adapter.persistence

import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class SyncRepositoryTest {

    private lateinit var jdbi: Jdbi
    private lateinit var repo: SyncRepository

    @BeforeEach
    fun setUp() {
        jdbi = Jdbi.create("jdbc:h2:mem:synctest;DB_CLOSE_DELAY=-1;MODE=ORACLE")
        jdbi.useHandle<Exception> { h ->
            // Prod tables
            h.execute("""
                CREATE TABLE IF NOT EXISTS dispatch_batch (
                    batch_token VARCHAR(64) NOT NULL PRIMARY KEY,
                    status VARCHAR(16) NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    config_count INT
                )
            """)
            h.execute("""
                CREATE TABLE IF NOT EXISTS dispatch_event (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    batch_token VARCHAR(64) NOT NULL,
                    config_id VARCHAR(64) NOT NULL,
                    dispatch_order INT NOT NULL,
                    product_id VARCHAR(64) NOT NULL,
                    source_bom_id VARCHAR(64) NOT NULL,
                    qty INT NOT NULL,
                    target_site_id VARCHAR(64) NOT NULL,
                    target_bom_id VARCHAR(64),
                    site_gap DECIMAL NOT NULL,
                    bom_gap DECIMAL
                )
            """)
            // Stg tables
            h.execute("""
                CREATE TABLE IF NOT EXISTS dispatch_batch_stg (
                    batch_token VARCHAR(64) NOT NULL PRIMARY KEY,
                    status VARCHAR(16) NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    config_count INT
                )
            """)
            h.execute("""
                CREATE TABLE IF NOT EXISTS dispatch_event_stg (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    batch_token VARCHAR(64) NOT NULL,
                    config_id VARCHAR(64) NOT NULL,
                    dispatch_order INT NOT NULL,
                    product_id VARCHAR(64) NOT NULL,
                    source_bom_id VARCHAR(64) NOT NULL,
                    qty INT NOT NULL,
                    target_site_id VARCHAR(64) NOT NULL,
                    target_bom_id VARCHAR(64),
                    site_gap DECIMAL NOT NULL,
                    bom_gap DECIMAL
                )
            """)
            // Seed prod data
            h.execute("DELETE FROM dispatch_event")
            h.execute("DELETE FROM dispatch_batch")
            h.execute("INSERT INTO dispatch_batch VALUES ('batch1', 'NORMAL', CURRENT_TIMESTAMP, 2)")
            h.execute("INSERT INTO dispatch_batch VALUES ('batch2', 'DRYRUN', CURRENT_TIMESTAMP, 1)")
            h.execute("INSERT INTO dispatch_batch VALUES ('batch3', 'NORMAL', CURRENT_TIMESTAMP, 1)")
            h.execute("""INSERT INTO dispatch_event (batch_token, config_id, dispatch_order, product_id, source_bom_id, qty, target_site_id, target_bom_id, site_gap, bom_gap)
                VALUES ('batch1', 'cfg1', 1, 'P1', 'BOM-A', 10, 'SITE-X', NULL, 5.0, NULL)""")
            h.execute("""INSERT INTO dispatch_event (batch_token, config_id, dispatch_order, product_id, source_bom_id, qty, target_site_id, target_bom_id, site_gap, bom_gap)
                VALUES ('batch1', 'cfg2', 1, 'P2', 'BOM-B', 8, 'SITE-Y', NULL, 3.0, NULL)""")
            h.execute("""INSERT INTO dispatch_event (batch_token, config_id, dispatch_order, product_id, source_bom_id, qty, target_site_id, target_bom_id, site_gap, bom_gap)
                VALUES ('batch2', 'cfg1', 1, 'P3', 'BOM-C', 5, 'SITE-Z', NULL, 2.0, NULL)""")
            h.execute("""INSERT INTO dispatch_event (batch_token, config_id, dispatch_order, product_id, source_bom_id, qty, target_site_id, target_bom_id, site_gap, bom_gap)
                VALUES ('batch3', 'cfg1', 1, 'P4', 'BOM-D', 3, 'SITE-W', NULL, 1.0, NULL)""")
            // Clear stg tables
            h.execute("DELETE FROM dispatch_event_stg")
            h.execute("DELETE FROM dispatch_batch_stg")
        }
        repo = SyncRepository(jdbi)
    }

    @Test
    fun `sync copies NORMAL batch events for specified configs`() = runTest {
        val result = repo.syncFromProd(listOf("cfg1"))

        // Should copy batch1 + batch3 (NORMAL batches with cfg1 events), skip batch2 (DRYRUN)
        assertEquals(2, result.batchesCopied)
        assertEquals(2, result.eventsCopied)  // 1 from batch1 + 1 from batch3

        jdbi.useHandle<Exception> { h ->
            val stgBatches = h.createQuery("SELECT batch_token FROM dispatch_batch_stg ORDER BY batch_token")
                .mapTo(String::class.java).list()
            assertEquals(listOf("batch1", "batch3"), stgBatches)

            val stgEvents = h.createQuery("SELECT config_id FROM dispatch_event_stg ORDER BY product_id")
                .mapTo(String::class.java).list()
            assertEquals(listOf("cfg1", "cfg1"), stgEvents)
        }
    }

    @Test
    fun `sync replaces existing stg data for synced configs`() = runTest {
        // Pre-populate stg with old data
        jdbi.useHandle<Exception> { h ->
            h.execute("INSERT INTO dispatch_batch_stg VALUES ('old-batch', 'NORMAL', CURRENT_TIMESTAMP, 1)")
            h.execute("""INSERT INTO dispatch_event_stg (batch_token, config_id, dispatch_order, product_id, source_bom_id, qty, target_site_id, site_gap)
                VALUES ('old-batch', 'cfg1', 1, 'OLD', 'BOM-OLD', 1, 'SITE-OLD', 0.0)""")
        }

        val result = repo.syncFromProd(listOf("cfg1"))

        // Old stg cfg1 data replaced with prod data
        assertEquals(2, result.batchesCopied)
        assertEquals(2, result.eventsCopied)

        jdbi.useHandle<Exception> { h ->
            val stgEvents = h.createQuery("SELECT product_id FROM dispatch_event_stg ORDER BY product_id")
                .mapTo(String::class.java).list()
            // OLD should be gone, replaced with P1 and P4
            assertEquals(listOf("P1", "P4"), stgEvents)

            // old-batch should be cleaned up (orphaned after cfg1 events deleted)
            val stgBatches = h.createQuery("SELECT batch_token FROM dispatch_batch_stg ORDER BY batch_token")
                .mapTo(String::class.java).list()
            assertEquals(listOf("batch1", "batch3"), stgBatches)
        }
    }

    @Test
    fun `sync preserves stg data for non-synced configs`() = runTest {
        // Pre-populate stg with cfg2 data
        jdbi.useHandle<Exception> { h ->
            h.execute("INSERT INTO dispatch_batch_stg VALUES ('stg-batch', 'NORMAL', CURRENT_TIMESTAMP, 1)")
            h.execute("""INSERT INTO dispatch_event_stg (batch_token, config_id, dispatch_order, product_id, source_bom_id, qty, target_site_id, site_gap)
                VALUES ('stg-batch', 'cfg2', 1, 'KEEP', 'BOM-KEEP', 1, 'SITE-KEEP', 0.0)""")
        }

        repo.syncFromProd(listOf("cfg1"))

        // cfg2 data in stg should be untouched
        jdbi.useHandle<Exception> { h ->
            val cfg2Events = h.createQuery("SELECT product_id FROM dispatch_event_stg WHERE config_id = 'cfg2'")
                .mapTo(String::class.java).list()
            assertEquals(listOf("KEEP"), cfg2Events)
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SyncRepositoryTest" -pl WorkFlow`
Expected: FAIL — `SyncRepository` does not exist.

- [ ] **Step 3: Commit failing tests**

```bash
git add src/test/kotlin/dispatch/adapter/persistence/SyncRepositoryTest.kt
git commit -m "test(dispatch): add SyncRepository tests"
```

---

### Task 2: Implement SyncRepository

**Files:**
- Create: `src/main/kotlin/dispatch/adapter/persistence/SyncRepository.kt`

- [ ] **Step 1: Create the repository**

Create `src/main/kotlin/dispatch/adapter/persistence/SyncRepository.kt`:

```kotlin
package com.workflow.dispatch.adapter.persistence

import com.workflow.infrastructure.persistence.inTransactionSuspend
import org.jdbi.v3.core.Jdbi

data class SyncResult(
    val syncedConfigs: List<String>,
    val batchesCopied: Int,
    val eventsCopied: Int,
)

class SyncRepository(private val jdbi: Jdbi) {

    suspend fun syncFromProd(configIds: List<String>): SyncResult {
        return jdbi.inTransactionSuspend<SyncResult, Exception> { h ->
            // 1. Delete stg events for the specified configs
            for (configId in configIds) {
                h.createUpdate("DELETE FROM dispatch_event_stg WHERE config_id = :configId")
                    .bind("configId", configId)
                    .execute()
            }

            // 2. Delete orphaned stg batches (no remaining events)
            h.createUpdate("""
                DELETE FROM dispatch_batch_stg
                WHERE batch_token NOT IN (SELECT DISTINCT batch_token FROM dispatch_event_stg)
            """).execute()

            // 3. Find prod batch tokens with NORMAL status that have events for these configs
            val batchTokens = h.createQuery("""
                SELECT DISTINCT e.batch_token
                FROM dispatch_event e
                JOIN dispatch_batch b ON b.batch_token = e.batch_token
                WHERE b.status = 'NORMAL'
                  AND e.config_id IN (<configIds>)
            """)
                .bindList("configIds", configIds)
                .mapTo(String::class.java)
                .list()

            if (batchTokens.isEmpty()) {
                return@inTransactionSuspend SyncResult(configIds, 0, 0)
            }

            // 4. Upsert batch records into stg
            var batchesCopied = 0
            for (token in batchTokens) {
                val merged = h.createUpdate("""
                    MERGE INTO dispatch_batch_stg tgt
                    USING (SELECT batch_token, status, created_at, config_count
                           FROM dispatch_batch WHERE batch_token = :token) src
                    ON (tgt.batch_token = src.batch_token)
                    WHEN NOT MATCHED THEN
                        INSERT (batch_token, status, created_at, config_count)
                        VALUES (src.batch_token, src.status, src.created_at, src.config_count)
                """)
                    .bind("token", token)
                    .execute()
                batchesCopied += merged
            }

            // 5. Copy events from prod to stg for the specified configs
            val eventsCopied = h.createUpdate("""
                INSERT INTO dispatch_event_stg
                    (batch_token, config_id, dispatch_order, product_id, source_bom_id,
                     qty, target_site_id, target_bom_id, site_gap, bom_gap)
                SELECT e.batch_token, e.config_id, e.dispatch_order, e.product_id, e.source_bom_id,
                       e.qty, e.target_site_id, e.target_bom_id, e.site_gap, e.bom_gap
                FROM dispatch_event e
                JOIN dispatch_batch b ON b.batch_token = e.batch_token
                WHERE b.status = 'NORMAL'
                  AND e.config_id IN (<configIds>)
            """)
                .bindList("configIds", configIds)
                .execute()

            SyncResult(configIds, batchesCopied, eventsCopied)
        }
    }
}
```

**Note:** The `MERGE INTO` syntax works on both H2 (in Oracle mode) and Oracle. If H2 compatibility issues arise in tests, the MERGE can be replaced with a conditional INSERT check.

- [ ] **Step 2: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SyncRepositoryTest" -pl WorkFlow`
Expected: All 3 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/dispatch/adapter/persistence/SyncRepository.kt
git commit -m "feat(dispatch): implement SyncRepository for prod-to-stg event sync"
```

---

### Task 3: Add CDI producer for SyncRepository

**Files:**
- Modify: `src/main/kotlin/dispatch/adapter/DispatchProducers.kt` (or wherever it was placed in P4)

- [ ] **Step 1: Add producer method**

Add to `DispatchProducers`:

```kotlin
@Produces
@ApplicationScoped
@IfBuildProfile("stg")
fun syncRepository(jdbi: Jdbi): SyncRepository = SyncRepository(jdbi)
```

Add import: `com.workflow.dispatch.adapter.persistence.SyncRepository`, `io.quarkus.arc.profile.IfBuildProfile`.

- [ ] **Step 2: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl WorkFlow`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/dispatch/adapter/DispatchProducers.kt
git commit -m "feat(dispatch): add CDI producer for SyncRepository (stg profile)"
```

---

### Task 4: Implement sync REST endpoint

**Files:**
- Create: `src/main/kotlin/dispatch/adapter/http/DispatchSyncResource.kt`

- [ ] **Step 1: Create the resource**

Create `src/main/kotlin/dispatch/adapter/http/DispatchSyncResource.kt`:

```kotlin
package com.workflow.dispatch.adapter.http

import com.workflow.dispatch.adapter.persistence.SyncRepository
import com.workflow.dispatch.adapter.persistence.SyncResult
import io.quarkus.arc.profile.IfBuildProfile
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import kotlinx.coroutines.runBlocking

data class SyncRequest(val configIds: List<String>)
data class SyncResponse(val syncedConfigs: List<String>, val batchesCopied: Int, val eventsCopied: Int)

@Path("/dispatch")
@ApplicationScoped
@IfBuildProfile("stg")
class DispatchSyncResource(
    private val syncRepository: SyncRepository,
) {

    @POST
    @Path("/sync")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    fun sync(request: SyncRequest): SyncResponse = runBlocking {
        val result = syncRepository.syncFromProd(request.configIds)
        SyncResponse(
            syncedConfigs = result.syncedConfigs,
            batchesCopied = result.batchesCopied,
            eventsCopied = result.eventsCopied,
        )
    }
}
```

- [ ] **Step 2: Write a unit test for the resource**

Create `src/test/kotlin/dispatch/adapter/http/DispatchSyncResourceTest.kt`:

```kotlin
package com.workflow.dispatch.adapter.http

import com.workflow.dispatch.adapter.persistence.SyncRepository
import com.workflow.dispatch.adapter.persistence.SyncResult
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import kotlin.test.assertEquals

class DispatchSyncResourceTest {

    @Test
    fun `sync endpoint delegates to SyncRepository`() = runTest {
        val syncRepo = mock<SyncRepository>()
        whenever(syncRepo.syncFromProd(listOf("cfg1", "cfg2"))).thenReturn(
            SyncResult(listOf("cfg1", "cfg2"), 5, 120)
        )

        val resource = DispatchSyncResource(syncRepo)
        val response = resource.sync(SyncRequest(configIds = listOf("cfg1", "cfg2")))

        assertEquals(listOf("cfg1", "cfg2"), response.syncedConfigs)
        assertEquals(5, response.batchesCopied)
        assertEquals(120, response.eventsCopied)
    }
}
```

- [ ] **Step 3: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchSyncResourceTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/dispatch/adapter/http/DispatchSyncResource.kt src/test/kotlin/dispatch/adapter/http/DispatchSyncResourceTest.kt
git commit -m "feat(dispatch): add POST /dispatch/sync endpoint for stg profile"
```
