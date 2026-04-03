# Dispatch Env P4: Environment-Aware Storage Paths

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `DispatchPathBuilder` utility that constructs MinIO/S3 paths based on environment and batch mode. This centralizes path logic so handlers don't build paths inline.

**Architecture:** A simple value class that takes `env` (from config) and provides methods for CSV and Parquet paths. Injected into handlers via CDI. Path format: `env={env}/mode={mode}/dispatch/{batchToken}/simulation/{configId}.csv.gz` for CSV, `env={env}/dispatch/result.parquet` (prod) or `env={env}/dispatch/{batchToken}/result.parquet` (stg) for Parquet.

**Tech Stack:** Kotlin, Quarkus CDI

---

### Task 1: Write tests for DispatchPathBuilder

**Files:**
- Create: `src/test/kotlin/dispatch/adapter/storage/DispatchPathBuilderTest.kt`

- [ ] **Step 1: Write the test class**

Create `src/test/kotlin/dispatch/adapter/storage/DispatchPathBuilderTest.kt`:

```kotlin
package com.workflow.dispatch.adapter.storage

import com.workflow.dispatch.model.BatchStatus
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class DispatchPathBuilderTest {

    @Test
    fun `csvPath for prod normal`() {
        val builder = DispatchPathBuilder("prod")
        val path = builder.csvPath(BatchStatus.NORMAL, "20260403060000", "cfg1")
        assertEquals("env=prod/mode=normal/dispatch/20260403060000/simulation/cfg1.csv.gz", path)
    }

    @Test
    fun `csvPath for prod dryrun`() {
        val builder = DispatchPathBuilder("prod")
        val path = builder.csvPath(BatchStatus.DRYRUN, "abc-123", "cfg1")
        assertEquals("env=prod/mode=dryrun/dispatch/abc-123/simulation/cfg1.csv.gz", path)
    }

    @Test
    fun `csvPath for stg normal`() {
        val builder = DispatchPathBuilder("stg")
        val path = builder.csvPath(BatchStatus.NORMAL, "20260403060000", "cfg1")
        assertEquals("env=stg/mode=normal/dispatch/20260403060000/simulation/cfg1.csv.gz", path)
    }

    @Test
    fun `prodParquetPath returns fixed prod path`() {
        val builder = DispatchPathBuilder("prod")
        val path = builder.prodParquetPath()
        assertEquals("env=prod/dispatch/result.parquet", path)
    }

    @Test
    fun `batchParquetPath includes batchToken`() {
        val builder = DispatchPathBuilder("stg")
        val path = builder.batchParquetPath("20260403060000")
        assertEquals("env=stg/dispatch/20260403060000/result.parquet", path)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchPathBuilderTest" -pl WorkFlow`
Expected: FAIL — `DispatchPathBuilder` does not exist.

- [ ] **Step 3: Commit failing tests**

```bash
git add src/test/kotlin/dispatch/adapter/storage/DispatchPathBuilderTest.kt
git commit -m "test(dispatch): add DispatchPathBuilder tests"
```

---

### Task 2: Implement DispatchPathBuilder

**Files:**
- Create: `src/main/kotlin/dispatch/adapter/storage/DispatchPathBuilder.kt`

- [ ] **Step 1: Create the path builder**

Create `src/main/kotlin/dispatch/adapter/storage/DispatchPathBuilder.kt`:

```kotlin
package com.workflow.dispatch.adapter.storage

import com.workflow.dispatch.model.BatchStatus

class DispatchPathBuilder(private val env: String) {

    fun csvPath(mode: BatchStatus, batchToken: String, configId: String): String =
        "env=$env/mode=${mode.name.lowercase()}/dispatch/$batchToken/simulation/$configId.csv.gz"

    fun prodParquetPath(): String =
        "env=$env/dispatch/result.parquet"

    fun batchParquetPath(batchToken: String): String =
        "env=$env/dispatch/$batchToken/result.parquet"
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchPathBuilderTest" -pl WorkFlow`
Expected: All 5 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/dispatch/adapter/storage/DispatchPathBuilder.kt
git commit -m "feat(dispatch): implement DispatchPathBuilder for env-aware storage paths"
```

---

### Task 3: Add CDI producer for DispatchPathBuilder

**Files:**
- Modify: `src/main/kotlin/dispatch/adapter/persistence/DispatchPersistenceProducer.kt`

- [ ] **Step 1: Add producer method**

Rename the file to `DispatchProducers.kt` (since it now produces more than persistence beans) and add:

```kotlin
@Produces
@ApplicationScoped
fun dispatchPathBuilder(
    @ConfigProperty(name = "dispatch.env", defaultValue = "prod") env: String,
): DispatchPathBuilder = DispatchPathBuilder(env)
```

Add import: `com.workflow.dispatch.adapter.storage.DispatchPathBuilder`

Also rename the class from `DispatchPersistenceProducer` to `DispatchProducers` and move it to `src/main/kotlin/dispatch/adapter/DispatchProducers.kt` (parent package, since it produces both persistence and storage beans).

- [ ] **Step 2: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl WorkFlow`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add -A src/main/kotlin/dispatch/adapter/
git commit -m "feat(dispatch): add CDI producer for DispatchPathBuilder, rename to DispatchProducers"
```
