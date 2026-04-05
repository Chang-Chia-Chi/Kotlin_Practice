# Dispatch E2E Integration Test Design

## Overview

End-to-end integration test for the dispatch pipeline covering the full lifecycle:
trigger → worker loop claim (K8s integrated) → worker handle → simulation →
CSV upload → final Parquet join result upload using DuckDB → shutdown.

Includes two new production components (`S3StorageGateway`, `DuckDbParquetFormatter`)
and a three-class test suite (`DuckDbParquetFormatterTest`, `DispatchE2EHappyPathTest`,
`DispatchE2EShutdownTest`).

## Decisions

| Aspect | Decision | Rationale |
|---|---|---|
| K8s | Fabric8 `KubernetesMockServer` | Tests real Watch API protocol without cluster overhead |
| Storage | MinIO container (S3-compatible) | Real S3 protocol, verifiable artifacts |
| Parquet | DuckDB-based `ParquetFormatter` (new) | Production dependency, fresh connection per invocation |
| Test harness | `@QuarkusTest` with real CDI | Tests production wiring, lifecycle hooks, shutdown |
| Fixture data | Representative JSON fixture file | 2-3 configs, at least one with fan-out |
| Entry point | Programmatic workflow creation | Focuses on worker pipeline, bypasses REST |
| Shutdown | Clean drain + mid-flight resilience | Two separate test classes |
| DB | Existing `OracleTestContainer` singleton | Shared Oracle Free container |

---

## 1. New Production Code

### 1.1 `S3StorageGateway`

**File:** `src/main/kotlin/dispatch/adapter/storage/S3StorageGateway.kt`

Implements `StorageGateway` using the AWS S3 SDK (already in POM).

```kotlin
@ApplicationScoped
class S3StorageGateway(
    private val s3Client: S3Client,
    @ConfigProperty(name = "dispatch.storage.bucket") private val bucket: String,
) : StorageGateway {

    override suspend fun uploadCsv(path: String, file: File) {
        // PutObject with content-type application/gzip
    }

    override suspend fun uploadParquet(path: String, content: ByteArray) {
        // PutObject with content-type application/octet-stream
    }
}
```

**CDI wiring:** The `S3Client` bean should be produced in `DispatchProducers` (or a
dedicated `StorageProducers`), reading endpoint URL, region, and credentials from
`application.properties`. This allows the test to override the endpoint to MinIO.

### 1.2 `DuckDbParquetFormatter`

**File:** `src/main/kotlin/dispatch/adapter/storage/DuckDbParquetFormatter.kt`

Replaces `NoOpParquetFormatter`. Implements `ParquetFormatter`.

```kotlin
@ApplicationScoped
class DuckDbParquetFormatter : ParquetFormatter {

    override fun format(decisions: List<DispatchDecision>): ByteArray {
        // 1. Create fresh DuckDB in-memory connection (no pooling — avoids memory leak)
        // 2. CREATE TABLE dispatch_decision (...)
        // 3. INSERT rows from decisions list
        // 4. COPY dispatch_decision TO '/tmp/result.parquet' (FORMAT PARQUET)
        // 5. Read bytes from temp file
        // 6. Close connection, delete temp file
        // 7. Return bytes
    }
}
```

**Key constraints:**
- Fresh `DriverManager.getConnection("jdbc:duckdb:")` per invocation
- Connection closed in `finally` block
- Temp file cleaned up in `finally` block
- Column schema: `dispatch_order INT, product_id VARCHAR, source_bom_id VARCHAR,
  qty INT, target_site_id VARCHAR, target_bom_id VARCHAR, site_gap DECIMAL,
  bom_gap DECIMAL`

---

## 2. Test Infrastructure

### 2.1 `MinioTestContainer`

**File:** `src/test/kotlin/infrastructure/storage/MinioTestContainer.kt`

Singleton object (same pattern as `OracleTestContainer`):

```kotlin
object MinioTestContainer {
    private val container = GenericContainer("minio/minio:latest")
        .withCommand("server /data")
        .withExposedPorts(9000)
        .withEnv("MINIO_ROOT_USER", "minioadmin")
        .withEnv("MINIO_ROOT_PASSWORD", "minioadmin")

    val s3Client: S3Client by lazy {
        container.start()
        // Build S3Client pointing at container endpoint
        // Create test bucket "dispatch-test"
    }

    val endpoint: String get() = "http://${container.host}:${container.getMappedPort(9000)}"
    const val BUCKET = "dispatch-test"
}
```

### 2.2 Fabric8 `KubernetesMockServer`

Used per-test-class (not singleton) because each test needs to control Job status
independently.

```kotlin
private lateinit var mockServer: KubernetesMockServer
private lateinit var k8sClient: KubernetesClient

@BeforeEach
fun setupK8s() {
    mockServer = KubernetesMockServer(/* CRUD mode = true for stateful behavior */)
    mockServer.start()
    k8sClient = mockServer.createClient()
}

@AfterEach
fun teardownK8s() {
    k8sClient.close()
    mockServer.shutdown()
}
```

The test controls Job lifecycle by pushing status updates to the mock server:
- Create Job in "Running" state
- Push "Complete" condition when the test is ready
- Push ConfigMap `{jobName}-output` with result JSON

### 2.3 Quarkus Test Wiring

For `@QuarkusTest`, CDI bean overrides:

- **`StorageGateway`**: Test `@Alternative` producer that returns `S3StorageGateway`
  pointing at MinIO container endpoint
- **`KubernetesClient`**: `@InjectMock` replaced with mock server's client
- **`ParquetFormatter`**: No override needed — `DuckDbParquetFormatter` replaces
  `NoOpParquetFormatter` as the default bean
- **Dispatch repositories** (`DispatchConfigRepository`, `CandidateRepository`,
  `BaselineProvider`): Either real JDBI-backed implementations using OracleTestContainer,
  or `@InjectMock` stubs that return fixture data (depending on whether fixture data
  is loaded into Oracle or served from memory)

### 2.4 Fixture Data

**File:** `src/test/resources/fixtures/dispatch-e2e-fixture.json`

Contains a representative dataset:
- 2-3 `DispatchConfig` entries:
  - Config A: QTY mode, 3 site targets, BOM mappings (exercises fan-out with
    multiple candidates per site)
  - Config B: RATIO mode, 2 site targets, no BOM mappings (simpler path)
  - Config C (optional): edge case config
- Corresponding `CandidateProduct` lists per config (5-10 candidates each)
- `Baseline` allocations per config

At least one config must produce a meaningful fan-out during scatter (multiple
configIds → parallel simulation tasks).

---

## 3. Test Suite

### 3.1 `DuckDbParquetFormatterTest`

**File:** `src/test/kotlin/dispatch/adapter/storage/DuckDbParquetFormatterTest.kt`

Pure unit test, no containers. Uses `runTest`.

| Test Case | Description |
|---|---|
| `testFormatProducesValidParquet` | 5-10 decisions with mix of nullable/non-null bomGap → format → read back via fresh DuckDB → assert row count + values match |
| `testEmptyDecisionList` | Empty list → valid Parquet with 0 rows |
| `testFreshConnectionPerInvocation` | Call format() twice with different data → both correct, no state leakage |
| `testColumnSchema` | Format decisions → read back Parquet metadata → assert column names and types match spec |

### 3.2 `DispatchE2EHappyPathTest`

**File:** `src/test/kotlin/dispatch/DispatchE2EHappyPathTest.kt`

`@QuarkusTest` with full CDI.

#### Setup (`@BeforeEach`)
1. Clean Oracle tables (workflow_instance, workflow_task, dispatch_batch, dispatch_event)
2. Clean MinIO bucket (delete all objects)
3. Load fixture data into Oracle (configs, candidates, baselines)
4. Reset K8s mock server

#### Test Flow

```
Step 1: Create workflow
    engine.start(dispatchWorkflow, input={})

Step 2: Await scatter completion
    Awaitility: scatter task claimed by WorkerLoop → DispatchScatterHandler
    → creates batch in Oracle, returns fan-out items (2-3 configIds)

Step 3: Await simulation tasks claimed and DEFERRED
    WorkerLoop claims N simulation tasks (parallel per fan-out)
    Each DispatchSimulationHandler:
      - Runs SimulationEngine with fixture data
      - Saves decisions to Oracle (dispatch_event table)
      - Formats CSV, GZIPs, uploads to MinIO
      - Returns HandlerResult.Defer(K8S_JOB, meta={jobName, namespace})
    WorkerLoop calls taskRepo.defer() → tasks become DEFERRED

Step 4: Trigger loop settles via K8s mock
    TriggerLoop sweep picks up DEFERRED tasks
    K8sJobTriggerDriver.start() → creates Watch on mock server
    Test pushes Job "Complete" condition to mock server for each Job
    Test pushes ConfigMap "{jobName}-output" with result JSON
    K8sJobTriggerDriver.poll() → TriggerResult.Succeeded per task
    TriggerLoop settles tasks as COMPLETED

Step 5: Await join completion
    WorkerLoop claims join task → DispatchJoinHandler
      - Reads all decisions from Oracle via resultStore.findByBatchToken()
      - DuckDbParquetFormatter converts to Parquet via fresh DuckDB connection
      - Uploads result.parquet to MinIO

Step 6: Await workflow COMPLETED
    Awaitility: workflow status = COMPLETED
```

#### Assertions

```
a. Oracle state:
   - All tasks: status = COMPLETED
   - Workflow: status = COMPLETED
   - dispatch_batch: exists with correct config count
   - dispatch_event: row count matches total decisions across configs

b. MinIO CSV artifacts:
   - N .csv.gz objects exist at expected paths
   - Decompress each → verify CSV row count matches decisions for that config

c. MinIO Parquet artifact:
   - result.parquet exists at expected path
   - Non-empty content

d. Parquet content verification:
   - Read result.parquet back via fresh DuckDB connection
   - SELECT COUNT(*) matches total decisions across all configs
   - Column schema matches: dispatch_order, product_id, source_bom_id,
     qty, target_site_id, target_bom_id, site_gap, bom_gap
```

### 3.3 `DispatchE2EShutdownTest`

**File:** `src/test/kotlin/dispatch/DispatchE2EShutdownTest.kt`

`@QuarkusTest` with full CDI.

#### Setup
Same as happy path test.

#### Test Flow

```
Step 1-3: Same as happy path (create workflow → scatter → simulation → DEFERRED)

Step 4: Partial K8s completion
    Push "Complete" for 1 of N K8s Jobs only
    Await: that 1 task settles as COMPLETED

Step 5: Trigger shutdown mid-flight
    Fire ShutdownEvent programmatically via CDI Event<ShutdownEvent>
    While remaining Jobs are still "Running" (tasks still DEFERRED)

Step 6: Await shutdown completes within timeout
```

#### Assertions — Preserved State

```
a. Completed Job's task:
   - Status = COMPLETED in Oracle
   - CSV uploaded to MinIO

b. Remaining DEFERRED tasks:
   - Still DEFERRED in Oracle (not lost, not orphaned as PROCESSING)

c. TriggerLoop cleanup:
   - driver.close() was called (Watches cleaned up)

d. No orphaned PROCESSING tasks in Oracle

e. Join task:
   - Never started (blocked by incomplete fan-out, JoinPolicy.All)
```

#### Recovery Simulation

```
Step 7: Re-start WorkerLoop + TriggerLoop

Step 8: Push remaining K8s Jobs to "Complete" on mock server

Step 9: Await remaining tasks settle → join completes → workflow COMPLETED

Step 10: Verify final Parquet in MinIO
    - result.parquet exists, correct row count via DuckDB read-back
```

---

## 4. Dependencies

### New Maven Dependencies

| Dependency | Scope | Purpose |
|---|---|---|
| `org.duckdb:duckdb_jdbc` | compile | DuckDB JDBC driver for ParquetFormatter |
| `io.fabric8:kubernetes-server-mock` | test | Fabric8 K8s mock server |
| `org.testcontainers:minio` (or generic) | test | MinIO container |

### Existing Dependencies (already in POM)

- `software.amazon.awssdk:s3` — S3 client for StorageGateway
- `org.testcontainers:oracle-free` — Oracle container
- `io.fabric8:kubernetes-client` — K8s client

---

## 5. Configuration

### Production (`application.properties`)

```properties
dispatch.storage.bucket=dispatch-bucket
dispatch.storage.endpoint=https://s3.amazonaws.com
dispatch.storage.region=us-west-2
```

### Test (`src/test/resources/application.properties`)

```properties
dispatch.storage.bucket=dispatch-test
dispatch.storage.endpoint=${MINIO_ENDPOINT}
dispatch.storage.region=us-east-1
```

The MinIO endpoint is injected at test startup via `@QuarkusTestResource` or
system property override.

---

## 6. Handler Modification for K8s Trigger Path

Currently `DispatchSimulationHandler` returns `HandlerResult.Completed(...)` after
simulation. To exercise the K8s trigger path, the handler needs to return
`HandlerResult.Defer(K8S_JOB, triggerMeta)` instead.

**Design decision:** This should be a **production code change**, not a test hack.
The simulation handler should support a configurable mode where, after completing
the simulation and uploading CSV, it submits a K8s Job for post-processing and
defers to the trigger loop. The K8s Job name and namespace are derived from the
batch token and config ID.

The test exercises this path by having the K8s mock server simulate Job completion,
which causes the trigger loop to settle the task.

If the K8s defer path is not yet needed in production, an alternative is to use a
**test-specific handler** registered with the same key that wraps the real handler
and returns `Defer` instead of `Completed`. This avoids modifying production code
for test purposes. The choice depends on whether K8s Job dispatch is a production
requirement.

---

## 7. File Summary

| Component | Type | Path |
|---|---|---|
| `S3StorageGateway` | Production adapter | `src/main/kotlin/dispatch/adapter/storage/S3StorageGateway.kt` |
| `DuckDbParquetFormatter` | Production adapter | `src/main/kotlin/dispatch/adapter/storage/DuckDbParquetFormatter.kt` |
| `MinioTestContainer` | Test infra | `src/test/kotlin/infrastructure/storage/MinioTestContainer.kt` |
| `DuckDbParquetFormatterTest` | Unit test | `src/test/kotlin/dispatch/adapter/storage/DuckDbParquetFormatterTest.kt` |
| `DispatchE2EHappyPathTest` | Integration test | `src/test/kotlin/dispatch/DispatchE2EHappyPathTest.kt` |
| `DispatchE2EShutdownTest` | Integration test | `src/test/kotlin/dispatch/DispatchE2EShutdownTest.kt` |
| `dispatch-e2e-fixture.json` | Test fixture | `src/test/resources/fixtures/dispatch-e2e-fixture.json` |
