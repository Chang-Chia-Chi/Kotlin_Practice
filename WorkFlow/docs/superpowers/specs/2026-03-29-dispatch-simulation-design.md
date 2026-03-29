# Dispatch Simulation System Design

## Overview

A dispatching system built on top of the lock-free workflow engine. Users configure dispatch rules (factory targets, BOM mappings), and the system simulates product-to-factory allocation using a pluggable gap-based algorithm, then persists results for downstream consumption.

## Workflow Lifecycle

```
Leader Cron (4x/day)
  |
  v
WorkflowEngine.startWorkflow(idempotencyKey = "dispatch-{batchToken}")
  |
  v
[Scatter] query active configs -> [configId1, configId2, ...]
  |
  v
[Fan-out] N parallel tasks (one per config)
  |  each: load config -> load baseline -> load candidates -> simulate -> persist + CSV to MinIO
  |
  v
[Join] query all decisions by batchToken -> merge into parquet -> upload to MinIO
```

- **Batch token:** `LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)` (e.g., `2026-03-29T06:00:00`)
- **Deduplication:** Idempotency key `dispatch-{batchToken}` on workflow table prevents duplicate workflows within the same hour bucket
- **Leader resilience:** Cron fires multiple times per window; MERGE INTO ensures at most one workflow per batch

## Domain Model

### Config

```kotlin
enum class DispatchMode { QTY, RATIO }

data class DispatchConfig(
    val id: String,
    val mode: DispatchMode,
    val algorithmId: String,                    // "default" for now, extensible
    val siteTargets: List<SiteTarget>,          // Lv1 (required)
    val bomMappings: Map<String, BomMapping>?,  // Lv2 (optional), keyed by siteId
)

data class SiteTarget(
    val siteId: String,
    val target: BigDecimal,                     // absolute qty or percentage (0-100)
)

data class BomMapping(
    val sourceBomId: String,                    // constraint on candidate products
    val targetAllocations: List<TargetBomAllocation>,
)

data class TargetBomAllocation(
    val targetBomId: String,
    val target: BigDecimal,
)
```

**Lv1/Lv2 relationship:**
- Each site has at most one `BomMapping` (one sourceBomId, multiple targetBomIds)
- Lv1 query config uses a sourceBomId prefix; lv2 sourceBomId is a full ID containing the lv1 prefix
- The prefix is used at query time only; the algorithm operates on full sourceBomIds

### Products & Results

```kotlin
data class CandidateProduct(
    val productId: String,
    val sourceBomId: String,
    val qty: Int,                               // 1-25, dispatched atomically
)

data class DispatchDecision(
    val dispatchOrder: Int,                     // 1-based sequence
    val productId: String,
    val sourceBomId: String,
    val qty: Int,
    val targetSiteId: String,
    val targetBomId: String?,                   // null if no lv2
    val siteGap: BigDecimal,                    // gap at moment of selection (audit)
    val bomGap: BigDecimal?,                    // null if no lv2 (audit)
)

data class SimulationResult(
    val decisions: List<DispatchDecision>,
    val finalSiteAllocations: Map<String, BigDecimal>,
    val finalBomAllocations: Map<SiteBomKey, BigDecimal>,
)

data class SiteBomKey(val siteId: String, val targetBomId: String)

data class Baseline(
    val siteAllocations: Map<String, BigDecimal>,
    val bomAllocations: Map<SiteBomKey, BigDecimal>,
)
```

## Port Interfaces

User implements these; the system defines the contracts.

```kotlin
// Load active, non-expired configs
interface DispatchConfigRepository {
    suspend fun findActiveConfigs(asOf: LocalDateTime): List<DispatchConfig>
    suspend fun findById(configId: String): DispatchConfig
}

// Query candidate products for a config, in dispatch order
interface CandidateQueryPort {
    suspend fun queryCandidates(config: DispatchConfig): List<CandidateProduct>
}

// Load historical allocation state (baseline before simulation)
interface BaselineProvider {
    suspend fun loadBaseline(config: DispatchConfig): Baseline
}

// Persist simulation results to DB
interface SimulationResultStore {
    suspend fun saveDecisions(batchToken: String, configId: String, decisions: List<DispatchDecision>)
    suspend fun findByBatchToken(batchToken: String): List<DispatchDecision>
}

// Upload files to object storage (MinIO/S3)
interface StoragePort {
    suspend fun uploadCsv(path: String, content: ByteArray)
    suspend fun uploadParquet(path: String, content: ByteArray)
}

// Format decisions to CSV bytes
interface CsvFormatter {
    fun format(batchToken: String, configId: String, decisions: List<DispatchDecision>): ByteArray
}

// Format decisions to Parquet bytes
interface ParquetFormatter {
    fun format(decisions: List<DispatchDecision>): ByteArray
}
```

## Algorithm Architecture

### Core Principle

The gap-based selection logic is identical at lv1 (pick site) and lv2 (pick targetBomId). The only difference is how "gap" is computed. This shared logic is extracted into a reusable selection kernel.

### Gap Computation

- **Gap = current - target** (negative means behind target)
- Dispatch to the **lowest** gap (most negative = furthest behind)
- Tiebreakers: highest target, then sticky routing (same as previous round)

```kotlin
interface GapComputer {
    fun computeGap(current: BigDecimal, target: BigDecimal, total: BigDecimal): BigDecimal
}

// Qty: gap = current - target (total unused)
class QtyGapComputer : GapComputer

// Ratio: gap = (current / total) - targetRatio
class RatioGapComputer : GapComputer
```

### Selection Kernel

Reused at both lv1 and lv2 levels:

```kotlin
data class SelectionEntry(
    val id: String,             // siteId or targetBomId
    val gap: BigDecimal,
    val target: BigDecimal,
)

// Rule: min gap -> max target -> sticky (lastSelected)
fun selectByGap(
    entries: List<SelectionEntry>,
    lastSelected: String?,
): String?
```

### Algorithm Interface

Pure function — no mutation, no I/O:

```kotlin
interface DispatchAlgorithm {
    val candidateMatcher: CandidateMatcher
    val terminationStrategy: TerminationStrategy

    fun selectTarget(
        siteTargets: List<SiteTarget>,
        siteCurrents: Map<String, BigDecimal>,
        bomMappings: Map<String, BomMapping>?,
        bomCurrents: Map<SiteBomKey, BigDecimal>,
        lastSiteId: String?,
        lastBomId: String?,
        total: BigDecimal,
    ): TargetSelection
}

sealed interface TargetSelection {
    data class Selected(
        val siteId: String,
        val targetBomId: String?,
        val sourceBomConstraint: String?,
        val siteGap: BigDecimal,
        val bomGap: BigDecimal?,
    ) : TargetSelection
    object NoTarget : TargetSelection
}
```

### Candidate Matching

Encapsulates mode-specific candidate eligibility (e.g., qty capacity check):

```kotlin
interface CandidateMatcher {
    fun findCandidate(
        index: CandidateIndex,
        sourceBomConstraint: String?,
        context: SimulationContext,
        siteTarget: SiteTarget,
    ): Int?    // candidate index, or null
}

// Qty mode: skip if candidate.qty + currentSiteQty > targetSiteQty
class QtyCandidateMatcher : CandidateMatcher

// Ratio mode: no capacity constraint, just sourceBom match
class DefaultCandidateMatcher : CandidateMatcher
```

### Termination Strategy

```kotlin
interface TerminationStrategy {
    fun onNoCandidate(
        siteId: String,
        targetBomId: String?,
        context: SimulationContext,
    ): TerminationDecision
}

enum class TerminationDecision { STOP, SKIP_SITE }

// Default: fail-fast (simulation ends immediately)
class FailFastTermination : TerminationStrategy
```

### Algorithm Composition (Kotlin DSL)

```kotlin
// Predefined defaults
val qtyDefault = dispatchAlgorithm(QTY)
val ratioDefault = dispatchAlgorithm(RATIO)

// Customize specific parts
val custom = dispatchAlgorithm(QTY) {
    termination(SkipAndContinue)
}

// DSL builder
fun dispatchAlgorithm(
    mode: DispatchMode,
    block: AlgorithmBuilder.() -> Unit = {},
): DispatchAlgorithm

class AlgorithmBuilder(mode: DispatchMode) {
    var gapComputer: GapComputer = when (mode) {
        QTY -> QtyGapComputer()
        RATIO -> RatioGapComputer()
    }
    var candidateMatcher: CandidateMatcher = when (mode) {
        QTY -> QtyCandidateMatcher()
        RATIO -> DefaultCandidateMatcher()
    }
    var termination: TerminationStrategy = FailFastTermination()
}
```

### Algorithm Registry

CDI-based lookup, mirrors existing `HandlerRegistry` pattern:

```kotlin
interface DispatchAlgorithmFactory {
    fun create(mode: DispatchMode, algorithmId: String): DispatchAlgorithm
}
```

## Simulation Engine

### CandidateIndex (Pre-grouped, Memory-efficient)

```kotlin
class CandidateIndex(private val candidates: List<CandidateProduct>) {
    // Pre-grouped by full sourceBomId, indices in dispatch order
    private val bySourceBom: Map<String, List<Int>> =
        candidates.indices.groupBy { candidates[it].sourceBomId }

    // Flat dispatch-order list for no-lv2 sites
    private val allIndices: List<Int> = candidates.indices.toList()

    private val consumed = BitSet(candidates.size)

    fun findFirst(
        sourceBomConstraint: String?,
        predicate: (CandidateProduct) -> Boolean = { true },
    ): Int? {
        val pool = if (sourceBomConstraint != null) {
            bySourceBom[sourceBomConstraint] ?: return null
        } else {
            allIndices
        }
        return pool.firstOrNull { !consumed[it] && predicate(candidates[it]) }
    }

    fun consume(index: Int) { consumed.set(index) }
    fun hasUnconsumed(): Boolean = consumed.cardinality() < candidates.size
    operator fun get(index: Int): CandidateProduct = candidates[index]
}
```

### SimulationContext (Mutable, Engine-internal)

```kotlin
class SimulationContext(
    val candidates: List<CandidateProduct>,
    val consumed: BitSet,
    val siteCurrents: MutableMap<String, BigDecimal>,
    val bomCurrents: MutableMap<SiteBomKey, BigDecimal>,
    var lastSiteId: String? = null,
    var lastBomId: String? = null,
    val decisions: MutableList<DispatchDecision> = mutableListOf(),
    var total: BigDecimal,
)
```

### Engine Loop

```kotlin
class SimulationEngine(
    private val algorithmFactory: DispatchAlgorithmFactory,
) {
    fun simulate(
        config: DispatchConfig,
        candidates: List<CandidateProduct>,
        baseline: Baseline,
    ): SimulationResult {
        val algorithm = algorithmFactory.create(config.mode, config.algorithmId)
        val index = CandidateIndex(candidates)
        val context = SimulationContext(
            candidates = candidates,
            consumed = BitSet(candidates.size),
            siteCurrents = baseline.siteAllocations.toMutableMap(),
            bomCurrents = baseline.bomAllocations.toMutableMap(),
            total = baseline.siteAllocations.values.fold(BigDecimal.ZERO, BigDecimal::add),
        )

        while (index.hasUnconsumed()) {
            // 1. Pure algorithm selects target
            val selection = algorithm.selectTarget(
                config.siteTargets, context.siteCurrents,
                config.bomMappings, context.bomCurrents,
                context.lastSiteId, context.lastBomId, context.total,
            )
            if (selection is NoTarget) break

            val selected = selection as Selected

            // 2. Find matching candidate via mode-specific matcher
            val siteTarget = config.siteTargets.first { it.siteId == selected.siteId }
            val idx = algorithm.candidateMatcher.findCandidate(
                index, selected.sourceBomConstraint, context, siteTarget,
            )

            // 3. Termination check
            if (idx == null) {
                val decision = algorithm.terminationStrategy
                    .onNoCandidate(selected.siteId, selected.targetBomId, context)
                if (decision == STOP) break
                continue
            }

            // 4. Dispatch — mutate context
            val candidate = index[idx]
            val qty = candidate.qty.toBigDecimal()

            index.consume(idx)
            context.siteCurrents.merge(selected.siteId, qty, BigDecimal::add)
            if (selected.targetBomId != null) {
                context.bomCurrents.merge(
                    SiteBomKey(selected.siteId, selected.targetBomId), qty, BigDecimal::add,
                )
            }
            context.total += qty
            context.lastSiteId = selected.siteId
            context.lastBomId = selected.targetBomId

            context.decisions += DispatchDecision(
                dispatchOrder = context.decisions.size + 1,
                productId = candidate.productId,
                sourceBomId = candidate.sourceBomId,
                qty = candidate.qty,
                targetSiteId = selected.siteId,
                targetBomId = selected.targetBomId,
                siteGap = selected.siteGap,
                bomGap = selected.bomGap,
            )
        }

        return SimulationResult(
            decisions = context.decisions.toList(),
            finalSiteAllocations = context.siteCurrents.toMap(),
            finalBomAllocations = context.bomCurrents.toMap(),
        )
    }
}
```

**Complexity:** O(candidates * sites) worst case. Pre-grouped index avoids full scans when lv2 constrains by sourceBomId.

## Workflow Integration

### Workflow Definition

```kotlin
val dispatchWorkflow = workflow {
    deadline(Duration.ofHours(2))

    activity("scatter") {
        transition("dispatch.scatter")
        fanOut("simulate") {
            transition("dispatch.simulate")
            retries(2)
            deadline(Duration.ofMinutes(30))
            joinPolicy(JoinPolicy.All)
        }
    }

    activity("join") {
        transition("dispatch.join")
        deadline(Duration.ofMinutes(10))
        inputs {
            "batchToken" from "scatter.batchToken"
        }
    }
}
```

### Handler: Scatter

```kotlin
@ApplicationScoped
class DispatchScatterHandler(
    private val configRepo: DispatchConfigRepository,
) : TransitionHandler {
    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val now = LocalDateTime.now()
        val batchToken = now.truncatedTo(ChronoUnit.HOURS)
            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

        val configs = configRepo.findActiveConfigs(now)
        val items = configs.map { mapOf("configId" to it.id, "batchToken" to batchToken) }

        return HandlerOutput(objectMapper.writeValueAsString(items))
    }
}
```

### Handler: Simulation (per config, parallel)

```kotlin
@ApplicationScoped
class DispatchSimulationHandler(
    private val configRepo: DispatchConfigRepository,
    private val candidateQuery: CandidateQueryPort,
    private val baselineProvider: BaselineProvider,
    private val simulationEngine: SimulationEngine,
    private val resultStore: SimulationResultStore,
    private val storage: StoragePort,
    private val csvFormatter: CsvFormatter,
) : TransitionHandler {
    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val item = objectMapper.readTree(input.item!!)
        val configId = item["configId"].asText()
        val batchToken = item["batchToken"].asText()

        val config = configRepo.findById(configId)
        val candidates = candidateQuery.queryCandidates(config)
        val baseline = baselineProvider.loadBaseline(config)

        val result = simulationEngine.simulate(config, candidates, baseline)

        resultStore.saveDecisions(batchToken, configId, result.decisions)

        val csv = csvFormatter.format(batchToken, configId, result.decisions)
        storage.uploadCsv("dispatch/$batchToken/simulation/$configId.csv", csv)

        return HandlerOutput(objectMapper.writeValueAsString(
            mapOf("configId" to configId, "batchToken" to batchToken),
        ))
    }
}
```

### Handler: Join

```kotlin
@ApplicationScoped
class DispatchJoinHandler(
    private val resultStore: SimulationResultStore,
    private val storage: StoragePort,
    private val parquetFormatter: ParquetFormatter,
) : TransitionHandler {
    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val batchToken = objectMapper.readTree(input.inputs!!)["batchToken"].asText()

        val allDecisions = resultStore.findByBatchToken(batchToken)
        val parquet = parquetFormatter.format(allDecisions)
        storage.uploadParquet("dispatch/$batchToken/result.parquet", parquet)

        return HandlerOutput(null)
    }
}
```

### Leader Cronjob

```kotlin
@ApplicationScoped
class DispatchScheduler(
    private val workflowEngine: WorkflowEngine,
) {
    @Scheduled(cron = "{dispatch.cron}")
    fun trigger() {
        val batchToken = LocalDateTime.now()
            .truncatedTo(ChronoUnit.HOURS)
            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

        workflowEngine.startWorkflow(
            definition = dispatchWorkflow,
            idempotencyKey = "dispatch-$batchToken",
        )
    }
}
```

### Framework Enhancement: Idempotency Key

Add to `WorkflowEngine.startWorkflow`:

```kotlin
fun startWorkflow(
    definition: WorkflowDefinition,
    idempotencyKey: String? = null,
): StartResult

sealed interface StartResult {
    data class Created(val workflowId: String) : StartResult
    data class AlreadyExists(val workflowId: String) : StartResult
}
```

Schema:

```sql
ALTER TABLE workflow ADD idempotency_key VARCHAR2(255) NULL;
CREATE UNIQUE INDEX uk_workflow_idem ON workflow (idempotency_key);
```

MERGE INTO:

```sql
MERGE INTO workflow w
USING (SELECT :key AS idem_key FROM dual) src
ON (w.idempotency_key = src.idem_key)
WHEN NOT MATCHED THEN INSERT (id, idempotency_key, definition, current_sequence, version, status, ...)
VALUES (SYS_GUID(), :key, :def, 1, 0, 'RUNNING', ...)
```

## MinIO/S3 Adapter (Best Practice)

Use AWS SDK v2 `S3AsyncClient` with `CompletableFuture.await()` for coroutine integration:

```kotlin
@ApplicationScoped
class S3StorageAdapter(
    private val client: S3AsyncClient,
) : StoragePort {
    override suspend fun uploadCsv(path: String, content: ByteArray) {
        upload(path, content, "text/csv")
    }

    override suspend fun uploadParquet(path: String, content: ByteArray) {
        upload(path, content, "application/octet-stream")
    }

    private suspend fun upload(key: String, content: ByteArray, contentType: String) {
        client.putObject(
            PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType(contentType)
                .build(),
            AsyncRequestBody.fromBytes(content),
        ).await()   // non-blocking via kotlinx-coroutines
    }
}
```

S3AsyncClient producer:

```kotlin
@ApplicationScoped
class S3ClientProducer {
    @Produces @ApplicationScoped
    fun s3AsyncClient(
        @ConfigProperty(name = "storage.endpoint") endpoint: String,
        @ConfigProperty(name = "storage.region") region: String,
        @ConfigProperty(name = "storage.access-key") accessKey: String,
        @ConfigProperty(name = "storage.secret-key") secretKey: String,
    ): S3AsyncClient = S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.of(region))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
        )
        .forcePathStyleAccess(true)    // Required for MinIO
        .build()
}
```

Maven dependency:

```xml
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>s3</artifactId>
    <version>2.29.x</version>
</dependency>
```

## CSV Simulation Result Format

File path: `dispatch/{batchToken}/simulation/{configId}.csv`

| Column | Type | Description |
|--------|------|-------------|
| `batch_token` | String | Batch identifier |
| `config_id` | String | Config that produced this decision |
| `dispatch_order` | Int | Sequence in algorithm (1-based) |
| `product_id` | String | Product dispatched |
| `source_bom_id` | String | Product's original BOM |
| `qty` | Int | Product quantity (1-25) |
| `target_site_id` | String | Factory assigned by lv1 |
| `target_bom_id` | String | BOM assigned by lv2 (empty if no lv2) |
| `site_gap` | BigDecimal | Site gap at selection time |
| `bom_gap` | BigDecimal | BOM gap at selection time (empty if no lv2) |

## Package Layout

```
src/main/kotlin/
  dispatch/
    model/          DispatchConfig, CandidateProduct, DispatchDecision,
                    SimulationResult, Baseline, SiteBomKey
    algorithm/      GapComputer, QtyGapComputer, RatioGapComputer,
                    CandidateMatcher, QtyCandidateMatcher, DefaultCandidateMatcher,
                    TerminationStrategy, FailFastTermination,
                    DispatchAlgorithm, DefaultDispatchAlgorithm,
                    DispatchAlgorithmFactory, AlgorithmBuilder (DSL)
    simulation/     SimulationEngine, SimulationContext, CandidateIndex
    port/           DispatchConfigRepository, CandidateQueryPort, BaselineProvider,
                    SimulationResultStore, StoragePort, CsvFormatter, ParquetFormatter
    handler/        DispatchScatterHandler, DispatchSimulationHandler,
                    DispatchJoinHandler, DispatchScheduler
```
