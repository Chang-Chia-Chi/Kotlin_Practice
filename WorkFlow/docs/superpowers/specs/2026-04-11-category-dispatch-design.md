# Category-Based Dispatch Scheduling

**Date:** 2026-04-11
**Scope:** `dispatch/model/`, `dispatch/usecase/port/outbound/persistence/DispatchConfigRepository.kt`, `dispatch/usecase/service/handler/DispatchScheduler.kt`, `dispatch/usecase/service/handler/DispatchScatterHandler.kt`, `application.properties`

---

## Problem

`DispatchScheduler` fires a single `@Scheduled(cron = "{dispatch.cron}")` that dispatches **all** active configs on one schedule. The business need is:

- Different categories of dispatch configs must run on different crontabs (e.g. one category twice daily, another four times daily).
- A single scheduled trigger must be able to cover one category, several categories, or all categories at once.
- Achieve this with **zero changes** to the workflow engine, worker, DSL, or any public interface outside `dispatch/`.

---

## Approach

Category is a new domain attribute on `DispatchConfig`. Each Quarkus `@Scheduled` method passes a **set** of categories through the already-existing `initialItem` channel on `WorkflowLifecycle.startWorkflow`. The scatter handler — which is the first activity in `dispatchWorkflow` and already consumes `taskPayload` for the dry-run branch — reads the set out of the payload and narrows the `findActiveConfigs` query.

- **Zero changes** to `WorkflowLifecycle`, `TransitionHandler`, `WorkflowDefinition`, any DSL, or any code under `workflow/` or `worker/`.
- **One shared** `dispatchWorkflow` definition, **one shared** `DispatchScatterHandler`, **one shared** downstream DAG.
- Only the cron **entry points** (new scheduler methods) and the **query predicate** (new repository parameter) differ per category.
- An empty category set means "no filter → all active configs". Operators own the crontab times and are responsible for avoiding double-dispatch by not overlapping a per-category schedule with an all-categories schedule.

---

## Data Flow

```
Quarkus @Scheduled (cron=dispatch.cron.urgent)
  └─▶ DispatchScheduler.triggerUrgent()
      └─▶ trigger(setOf(URGENT))
          └─▶ workflowEngine.startWorkflow(
                  definition     = dispatchWorkflow,
                  idempotencyKey = "dispatch-URGENT-$batchToken",
                  initialItem    = """{"categories":["URGENT"]}"""
              )
              └─▶ engine creates WorkflowRun + first task (taskPayload = initialItem)
                  └─▶ worker picks up task
                      └─▶ DispatchScatterHandler.execute(input)
                          ├─ dry-run branch     (payload has batchToken + configIds — unchanged)
                          └─ cron-trigger branch
                              ├─ categories = parse from input.taskPayload (empty set if missing)
                              └─ configRepo.findActiveConfigs(now, categories)
```

Idempotency-key shape:

| Categories argument | Key shape |
|---|---|
| `setOf(URGENT)` | `dispatch-URGENT-<token>` |
| `setOf(URGENT, NORMAL)` | `dispatch-NORMAL-URGENT-<token>` (sorted) |
| `emptySet()` | `dispatch-ALL-<token>` |

Sorted join guarantees two schedulers passing the same set produce the same key regardless of insertion order, so idempotency still protects against cron misfires.

---

## Components

### 1. `DispatchCategory` — new enum

```kotlin
// src/main/kotlin/dispatch/model/DispatchCategory.kt
package com.workflow.dispatch.model

enum class DispatchCategory { URGENT, NORMAL, BACKGROUND }
```

Rationale for an enum over a `String`:

- Compile-time safety at the two places the category is produced (scheduler) and consumed (handler filter path).
- Enum constants give us a finite, reviewable set — aligned with the decision that adding a new category is a code change, not a runtime change.
- JSON serialization still uses `.name` / `valueOf(...)`; the payload wire format remains textual.

Actual enum values will be finalized against real business categories during implementation; the three above are placeholders.

### 2. `DispatchConfig` — add `category` field

```kotlin
data class DispatchConfig(
    val id: String,
    val category: DispatchCategory,   // NEW
    val mode: DispatchMode,
    val algorithmId: String,
    val sourceBomPrefix: String,
    val siteTargets: List<SiteTarget>,
    val bomMappings: Map<String, BomMapping>?,
)
```

Non-nullable. External config sources that supply `DispatchConfig` instances must populate the field. Backfill strategy for legacy rows is owned by whoever manages the source system (see Migration section below).

### 3. `DispatchConfigRepository` — filter by category set

```kotlin
interface DispatchConfigRepository {
    suspend fun findActiveConfigs(
        asOf: LocalDateTime,
        categories: Set<DispatchCategory> = emptySet(),  // empty = no filter, all active
    ): List<DispatchConfig>

    suspend fun findById(configId: String): DispatchConfig
}
```

**Contract:** `categories = emptySet()` means "no category predicate" — return all active configs. A non-empty set narrows to the given categories only (SQL-equivalent `AND category IN (...)`).

The default argument value keeps every existing call site (`findActiveConfigs(now)`) source-compatible. Tests and the dry-run endpoint that do not care about category continue to compile unchanged.

### 4. `DispatchScheduler` — N methods, one private helper

```kotlin
@ApplicationScoped
class DispatchScheduler(
    private val workflowEngine: WorkflowEngine,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(DispatchScheduler::class.java)

    @Blocking
    @Scheduled(cron = "{dispatch.cron.urgent}", skipExecutionIf = NotLeader::class)
    fun triggerUrgent() = runBlocking { trigger(setOf(DispatchCategory.URGENT)) }

    @Blocking
    @Scheduled(cron = "{dispatch.cron.normal}", skipExecutionIf = NotLeader::class)
    fun triggerNormal() = runBlocking { trigger(setOf(DispatchCategory.NORMAL)) }

    @Blocking
    @Scheduled(cron = "{dispatch.cron.background}", skipExecutionIf = NotLeader::class)
    fun triggerBackground() = runBlocking { trigger(setOf(DispatchCategory.BACKGROUND)) }

    // Optional combined or all-categories entry points follow the same shape.
    // Operators add them when a single trigger should cover several categories at once:
    //
    // @Scheduled(cron = "{dispatch.cron.urgent-and-normal}", skipExecutionIf = NotLeader::class)
    // fun triggerUrgentAndNormal() = runBlocking {
    //     trigger(setOf(DispatchCategory.URGENT, DispatchCategory.NORMAL))
    // }
    //
    // @Scheduled(cron = "{dispatch.cron.all}", skipExecutionIf = NotLeader::class)
    // fun triggerAll() = runBlocking { trigger(emptySet()) }

    private suspend fun trigger(categories: Set<DispatchCategory>) {
        val batchToken = currentBatchToken()
        val keyCats = if (categories.isEmpty()) "ALL"
                      else categories.map { it.name }.sorted().joinToString("-")
        val payload = objectMapper.writeValueAsString(
            mapOf("categories" to categories.map { it.name }.sorted())
        )
        val result = workflowEngine.startWorkflow(
            definition     = dispatchWorkflow,
            idempotencyKey = "dispatch-$keyCats-$batchToken",
            initialItem    = payload,
        )
        log.info("Dispatch trigger: categories={}, batchToken={}, result={}",
                 keyCats, batchToken, result)
    }
}
```

**Why one hardcoded method per category:** Quarkus processes `@Scheduled` at build time, so the cron expression must be a constant property placeholder. A runtime loop over the enum is not possible without switching to programmatic `Scheduler` injection, which was an explicitly rejected alternative (see Alternatives section).

Adding a new category later requires: one new enum constant, one new scheduler method, one new property key, one upstream schema update at the config source. Deliberately small and reviewable.

### 5. `application.properties` — per-category crontabs

```properties
# Replaces the previous single `dispatch.cron`
dispatch.cron.urgent=0 */5 * * * ?
dispatch.cron.normal=0 0 * * * ?
dispatch.cron.background=0 0 2 * * ?
```

Actual cron expressions are placeholders. Operators configure the real schedules; they are responsible for not overlapping a per-category schedule with a combined/all schedule.

### 6. `DispatchScatterHandler` — read categories from payload

```kotlin
override suspend fun execute(input: HandlerInput): HandlerResult {
    val itemNode = input.taskPayload?.let { objectMapper.readTree(it) }
    val providedToken = itemNode?.get("batchToken")?.takeIf { !it.isNull }?.asText()
    val configIdsNode = itemNode?.get("configIds")?.takeIf { it.isArray }

    val (items, token) = if (providedToken != null && configIdsNode != null) {
        handleDryRun(configIdsNode, providedToken)                              // unchanged
    } else {
        val categories = itemNode?.get("categories")
            ?.takeIf { it.isArray }
            ?.map { DispatchCategory.valueOf(it.asText()) }
            ?.toSet()
            ?: emptySet()
        handleCronTrigger(categories)
    }
    return HandlerResult(
        result = objectMapper.writeValueAsString(mapOf("batchToken" to token)),
        fanOutPayloads = items,
    )
}

private suspend fun handleCronTrigger(
    categories: Set<DispatchCategory>,
): Pair<List<String>, String> {
    val token = clock.generate()
    val configs = configRepo.findActiveConfigs(LocalDateTime.now(), categories)
    resultStore.createBatch(token, BatchStatus.NORMAL, configs.size)
    return toItems(configs) to token
}
```

The dry-run branch is untouched. It still selects on `batchToken + configIds` and never consults `categories` — dry-run callers already know which configs they want.

---

## Error Handling & Edge Cases

| Case | Behavior |
|---|---|
| `taskPayload` has neither `{batchToken,configIds}` nor `categories` | Cron-trigger branch with `emptySet()` → all active configs. Equivalent to the previous "all" behavior. Not an error. |
| `taskPayload` has `categories=["URGENT"]` | Filter by `URGENT` only. |
| `taskPayload` has `categories=["URGENT","NORMAL"]` | Filter by `URGENT OR NORMAL`. |
| `taskPayload` has `categories=[]` (explicit empty array) | Same as missing — empty set, no filter. |
| A category string doesn't match any enum constant | `IllegalArgumentException` from `valueOf` — crash loudly. This is invalid input, not a runtime condition. |
| Two categories fire on the same minute | Succeed independently via distinct idempotency keys: `dispatch-URGENT-<token>`, `dispatch-NORMAL-<token>`. |
| Same category fires twice in the same batch window (cron misfire, leader flapping) | Prevented by idempotency key uniqueness — second call returns `StartResult.AlreadyExists`, logged and dropped. Existing behavior, unchanged. |
| `findActiveConfigs` returns empty list (any path) | `createBatch(token, NORMAL, 0)`, empty fan-out. Join handler already handles empty fan-out. Unchanged. |
| Dry-run request omits `categories` | Works — the dry-run branch does not consult `categories` at all. |

**Double-dispatch note.** If a "all categories" schedule runs concurrently with a per-category schedule, configs in the named categories will be dispatched twice per overlapping window. This is the operator's responsibility to avoid by choosing non-overlapping crontab times. The engine does not try to detect or prevent overlap.

---

## Migration

`DispatchConfigRepository` is a port with **no Jdbi implementation in this repository**. Production wiring is supplied by an external system (see `dispatch/DispatchE2EMockBeans.kt` for the comment trail). Therefore:

- **In-repo scope:** add `category` to `DispatchConfig`, update the repository interface signature, update all in-repo callers and tests.
- **Out-of-repo scope (owned by the external source system):** backfill existing rows to a default category (proposal: `NORMAL`), add the column with a `NOT NULL` constraint, add an index on `(category, <existing active-window columns>)` for the filtered query.

The external schema change must land before the new scheduler methods activate in production, otherwise `findActiveConfigs(now, {URGENT})` will query a nonexistent column. Coordination is a deploy-time concern; no in-repo code protects against this ordering.

---

## Testing

| Layer | Test | Asserts |
|---|---|---|
| Handler | `DispatchScatterHandlerTest — cron trigger with single category filters repo call` | Payload `{"categories":["URGENT"]}` → `findActiveConfigs(any, setOf(URGENT))`. |
| Handler | `cron trigger with multiple categories passes full set` | Payload `{"categories":["URGENT","NORMAL"]}` → `findActiveConfigs(any, setOf(URGENT, NORMAL))`. |
| Handler | `cron trigger with missing categories passes empty set` | Payloads `null`, `{}`, `{"categories":[]}` → `findActiveConfigs(any, emptySet())`. |
| Handler | `cron trigger with unknown enum value throws IllegalArgumentException` | Payload `{"categories":["BOGUS"]}` → `IllegalArgumentException`. |
| Handler | `dry-run path still works and ignores categories` | Payload `{"batchToken":"T1","configIds":["c1"]}` → dry-run branch, `findById` only, `findActiveConfigs` never called. Existing test updated to confirm. |
| Scheduler | `DispatchSchedulerTest — triggerUrgent emits URGENT-scoped idempotency key and payload` | Mock `WorkflowEngine`, call `triggerUrgent()`, assert `idempotencyKey=dispatch-URGENT-<token>` and `initialItem` parses to `{"categories":["URGENT"]}`. |
| Scheduler | `empty set produces ALL-scoped idempotency key` | Invoke a test helper that calls the private `trigger(emptySet())`, assert `dispatch-ALL-<token>`. |
| Scheduler | `multi-category key is lexicographically sorted` | Invoke `trigger(setOf(DispatchCategory.URGENT, DispatchCategory.NORMAL))`, assert the key is `dispatch-NORMAL-URGENT-<token>` (sorted), not `dispatch-URGENT-NORMAL-<token>`. Guards against regressions that drop the `.sorted()` call. |
| E2E | `DispatchE2EHappyPathTest` — updated fixture seeds configs with categories; one variant asserts that a URGENT cron payload fans out only URGENT configs to simulation and that join receives the correct count. |

**Not tested (out of scope):**
- Quarkus `@Scheduled` actually firing at the declared wall-clock times — that is Quarkus's contract.
- Double-dispatch prevention across overlapping schedules — by design, an operator concern.
- Jdbi repository SQL for the `category IN (...)` predicate — no in-repo implementation exists to test.

---

## Non-Goals

The following are explicitly out of scope and must not be bundled into this change:

- Per-category deadline, retry policy, or `BatchStatus` variant.
- Per-category `WorkflowDefinition` DSL variant (see rejected alternative below).
- Admin API or DB-backed runtime mutation of categories or their crontabs.
- Metrics / log tags keyed by category. If needed later, can be added by reading the category out of the existing log line or payload.
- Automatic overlap detection across schedules.

---

## Alternatives Considered

**One `WorkflowDefinition` per category** — define `dispatchWorkflowUrgent`, `dispatchWorkflowNormal`, etc. Rejected because it duplicates the DSL and either requires N handler classes or an awkward CDI registration of one bean under N transition names. Category becomes compile-time visible at the cost of real churn in the DSL layer. Worse "minimum modification" story than the chosen approach despite not touching interfaces.

**Add `category` as a first-class parameter on `WorkflowLifecycle.startWorkflow`** — plumb the category through the engine. Rejected because it modifies the engine's public interface for a concern the engine has no business knowing about. Violates the hard requirement that dispatch-specific concerns stay in `dispatch/`.

**Programmatic scheduler registration from a config-driven map** — inject Quarkus `Scheduler` and register jobs at startup from `dispatch.categories.*=<cron>`. Rejected because the user confirmed category set is fixed and small; the added wiring complexity is not worth the configuration-only flexibility gain.

**Runtime DB-backed category/cron table** — a `dispatch_category` table with fast-polling scheduler. Rejected for the same reason: categories change via code review, not via DML.
