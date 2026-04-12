# Phase 3 — Scatter Handler Reads `categories` From Payload

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Teach `DispatchScatterHandler` to extract a `categories` list from `taskPayload` and pass it to `handleCronTrigger`, which forwards the set to `configRepo.findActiveConfigs(now, categories)`. Missing, null, and empty-array payloads all collapse to `emptySet()` (meaning "no filter"). Unknown enum values crash loudly via `DispatchCategory.valueOf`. The dry-run branch is untouched.

**Architecture:** One file touched in `main/` (`DispatchScatterHandler.kt`) plus test additions in `DispatchHandlersTest.kt`. TDD: write the failing tests first for each parsing case, then implement.

**Tech Stack:** Jackson `JsonNode`, `kotlinx-coroutines-test` `runTest`, Mockito Kotlin argument captors.

---

## Task 1 — TDD: Write failing tests for payload parsing

**Files:**
- Modify: `src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt`

Add four new tests covering the new parsing branches. Write them all, run the suite, confirm they fail before implementing.

- [ ] **Step 1: Add an import for `argumentCaptor`, `eq`, and `Set` matchers**

Ensure these imports exist at the top (most already do via `org.mockito.kotlin.*`):

```kotlin
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
```

- [ ] **Step 2: Add the single-category test**

Append inside the `DispatchHandlersTest` class:

```kotlin
@Test
fun `scatter handler cron trigger with single category filters repo call`() = runTest {
    val configRepo = mock<DispatchConfigRepository>()
    val resultStore = mock<SimulationResultStore>()
    val config = DispatchConfig(
        id = "cfg1",
        category = DispatchCategory.URGENT,
        mode = DispatchMode.QTY,
        algorithmId = "default",
        sourceBomPrefix = "bom",
        siteTargets = listOf(SiteTarget("A", BigDecimal("100"))),
        bomMappings = null,
    )
    whenever(configRepo.findActiveConfigs(any(), any())).thenReturn(listOf(config))

    val handler = DispatchScatterHandler(configRepo, resultStore, objectMapper, SystemBatchTokenClock())
    val payload = """{"categories":["URGENT"]}"""
    handler.execute(HandlerInput("t1", "w1", 1, null, payload))

    val captor = argumentCaptor<Set<DispatchCategory>>()
    verify(configRepo).findActiveConfigs(any(), captor.capture())
    assertEquals(setOf(DispatchCategory.URGENT), captor.firstValue)
}
```

- [ ] **Step 3: Add the multi-category test**

Append:

```kotlin
@Test
fun `scatter handler cron trigger with multiple categories passes full set`() = runTest {
    val configRepo = mock<DispatchConfigRepository>()
    val resultStore = mock<SimulationResultStore>()
    whenever(configRepo.findActiveConfigs(any(), any())).thenReturn(emptyList())

    val handler = DispatchScatterHandler(configRepo, resultStore, objectMapper, SystemBatchTokenClock())
    val payload = """{"categories":["URGENT","NORMAL"]}"""
    handler.execute(HandlerInput("t1", "w1", 1, null, payload))

    val captor = argumentCaptor<Set<DispatchCategory>>()
    verify(configRepo).findActiveConfigs(any(), captor.capture())
    assertEquals(setOf(DispatchCategory.URGENT, DispatchCategory.NORMAL), captor.firstValue)
}
```

- [ ] **Step 4: Add the missing / empty / null tolerance test**

Append:

```kotlin
@Test
fun `scatter handler cron trigger with missing or empty categories passes empty set`() = runTest {
    val configRepo = mock<DispatchConfigRepository>()
    val resultStore = mock<SimulationResultStore>()
    whenever(configRepo.findActiveConfigs(any(), any())).thenReturn(emptyList())
    val handler = DispatchScatterHandler(configRepo, resultStore, objectMapper, SystemBatchTokenClock())

    // Case 1: null payload
    handler.execute(HandlerInput("t1", "w1", 1, null, null))
    // Case 2: empty object
    handler.execute(HandlerInput("t1", "w1", 1, null, "{}"))
    // Case 3: explicit empty array
    handler.execute(HandlerInput("t1", "w1", 1, null, """{"categories":[]}"""))

    val captor = argumentCaptor<Set<DispatchCategory>>()
    verify(configRepo, times(3)).findActiveConfigs(any(), captor.capture())
    assertEquals(emptySet<DispatchCategory>(), captor.firstValue)
    assertEquals(emptySet<DispatchCategory>(), captor.secondValue)
    assertEquals(emptySet<DispatchCategory>(), captor.thirdValue)
}
```

- [ ] **Step 5: Add the unknown-enum crash test**

Append:

```kotlin
@Test
fun `scatter handler cron trigger with unknown category throws IllegalArgumentException`() = runTest {
    val configRepo = mock<DispatchConfigRepository>()
    val resultStore = mock<SimulationResultStore>()
    val handler = DispatchScatterHandler(configRepo, resultStore, objectMapper, SystemBatchTokenClock())

    assertFailsWith<IllegalArgumentException> {
        handler.execute(HandlerInput("t1", "w1", 1, null, """{"categories":["BOGUS"]}"""))
    }
    verify(configRepo, never()).findActiveConfigs(any(), any())
}
```

- [ ] **Step 6: Add `org.mockito.kotlin.times` to imports if missing**

```kotlin
import org.mockito.kotlin.times
```

- [ ] **Step 7: Run the four new tests — they must fail**

Run:
```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test -q -Dtest=DispatchHandlersTest
```
Expected: FAIL. The new tests fail because:
- `findActiveConfigs(any(), any())` does not match the handler's current `findActiveConfigs(any())` call (arity mismatch at the stub, or the captor captures the default `emptySet()` even for `{"categories":["URGENT"]}`).
- Specifically, the single-category and multi-category tests fail their `assertEquals` assertions because the handler ignores the payload's `categories` field.
- The unknown-enum test fails because the handler never parses the payload at all and so never reaches `valueOf`.

If any of the new tests pass before the handler is updated, something is wrong — stop and investigate.

---

## Task 2 — Implement payload parsing in `DispatchScatterHandler`

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/service/handler/DispatchScatterHandler.kt`

- [ ] **Step 1: Add the `DispatchCategory` import**

Add to the imports block near the top:

```kotlin
import com.workflow.dispatch.model.DispatchCategory
```

- [ ] **Step 2: Rewrite `execute` to parse `categories`**

Replace the existing `execute` method body with:

```kotlin
override suspend fun execute(input: HandlerInput): HandlerResult {
    val itemNode = input.taskPayload?.let { objectMapper.readTree(it) }
    val providedToken = itemNode?.get("batchToken")?.takeIf { !it.isNull }?.asText()
    val configIdsNode = itemNode?.get("configIds")?.takeIf { it.isArray }

    val (items, token) = if (providedToken != null && configIdsNode != null) {
        handleDryRun(configIdsNode, providedToken)
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
```

- [ ] **Step 3: Update `handleCronTrigger` to accept and forward the category set**

Replace the existing `handleCronTrigger` method with:

```kotlin
// Path B — cron: generate token, create batch, query active configs (optionally filtered)
private suspend fun handleCronTrigger(
    categories: Set<DispatchCategory>,
): Pair<List<String>, String> {
    val token = clock.generate()
    val configs = configRepo.findActiveConfigs(LocalDateTime.now(), categories)
    resultStore.createBatch(token, BatchStatus.NORMAL, configs.size)
    return toItems(configs) to token
}
```

- [ ] **Step 4: Run the `DispatchHandlersTest` suite again**

Run:
```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test -q -Dtest=DispatchHandlersTest
```
Expected: PASS. The four new tests pass. The existing `scatter handler creates NORMAL batch and uses all active configs when no item` test also still passes — the null payload now routes through the new branch with `emptySet()`, and the existing Mockito stub `whenever(configRepo.findActiveConfigs(any()))` still matches because `any()` matches the first argument and the defaulted second argument is supplied automatically.

**If the old test fails** because Mockito's `whenever(configRepo.findActiveConfigs(any()))` no longer matches the two-argument call, update the two pre-existing stubs in that file to use `whenever(configRepo.findActiveConfigs(any(), any())).thenReturn(...)`. Search for both existing `whenever(configRepo.findActiveConfigs(any())).thenReturn(...)` call sites and widen them. Re-run the test and confirm pass.

- [ ] **Step 5: Run the full test suite**

Run:
```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test -q
```
Expected: BUILD SUCCESS. If `DispatchDryRunResourceTest` or `DispatchE2EHappyPathTest` fail on `findActiveConfigs` stubs, widen those stubs the same way — `any()` → `any(), any()`.

---

## Task 3 — Commit

- [ ] **Step 1: Stage**

```bash
git add src/main/kotlin/dispatch/usecase/service/handler/DispatchScatterHandler.kt
```
```bash
git add src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt
```

If any stubs in `DispatchDryRunResourceTest.kt` or `DispatchE2EHappyPathTest.kt` were widened in Task 2 Step 4, also stage those:

```bash
git add src/test/kotlin/dispatch/adapter/http/DispatchDryRunResourceTest.kt
```
```bash
git add src/test/kotlin/dispatch/DispatchE2EHappyPathTest.kt
```

- [ ] **Step 2: Commit**

```bash
git commit -m "✨ feat(dispatch): scatter handler filters configs by category set from payload

Parses a 'categories' array out of taskPayload and forwards it to
findActiveConfigs. Missing, null, and empty-array payloads collapse to
emptySet() (no filter). Unknown enum values crash loudly via valueOf.
Dry-run branch is untouched — it still selects on batchToken and
configIds without consulting categories."
```

- [ ] **Step 3: Verify**

```bash
git status
```
Expected: working tree clean of Phase 3 changes.
