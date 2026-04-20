# Phase 1 — Domain Model

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce `DispatchCategory` as a new domain enum, add `category` as a required field on `DispatchConfig`, and update every in-repo construction site so the project still compiles and all existing tests still pass. No behavior change, no production code paths touched yet.

**Architecture:** Enum lives in `dispatch/model/`. `DispatchConfig` gains the field at **position 2** (matching the spec's canonical order). Existing positional call sites in test code are converted to named arguments. The E2E fixture JSON gains a `"category"` field on every config, and `DispatchE2EFixture` reads it.

**Tech Stack:** Kotlin 2.3.x enum class, Jackson JSON.

---

## Task 1 — Create `DispatchCategory` enum

**Files:**
- Create: `src/main/kotlin/dispatch/model/DispatchCategory.kt`

- [ ] **Step 1: Create the enum file**

```kotlin
package com.workflow.dispatch.model

enum class DispatchCategory {
    URGENT,
    NORMAL,
    BACKGROUND,
}
```

- [ ] **Step 2: Verify it compiles**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o compile -q`
Expected: BUILD SUCCESS.

---

## Task 2 — Add `category` field to `DispatchConfig`

**Files:**
- Modify: `src/main/kotlin/dispatch/model/DispatchConfig.kt`

- [ ] **Step 1: Add the field at position 2**

Replace the entire data class body with:

```kotlin
package com.workflow.dispatch.model

data class DispatchConfig(
    val id: String,
    val category: DispatchCategory,
    val mode: DispatchMode,
    val algorithmId: String,
    /** Source BOM ID prefix (or full) used by CandidateRepository to filter candidates. */
    val sourceBomPrefix: String,
    val siteTargets: List<SiteTarget>,
    /** Keyed by siteId. LV2 sourceBomId must be full and start with [sourceBomPrefix]. */
    val bomMappings: Map<String, BomMapping>?,
)
```

- [ ] **Step 2: Attempt to compile — expect failure**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test-compile -q`
Expected: FAIL. Compilation errors in `DispatchHandlersTest.kt` and `DispatchDryRunResourceTest.kt` (positional constructions missing the new arg). `SimulationEngineTest.kt` and `DispatchE2EFixture.kt` also fail because they use the field names but are missing `category`. Task 3 fixes them.

---

## Task 3 — Fix positional `DispatchConfig` constructions in `DispatchHandlersTest.kt`

**Files:**
- Modify: `src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt`

`DispatchHandlersTest.kt` has 7 `DispatchConfig(...)` calls at lines ~37, 68, 77, 113, 168, 223, 272 — all positional. Convert every one to named arguments and add `category = DispatchCategory.NORMAL`.

- [ ] **Step 1: Add the import**

At the top of the file, next to the existing `com.workflow.dispatch.model.*` wildcard import, no change is needed — the wildcard already covers `DispatchCategory`. If the file uses specific imports instead of `model.*`, add:

```kotlin
import com.workflow.dispatch.model.DispatchCategory
```

- [ ] **Step 2: Convert every `DispatchConfig(...)` call to named args with `category = DispatchCategory.NORMAL`**

For each of the 7 call sites, replace the positional form with the named form. Example conversion (the exact pattern repeats at each site):

Before:
```kotlin
DispatchConfig(
    "cfg1",
    DispatchMode.QTY,
    "default",
    "bom",
    listOf(SiteTarget("A", BigDecimal("100"))),
    null,
)
```

After:
```kotlin
DispatchConfig(
    id = "cfg1",
    category = DispatchCategory.NORMAL,
    mode = DispatchMode.QTY,
    algorithmId = "default",
    sourceBomPrefix = "bom",
    siteTargets = listOf(SiteTarget("A", BigDecimal("100"))),
    bomMappings = null,
)
```

Do this for all 7 sites in the file. The only differences between sites are the `id` (`"cfg1"` vs `"cfg2"`) and the `siteTargets` list — everything else is copy-paste.

- [ ] **Step 3: Compile the test sources**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test-compile -q`
Expected: Remaining compilation errors only in `DispatchDryRunResourceTest.kt` and `DispatchE2EFixture.kt`. `DispatchHandlersTest.kt` compiles clean.

---

## Task 4 — Fix positional `DispatchConfig` constructions in `DispatchDryRunResourceTest.kt`

**Files:**
- Modify: `src/test/kotlin/dispatch/adapter/http/DispatchDryRunResourceTest.kt`

This file has 3 positional constructions at lines ~118, 146, 148.

- [ ] **Step 1: Ensure `DispatchCategory` is imported**

Add (if not already present via wildcard):

```kotlin
import com.workflow.dispatch.model.DispatchCategory
```

- [ ] **Step 2: Convert each positional construction to named form with `category = DispatchCategory.NORMAL`**

Apply the same pattern as Task 3 Step 2 to each of the 3 call sites. Example conversion for line ~146:

Before:
```kotlin
DispatchConfig("id1", DispatchMode.QTY, "default", "bom", ...)
```

After:
```kotlin
DispatchConfig(
    id = "id1",
    category = DispatchCategory.NORMAL,
    mode = DispatchMode.QTY,
    algorithmId = "default",
    sourceBomPrefix = "bom",
    siteTargets = ...,
    bomMappings = ...,
)
```

Do this for all 3 sites.

- [ ] **Step 3: Compile**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test-compile -q`
Expected: Remaining failures only in `SimulationEngineTest.kt` and `DispatchE2EFixture.kt`.

---

## Task 5 — Add `category` to every `DispatchConfig` in `SimulationEngineTest.kt`

**Files:**
- Modify: `src/test/kotlin/dispatch/usecase/service/simulation/SimulationEngineTest.kt`

This file has ~40 `DispatchConfig(...)` calls, all using named arguments already. Each needs a `category = DispatchCategory.NORMAL` entry added. Because the test file is large, do this in one focused pass.

- [ ] **Step 1: Ensure `DispatchCategory` is imported**

Confirm the import at the top of the file. If specific imports, add:

```kotlin
import com.workflow.dispatch.model.DispatchCategory
```

- [ ] **Step 2: Add `category = DispatchCategory.NORMAL` to every `DispatchConfig(...)` call**

In every call that currently looks like:

```kotlin
val config = DispatchConfig(
    id = "...",
    mode = ...,
    algorithmId = "...",
    sourceBomPrefix = "...",
    siteTargets = ...,
    bomMappings = ...,
)
```

insert `category = DispatchCategory.NORMAL,` between the `id` line and the `mode` line so the final form is:

```kotlin
val config = DispatchConfig(
    id = "...",
    category = DispatchCategory.NORMAL,
    mode = ...,
    algorithmId = "...",
    sourceBomPrefix = "...",
    siteTargets = ...,
    bomMappings = ...,
)
```

This is mechanical — there are no semantic choices. Every site in the file uses `DispatchCategory.NORMAL`.

- [ ] **Step 3: Compile**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test-compile -q`
Expected: Remaining failure only in `DispatchE2EFixture.kt`.

---

## Task 6 — Update `DispatchE2EFixture.kt` to read `category` from fixture JSON

**Files:**
- Modify: `src/test/kotlin/dispatch/DispatchE2EFixture.kt:25-45`

The fixture loader builds `DispatchConfig` instances from JSON. It needs to read `node["category"]` and convert to `DispatchCategory`.

- [ ] **Step 1: Ensure `DispatchCategory` is imported**

At the top of the file, add if missing:

```kotlin
import com.workflow.dispatch.model.DispatchCategory
```

- [ ] **Step 2: Update the `configs()` method to read `category`**

Replace the `DispatchConfig(...)` construction inside `configs()` with:

```kotlin
fun configs(): List<DispatchConfig> = root["configs"].map { node ->
    DispatchConfig(
        id = node["id"].asText(),
        category = DispatchCategory.valueOf(node["category"].asText()),
        mode = DispatchMode.valueOf(node["mode"].asText()),
        algorithmId = node["algorithmId"].asText(),
        sourceBomPrefix = node["sourceBomPrefix"].asText(),
        siteTargets = node["siteTargets"].map { st ->
            SiteTarget(st["siteId"].asText(), BigDecimal(st["target"].asText()))
        },
        bomMappings = node["bomMappings"]?.takeIf { !it.isNull }?.let { bm ->
            bm.fields().asSequence().associate { (siteId, mapping) ->
                siteId to BomMapping(
                    sourceBomId = mapping["sourceBomId"].asText(),
                    targetAllocations = mapping["targetAllocations"].map { ta ->
                        TargetBomAllocation(ta["targetBomId"].asText(), BigDecimal(ta["target"].asText()))
                    },
                )
            }
        },
    )
}
```

The only change is the addition of the `category = DispatchCategory.valueOf(node["category"].asText())` line.

- [ ] **Step 3: Compile all test sources**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test-compile -q`
Expected: BUILD SUCCESS. No compilation errors anywhere.

---

## Task 7 — Add `"category"` to every entry in the E2E fixture JSON

**Files:**
- Modify: `src/test/resources/fixtures/dispatch-e2e-fixture.json`

The fixture currently has three configs (`CFG-A`, `CFG-B`, `CFG-C`). `DispatchE2EFixture.kt` now requires every entry to have a `category` field, otherwise the test will throw at load time.

- [ ] **Step 1: Add `"category": "NORMAL"` to every config in the JSON**

After the `"id"` line of each config, add `"category": "NORMAL",`. The final shape of the first config should be:

```json
{
  "id": "CFG-A",
  "category": "NORMAL",
  "mode": "QTY",
  "algorithmId": "default",
  "sourceBomPrefix": "BOM-A",
  ...
}
```

Apply the same edit to `CFG-B` and `CFG-C`.

- [ ] **Step 2: Run the full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test -q`
Expected: BUILD SUCCESS, all existing tests green. No new tests yet.

---

## Task 8 — Commit

- [ ] **Step 1: Stage only the Phase 1 files**

Run each command in its own invocation — do not chain with `&&` or `;`.

```bash
git add src/main/kotlin/dispatch/model/DispatchCategory.kt
```
```bash
git add src/main/kotlin/dispatch/model/DispatchConfig.kt
```
```bash
git add src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt
```
```bash
git add src/test/kotlin/dispatch/adapter/http/DispatchDryRunResourceTest.kt
```
```bash
git add src/test/kotlin/dispatch/usecase/service/simulation/SimulationEngineTest.kt
```
```bash
git add src/test/kotlin/dispatch/DispatchE2EFixture.kt
```
```bash
git add src/test/resources/fixtures/dispatch-e2e-fixture.json
```

- [ ] **Step 2: Commit**

```bash
git commit -m "♻️ refactor(dispatch): add DispatchCategory enum and DispatchConfig.category field

Introduces a new domain enum and adds category as a required field at
position 2 of DispatchConfig. All in-repo construction sites default to
DispatchCategory.NORMAL. No behavior change — production paths still
call findActiveConfigs with no category filter. Sets up the model for
the category-based scheduling work in subsequent phases."
```

- [ ] **Step 3: Verify the commit is clean**

```bash
git status
```
Expected: working tree clean (ignoring any unrelated files already modified before the plan started).
