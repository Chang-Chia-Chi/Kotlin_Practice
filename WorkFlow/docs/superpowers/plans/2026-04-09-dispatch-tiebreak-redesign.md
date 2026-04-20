# Dispatch Tiebreaker Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace cyclic round-robin tiebreaker with sticky (last-picked) plus mode-aware cumulative tiebreaker; track last-picked BOM per site instead of a single global field.

**Architecture:** All changes are self-contained within `dispatch/usecase/service/algorithm/`, `dispatch/usecase/service/simulation/`, `dispatch/model/`, and `dispatch/usecase/port/inbound/algorithm/`. No API or contract changes.

**Tech Stack:** Kotlin 2.3, JUnit 5, BigDecimal arithmetic. Maven: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn`

---

## Files Modified

| File | Task |
|---|---|
| `src/main/kotlin/dispatch/usecase/service/algorithm/GapKernel.kt` | Task 1 |
| `src/test/kotlin/dispatch/usecase/service/algorithm/GapKernelTest.kt` | Task 1 |
| `src/main/kotlin/dispatch/usecase/service/algorithm/GapBasedDispatchAlgorithm.kt` | Tasks 1, 2, 3 |
| `src/main/kotlin/dispatch/usecase/port/inbound/algorithm/GapComputer.kt` | Task 2 |
| `src/main/kotlin/dispatch/usecase/service/algorithm/QtyGapComputer.kt` | Task 2 |
| `src/main/kotlin/dispatch/usecase/service/algorithm/RatioGapComputer.kt` | Task 2 |
| `src/test/kotlin/dispatch/usecase/service/algorithm/GapComputerTest.kt` | Task 2 |
| `src/main/kotlin/dispatch/usecase/port/inbound/algorithm/DispatchAlgorithm.kt` | Task 3 |
| `src/main/kotlin/dispatch/model/SimulationContext.kt` | Task 3 |
| `src/main/kotlin/dispatch/usecase/service/simulation/SimulationEngine.kt` | Task 3 |
| `src/test/kotlin/dispatch/usecase/service/algorithm/DispatchAlgorithmTest.kt` | Task 3 |
| `src/test/kotlin/dispatch/usecase/service/simulation/SimulationEngineTest.kt` | Task 3 |

---

## Task 1: Rewrite `GapKernel` — sticky tiebreaker + cumulative support

**What changes:**
- `GapEntry` gets a `current: BigDecimal` field (used by the ratio cumulative tiebreaker).
- `selectByGap` replaces the cyclic rank array with a simple sticky check (`if (it == lastIdx) 0 else 1`) and gains a `useCumulative: Boolean` parameter.
- `GapBasedDispatchAlgorithm` is patched to compile (passes `current` to `GapEntry`; `useCumulative = false` temporarily).

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/service/algorithm/GapKernel.kt`
- Modify: `src/test/kotlin/dispatch/usecase/service/algorithm/GapKernelTest.kt`
- Modify: `src/main/kotlin/dispatch/usecase/service/algorithm/GapBasedDispatchAlgorithm.kt`

- [ ] **Step 1: Replace `GapKernelTest.kt`**

Replace the entire file:

```kotlin
package com.workflow.dispatch.usecase.service.algorithm

import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNull

class GapKernelTest {

    @Test
    fun `selects entry with lowest gap`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-10"), BigDecimal("50"), BigDecimal("40")),
            GapEntry("B", BigDecimal("-20"), BigDecimal("50"), BigDecimal("40")),
        )
        assertEquals("B", selectByGap(entries, null, false)?.id)
    }

    @Test
    fun `breaks tie by highest target`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-10"), BigDecimal("30"), BigDecimal("40")),
            GapEntry("B", BigDecimal("-10"), BigDecimal("50"), BigDecimal("40")),
        )
        assertEquals("B", selectByGap(entries, null, false)?.id)
    }

    @Test
    fun `breaks remaining tie by last dispatched — no prior defaults to list order`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-10"), BigDecimal("50"), BigDecimal("40")),
            GapEntry("B", BigDecimal("-10"), BigDecimal("50"), BigDecimal("40")),
        )
        assertEquals("A", selectByGap(entries, null, false)?.id)   // no prior → list order → A
        assertEquals("A", selectByGap(entries, "A", false)?.id)    // last was A → sticky → A
        assertEquals("B", selectByGap(entries, "B", false)?.id)    // last was B → sticky → B
    }

    @Test
    fun `returns null for empty entries`() {
        assertNull(selectByGap(emptyList(), null, false))
    }

    @Test
    fun `breaks tie by lowest current when useCumulative is true`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-10"), BigDecimal("50"), BigDecimal("60")),
            GapEntry("B", BigDecimal("-10"), BigDecimal("50"), BigDecimal("40")),
        )
        // B has lower current → wins cumulative tiebreaker even though A was last
        assertEquals("B", selectByGap(entries, "A", true)?.id)
    }

    @Test
    fun `sticky applies after cumulative when current is also tied`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-10"), BigDecimal("50"), BigDecimal("40")),
            GapEntry("B", BigDecimal("-10"), BigDecimal("50"), BigDecimal("40")),
        )
        // cumulative tied (same current) → sticky → B
        assertEquals("B", selectByGap(entries, "B", true)?.id)
    }

    @Test
    fun `sticky does not override lower gap`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-5"), BigDecimal("50"), BigDecimal("40")),
            GapEntry("B", BigDecimal("-20"), BigDecimal("50"), BigDecimal("40")),
        )
        assertEquals("B", selectByGap(entries, "A", false)?.id)
    }
}
```

- [ ] **Step 2: Run the tests — expect compilation failure**

```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="GapKernelTest" 2>&1 | tail -20
```

Expected: compilation error — `GapEntry` has no `current` parameter and `selectByGap` has wrong arity.

- [ ] **Step 3: Rewrite `GapKernel.kt`**

Replace the entire file:

```kotlin
package com.workflow.dispatch.usecase.service.algorithm

import java.math.BigDecimal

data class GapEntry(
    val id: String,
    val gap: BigDecimal,
    val target: BigDecimal,
    val current: BigDecimal,
)

fun selectByGap(entries: List<GapEntry>, lastSelected: String?, useCumulative: Boolean): GapEntry? {
    if (entries.isEmpty()) return null
    val lastIdx = entries.indexOfFirst { it.id == lastSelected }
    val comparator = compareBy<Int> { entries[it].gap }
        .thenByDescending { entries[it].target }
        .run { if (useCumulative) thenBy { entries[it].current } else this }
        .thenBy { if (it == lastIdx) 0 else 1 }
    return entries.indices.minWithOrNull(comparator)?.let { entries[it] }
}
```

- [ ] **Step 4: Patch `GapBasedDispatchAlgorithm.kt` to compile**

In `GapBasedDispatchAlgorithm.kt`, update the four lines that construct `GapEntry` and call `selectByGap`.

Replace the site entries block:
```kotlin
// Before
val siteEntries = siteTargets.map { st ->
    val current = siteCurrents[st.siteId] ?: BigDecimal.ZERO
    GapEntry(st.siteId, gapComputer.computeGap(current, st.target, total), st.target)
}
val siteEntry = selectByGap(siteEntries, lastSiteId) ?: return TargetSelection.NoTarget
```

```kotlin
// After
val siteEntries = siteTargets.map { st ->
    val current = siteCurrents[st.siteId] ?: BigDecimal.ZERO
    GapEntry(st.siteId, gapComputer.computeGap(current, st.target, total), st.target, current)
}
val siteEntry = selectByGap(siteEntries, lastSiteId, false) ?: return TargetSelection.NoTarget
```

Replace the BOM entries block:
```kotlin
// Before
val bomEntries = bomMapping.targetAllocations.map { alloc ->
    val bomCurrent = bomCurrents[SiteBomKey(siteEntry.id, alloc.targetBomId)] ?: BigDecimal.ZERO
    GapEntry(alloc.targetBomId, gapComputer.computeGap(bomCurrent, alloc.target, siteCurrent), alloc.target)
}
val bomEntry = selectByGap(bomEntries, lastBomId) ?: return TargetSelection.NoTarget
```

```kotlin
// After
val bomEntries = bomMapping.targetAllocations.map { alloc ->
    val bomCurrent = bomCurrents[SiteBomKey(siteEntry.id, alloc.targetBomId)] ?: BigDecimal.ZERO
    GapEntry(alloc.targetBomId, gapComputer.computeGap(bomCurrent, alloc.target, siteCurrent), alloc.target, bomCurrent)
}
val bomEntry = selectByGap(bomEntries, lastBomId, false) ?: return TargetSelection.NoTarget
```

- [ ] **Step 5: Run `GapKernelTest` — expect all 7 PASS**

```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="GapKernelTest"
```

Expected: BUILD SUCCESS, 7 tests pass.

- [ ] **Step 6: Run the full dispatch test suite — expect all green**

```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test
```

Expected: all green.

- [ ] **Step 7: Commit**

```
git add src/main/kotlin/dispatch/usecase/service/algorithm/GapKernel.kt
git add src/test/kotlin/dispatch/usecase/service/algorithm/GapKernelTest.kt
git add src/main/kotlin/dispatch/usecase/service/algorithm/GapBasedDispatchAlgorithm.kt
git commit -m "refactor(dispatch): replace round-robin tiebreaker with sticky + cumulative support in selectByGap"
```

---

## Task 2: `GapComputer` — add `useCumulativeTiebreaker`; wire into algorithm

**What changes:**
- `GapComputer` interface gets `val useCumulativeTiebreaker: Boolean`.
- `QtyGapComputer` declares `false`; `RatioGapComputer` declares `true`.
- `GapBasedDispatchAlgorithm` replaces the temporary `false` with `gapComputer.useCumulativeTiebreaker`.
- `GapComputerTest` asserts the flag values.

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/port/inbound/algorithm/GapComputer.kt`
- Modify: `src/main/kotlin/dispatch/usecase/service/algorithm/QtyGapComputer.kt`
- Modify: `src/main/kotlin/dispatch/usecase/service/algorithm/RatioGapComputer.kt`
- Modify: `src/test/kotlin/dispatch/usecase/service/algorithm/GapComputerTest.kt`
- Modify: `src/main/kotlin/dispatch/usecase/service/algorithm/GapBasedDispatchAlgorithm.kt`

- [ ] **Step 1: Add two tests to `GapComputerTest.kt`**

Append inside the `GapComputerTest` class, after the existing tests:

```kotlin
@Test
fun `QtyGapComputer does not use cumulative tiebreaker`() {
    assertFalse(QtyGapComputer().useCumulativeTiebreaker)
}

@Test
fun `RatioGapComputer uses cumulative tiebreaker`() {
    assertTrue(RatioGapComputer().useCumulativeTiebreaker)
}
```

Also add the missing imports at the top of the file:
```kotlin
import kotlin.test.assertFalse
import kotlin.test.assertTrue
```

- [ ] **Step 2: Run `GapComputerTest` — expect compilation failure**

```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="GapComputerTest" 2>&1 | tail -20
```

Expected: compilation error — `useCumulativeTiebreaker` not found.

- [ ] **Step 3: Add `useCumulativeTiebreaker` to `GapComputer.kt`**

Replace the entire file:

```kotlin
package com.workflow.dispatch.usecase.port.inbound.algorithm

import java.math.BigDecimal

interface GapComputer {
    val useCumulativeTiebreaker: Boolean
    fun computeGap(current: BigDecimal, target: BigDecimal, total: BigDecimal): BigDecimal
}
```

- [ ] **Step 4: Implement in `QtyGapComputer.kt`**

Replace the entire file:

```kotlin
package com.workflow.dispatch.usecase.service.algorithm

import com.workflow.dispatch.usecase.port.inbound.algorithm.GapComputer
import java.math.BigDecimal

class QtyGapComputer : GapComputer {
    override val useCumulativeTiebreaker: Boolean = false
    override fun computeGap(current: BigDecimal, target: BigDecimal, total: BigDecimal): BigDecimal =
        current - target
}
```

- [ ] **Step 5: Implement in `RatioGapComputer.kt`**

Replace the entire file:

```kotlin
package com.workflow.dispatch.usecase.service.algorithm

import com.workflow.dispatch.usecase.port.inbound.algorithm.GapComputer
import java.math.BigDecimal
import java.math.RoundingMode

class RatioGapComputer : GapComputer {

    override val useCumulativeTiebreaker: Boolean = true

    override fun computeGap(current: BigDecimal, target: BigDecimal, total: BigDecimal): BigDecimal {
        val currentRatio = if (total > BigDecimal.ZERO) {
            current.divide(total, 10, RoundingMode.HALF_UP)
        } else {
            BigDecimal.ZERO
        }
        val targetRatio = target.divide(HUNDRED, 10, RoundingMode.HALF_UP)
        return currentRatio - targetRatio
    }

    private companion object {
        val HUNDRED: BigDecimal = BigDecimal("100")
    }
}
```

- [ ] **Step 6: Wire `gapComputer.useCumulativeTiebreaker` in `GapBasedDispatchAlgorithm.kt`**

Replace both temporary `false` values:

```kotlin
// Before
val siteEntry = selectByGap(siteEntries, lastSiteId, false) ?: return TargetSelection.NoTarget
```

```kotlin
// After
val siteEntry = selectByGap(siteEntries, lastSiteId, gapComputer.useCumulativeTiebreaker) ?: return TargetSelection.NoTarget
```

```kotlin
// Before
val bomEntry = selectByGap(bomEntries, lastBomId, false) ?: return TargetSelection.NoTarget
```

```kotlin
// After
val bomEntry = selectByGap(bomEntries, lastBomId, gapComputer.useCumulativeTiebreaker) ?: return TargetSelection.NoTarget
```

- [ ] **Step 7: Run `GapComputerTest` — expect all 6 PASS**

```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="GapComputerTest"
```

Expected: BUILD SUCCESS, 6 tests pass.

- [ ] **Step 8: Run the full suite — expect all green**

```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test
```

Expected: all green.

- [ ] **Step 9: Commit**

```
git add src/main/kotlin/dispatch/usecase/port/inbound/algorithm/GapComputer.kt
git add src/main/kotlin/dispatch/usecase/service/algorithm/QtyGapComputer.kt
git add src/main/kotlin/dispatch/usecase/service/algorithm/RatioGapComputer.kt
git add src/test/kotlin/dispatch/usecase/service/algorithm/GapComputerTest.kt
git add src/main/kotlin/dispatch/usecase/service/algorithm/GapBasedDispatchAlgorithm.kt
git commit -m "feat(dispatch): add useCumulativeTiebreaker to GapComputer; wire into selectByGap calls"
```

---

## Task 3: Per-site BOM tracking — all layers

**What changes:**
- `DispatchAlgorithm` interface: `lastBomId: String?` → `lastBomIds: Map<String, String>`.
- `GapBasedDispatchAlgorithm`: parameter renamed; BOM lookup becomes `lastBomIds[siteEntry.id]`.
- `SimulationContext`: `lastBomId: String?` removed; `lastBomIds: MutableMap<String, String>` added.
- `SimulationEngine`: passes `context.lastBomIds`; replaces site-change reset with per-site put.
- `DispatchAlgorithmTest`: existing calls updated (`null` → `emptyMap()`); tied-BOM sticky test added.
- `SimulationEngineTest`: Task 3 test renamed; comment updated.

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/port/inbound/algorithm/DispatchAlgorithm.kt`
- Modify: `src/main/kotlin/dispatch/usecase/service/algorithm/GapBasedDispatchAlgorithm.kt`
- Modify: `src/main/kotlin/dispatch/model/SimulationContext.kt`
- Modify: `src/main/kotlin/dispatch/usecase/service/simulation/SimulationEngine.kt`
- Modify: `src/test/kotlin/dispatch/usecase/service/algorithm/DispatchAlgorithmTest.kt`
- Modify: `src/test/kotlin/dispatch/usecase/service/simulation/SimulationEngineTest.kt`

- [ ] **Step 1: Update `DispatchAlgorithmTest.kt`**

Three changes:

**1a. Update `lv1 only selects site with lowest gap`** — change 6th arg `null` → `emptyMap()`:

```kotlin
// Before
val result = algo.selectTarget(
    targets, currents, null, emptyMap(), null, null, BigDecimal("140"),
)
```

```kotlin
// After
val result = algo.selectTarget(
    targets, currents, null, emptyMap(), null, emptyMap(), BigDecimal("140"),
)
```

**1b. Update `lv2 selects site and targetBomId`** — change 6th arg `null` → `emptyMap()`:

```kotlin
// Before
val result = algo.selectTarget(
    targets, currents, bomMappings, bomCurrents, null, null, BigDecimal("50"),
)
```

```kotlin
// After
val result = algo.selectTarget(
    targets, currents, bomMappings, bomCurrents, null, emptyMap(), BigDecimal("50"),
)
```

**1c. Update `returns NoTarget when no sites`** — change 6th arg `null` → `emptyMap()`:

```kotlin
// Before
val result = algo.selectTarget(
    emptyList(), emptyMap(), null, emptyMap(), null, null, BigDecimal.ZERO,
)
```

```kotlin
// After
val result = algo.selectTarget(
    emptyList(), emptyMap(), null, emptyMap(), null, emptyMap(), BigDecimal.ZERO,
)
```

**1d. Add the tied-BOM sticky test** — append inside the `DispatchAlgorithmTest` class:

```kotlin
@Test
fun `lv2 sticky bom — returns last selected bom when all boms are tied`() {
    val algo = qtyAlgorithm()
    val targets = listOf(SiteTarget("A", BigDecimal("100")))
    val currents = mapOf("A" to BigDecimal("0"))
    val bomMappings = mapOf(
        "A" to BomMapping(
            sourceBomId = "src",
            targetAllocations = listOf(
                TargetBomAllocation("bom1", BigDecimal("50")),
                TargetBomAllocation("bom2", BigDecimal("50")),
            ),
        ),
    )
    val bomCurrents = emptyMap<SiteBomKey, BigDecimal>()

    // no prior → list order → bom1
    val first = algo.selectTarget(
        targets, currents, bomMappings, bomCurrents, null, emptyMap(), BigDecimal.ZERO,
    )
    assertIs<TargetSelection.Selected>(first)
    assertEquals("bom1", first.targetBomId)

    // last was bom1 for site A → sticky → bom1
    val second = algo.selectTarget(
        targets, currents, bomMappings, bomCurrents, null, mapOf("A" to "bom1"), BigDecimal.ZERO,
    )
    assertIs<TargetSelection.Selected>(second)
    assertEquals("bom1", second.targetBomId)

    // last was bom2 for site A → sticky → bom2
    val third = algo.selectTarget(
        targets, currents, bomMappings, bomCurrents, null, mapOf("A" to "bom2"), BigDecimal.ZERO,
    )
    assertIs<TargetSelection.Selected>(third)
    assertEquals("bom2", third.targetBomId)
}
```

- [ ] **Step 2: Run `DispatchAlgorithmTest` — expect compilation failure**

```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchAlgorithmTest" 2>&1 | tail -20
```

Expected: compilation error — `selectTarget` 6th parameter type mismatch.

- [ ] **Step 3: Update `DispatchAlgorithm.kt` interface**

Replace the entire file:

```kotlin
package com.workflow.dispatch.usecase.port.inbound.algorithm

import com.workflow.dispatch.model.BomMapping
import com.workflow.dispatch.model.SiteBomKey
import com.workflow.dispatch.model.SiteTarget
import com.workflow.dispatch.model.TargetSelection
import java.math.BigDecimal

interface DispatchAlgorithm {
    val candidateMatcher: CandidateMatcher
    val terminationStrategy: TerminationStrategy

    fun selectTarget(
        siteTargets: List<SiteTarget>,
        siteCurrents: Map<String, BigDecimal>,
        bomMappings: Map<String, BomMapping>?,
        bomCurrents: Map<SiteBomKey, BigDecimal>,
        lastSiteId: String?,
        lastBomIds: Map<String, String>,
        total: BigDecimal,
    ): TargetSelection
}
```

- [ ] **Step 4: Update `GapBasedDispatchAlgorithm.kt` — rename parameter + fix BOM lookup**

Replace the `selectTarget` signature and the BOM `selectByGap` call:

```kotlin
// Before — signature
override fun selectTarget(
    siteTargets: List<SiteTarget>,
    siteCurrents: Map<String, BigDecimal>,
    bomMappings: Map<String, BomMapping>?,
    bomCurrents: Map<SiteBomKey, BigDecimal>,
    lastSiteId: String?,
    lastBomId: String?,
    total: BigDecimal,
): TargetSelection {
```

```kotlin
// After — signature
override fun selectTarget(
    siteTargets: List<SiteTarget>,
    siteCurrents: Map<String, BigDecimal>,
    bomMappings: Map<String, BomMapping>?,
    bomCurrents: Map<SiteBomKey, BigDecimal>,
    lastSiteId: String?,
    lastBomIds: Map<String, String>,
    total: BigDecimal,
): TargetSelection {
```

```kotlin
// Before — BOM selection
val bomEntry = selectByGap(bomEntries, lastBomId, gapComputer.useCumulativeTiebreaker) ?: return TargetSelection.NoTarget
```

```kotlin
// After — BOM selection
val bomEntry = selectByGap(bomEntries, lastBomIds[siteEntry.id], gapComputer.useCumulativeTiebreaker) ?: return TargetSelection.NoTarget
```

- [ ] **Step 5: Update `SimulationContext.kt`**

Replace the entire file:

```kotlin
package com.workflow.dispatch.model

import java.math.BigDecimal

class SimulationContext(
    val siteCurrents: MutableMap<String, BigDecimal>,
    val bomCurrents: MutableMap<SiteBomKey, BigDecimal>,
    var lastSiteId: String? = null,
    val lastBomIds: MutableMap<String, String> = mutableMapOf(),
    val decisions: MutableList<DispatchDecision> = mutableListOf(),
    var total: BigDecimal,
)
```

- [ ] **Step 6: Update `SimulationEngine.kt` — call site + context update**

Replace the `algorithm.selectTarget(...)` call:

```kotlin
// Before
val selection = algorithm.selectTarget(
    config.siteTargets, context.siteCurrents,
    config.bomMappings, context.bomCurrents,
    context.lastSiteId, context.lastBomId, context.total,
)
```

```kotlin
// After
val selection = algorithm.selectTarget(
    config.siteTargets, context.siteCurrents,
    config.bomMappings, context.bomCurrents,
    context.lastSiteId, context.lastBomIds, context.total,
)
```

Replace the context update block (lines after `context.total += qty`):

```kotlin
// Before
// Reset BOM round-robin when the site changes so each site gets independent cycling.
context.lastBomId = if (selection.siteId == context.lastSiteId) selection.targetBomId else null
context.lastSiteId = selection.siteId
```

```kotlin
// After
if (selection.targetBomId != null) context.lastBomIds[selection.siteId] = selection.targetBomId
context.lastSiteId = selection.siteId
```

- [ ] **Step 7: Rename the Task 3 test in `SimulationEngineTest.kt`**

Find and rename the test function and update its comment:

```kotlin
// Before
@Test
fun `bom round-robin resets to first entry when arriving at a new site`() {
    // Both sites share the same 3 BOM IDs: [bom1(50), bom2(50), bom3(50)].
    // After dispatching to site A (→ bom1), the next dispatch goes to site B.
    // Expected: B picks bom1 (round-robin resets), not bom2 (cross-site carry-over).
```

```kotlin
// After
@Test
fun `each site starts at first bom when no prior dispatch recorded for that site`() {
    // Both sites share the same 3 BOM IDs: [bom1(50), bom2(50), bom3(50)].
    // After dispatching to site A (→ bom1, stored in lastBomIds["A"]), the next dispatch goes to site B.
    // Expected: B picks bom1 (no entry in lastBomIds for B → list order), not bom2.
```

- [ ] **Step 8: Run `DispatchAlgorithmTest` — expect all 4 PASS**

```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchAlgorithmTest"
```

Expected: BUILD SUCCESS, 4 tests pass.

- [ ] **Step 9: Run `SimulationEngineTest` — expect all PASS**

```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SimulationEngineTest"
```

Expected: all green.

- [ ] **Step 10: Run full suite — expect all green**

```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test
```

Expected: all green.

- [ ] **Step 11: Commit**

```
git add src/main/kotlin/dispatch/usecase/port/inbound/algorithm/DispatchAlgorithm.kt
git add src/main/kotlin/dispatch/usecase/service/algorithm/GapBasedDispatchAlgorithm.kt
git add src/main/kotlin/dispatch/model/SimulationContext.kt
git add src/main/kotlin/dispatch/usecase/service/simulation/SimulationEngine.kt
git add src/test/kotlin/dispatch/usecase/service/algorithm/DispatchAlgorithmTest.kt
git add src/test/kotlin/dispatch/usecase/service/simulation/SimulationEngineTest.kt
git commit -m "feat(dispatch): track last-picked BOM per site; replace global lastBomId with lastBomIds map"
```

---

## Self-Review

| Spec requirement | Task |
|---|---|
| Remove cyclic round-robin | Task 1 ✓ |
| Sticky tiebreaker (last-picked wins ties) | Task 1 ✓ |
| `GapEntry.current` for cumulative | Task 1 ✓ |
| `useCumulativeTiebreaker` on `GapComputer` | Task 2 ✓ |
| Qty = false, Ratio = true | Task 2 ✓ |
| Wire `useCumulativeTiebreaker` into algorithm | Task 2 ✓ |
| `lastBomIds: Map<String, String>` interface param | Task 3 ✓ |
| Per-site BOM lookup in algorithm | Task 3 ✓ |
| `SimulationContext.lastBomIds` | Task 3 ✓ |
| Engine passes `context.lastBomIds` | Task 3 ✓ |
| Engine update: per-site put, no reset | Task 3 ✓ |
| GapKernelTest: sticky + cumulative tests | Task 1 ✓ |
| GapComputerTest: useCumulativeTiebreaker assertions | Task 2 ✓ |
| DispatchAlgorithmTest: tied-BOM sticky scenario | Task 3 ✓ |
| SimulationEngineTest: rename Task 3 test | Task 3 ✓ |

**Placeholder scan:** No TBDs. All code blocks are complete. All commands have expected output.

**Type consistency:**
- `GapEntry(id, gap, target, current)` — 4-param constructor defined Task 1, used Task 1 (test + algorithm patch).
- `selectByGap(entries, lastSelected, useCumulative)` — 3-param signature defined Task 1, used Tasks 1–3.
- `lastBomIds: Map<String, String>` — defined Task 3 (interface), matched in algorithm, context, engine.
- `context.lastBomIds` — `MutableMap<String, String>` defined Task 3 Step 5, used Task 3 Step 6.
