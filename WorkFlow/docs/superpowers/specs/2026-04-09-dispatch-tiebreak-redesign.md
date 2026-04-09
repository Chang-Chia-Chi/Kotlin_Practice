# Dispatch Tiebreaker Redesign

**Date:** 2026-04-09
**Scope:** `dispatch/usecase/service/algorithm/`, `dispatch/usecase/simulation/`, `dispatch/model/`, `dispatch/usecase/port/inbound/algorithm/`

---

## Problem

The current tiebreaker in `selectByGap` uses cyclic round-robin (picks the entry *after* `lastSelected`). This is wrong for two reasons:

1. **Site/BOM should be sticky, not rotating.** When all scoring conditions are tied, the correct behavior is to continue dispatching to the same site/BOM as last time — not advance to the next one.
2. **Ratio mode needs an additional tiebreaker.** Before falling back to sticky, ratio mode should prefer the BOM/site with lower cumulative dispatched quantity. Qty mode does not need this step.
3. **BOM last-picked is tracked globally.** `SimulationContext.lastBomId` is a single field reset on site change, meaning each site does not independently remember which BOM it last used.

---

## Tiebreaker Chains

| Mode | Chain |
|---|---|
| Qty | `gap → target (desc) → last dispatched (sticky)` |
| Ratio | `gap → target (desc) → current (asc) → last dispatched (sticky)` |

---

## Changes

### 1. `GapEntry` — add `current`

```kotlin
data class GapEntry(
    val id: String,
    val gap: BigDecimal,
    val target: BigDecimal,
    val current: BigDecimal,   // added: used by ratio cumulative tiebreaker
)
```

All construction sites pass the raw current value (`siteCurrents[siteId]` or `bomCurrents[key]`).

---

### 2. `selectByGap` — replace cyclic rank with sticky + optional cumulative

**Signature change:** add `useCumulative: Boolean`

```kotlin
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

**Key behaviors:**
- `lastIdx == -1` (null or unrecognized `lastSelected`): sticky term scores all entries `1` → list order wins.
- `lastIdx >= 0`: that entry scores `0`, all others score `1` → last picked wins all-tied scenarios.
- Round-robin is completely removed.

---

### 3. `GapComputer` — add `useCumulativeTiebreaker`

```kotlin
interface GapComputer {
    val useCumulativeTiebreaker: Boolean
    fun computeGap(current: BigDecimal, target: BigDecimal, total: BigDecimal): BigDecimal
}
```

| Implementation | Value |
|---|---|
| `QtyGapComputer` | `false` |
| `RatioGapComputer` | `true` |

---

### 4. `GapBasedDispatchAlgorithm` — wire `useCumulative` + per-site BOM lookup

Interface parameter change: `lastBomId: String?` → `lastBomIds: Map<String, String>`

```kotlin
// Site selection
val siteEntries = siteTargets.map { st ->
    val current = siteCurrents[st.siteId] ?: BigDecimal.ZERO
    GapEntry(st.siteId, gapComputer.computeGap(current, st.target, total), st.target, current)
}
val siteEntry = selectByGap(siteEntries, lastSiteId, gapComputer.useCumulativeTiebreaker)
    ?: return TargetSelection.NoTarget

// BOM selection
val bomEntries = bomMapping.targetAllocations.map { alloc ->
    val bomCurrent = bomCurrents[SiteBomKey(siteEntry.id, alloc.targetBomId)] ?: BigDecimal.ZERO
    GapEntry(alloc.targetBomId, gapComputer.computeGap(bomCurrent, alloc.target, siteCurrent), alloc.target, bomCurrent)
}
val bomEntry = selectByGap(bomEntries, lastBomIds[siteEntry.id], gapComputer.useCumulativeTiebreaker)
    ?: return TargetSelection.NoTarget
```

---

### 5. `SimulationContext` — per-site BOM tracking

```kotlin
// removed
var lastBomId: String? = null

// added
val lastBomIds: MutableMap<String, String> = mutableMapOf()
```

---

### 6. `SimulationEngine` — update call site and context update

**Call site:**
```kotlin
// before
context.lastSiteId, context.lastBomId, context.total

// after
context.lastSiteId, context.lastBomIds, context.total
```

**Context update after dispatch** (site-change reset logic removed):
```kotlin
// before
context.lastBomId = if (selection.siteId == context.lastSiteId) selection.targetBomId else null
context.lastSiteId = selection.siteId

// after
if (selection.targetBomId != null) context.lastBomIds[selection.siteId] = selection.targetBomId
context.lastSiteId = selection.siteId
```

---

### 7. `DispatchAlgorithm` interface

```kotlin
fun selectTarget(
    siteTargets: List<SiteTarget>,
    siteCurrents: Map<String, BigDecimal>,
    bomMappings: Map<String, BomMapping>?,
    bomCurrents: Map<SiteBomKey, BigDecimal>,
    lastSiteId: String?,
    lastBomIds: Map<String, String>,   // changed from lastBomId: String?
    total: BigDecimal,
): TargetSelection
```

---

## Test Changes

| File | Change |
|---|---|
| `GapKernelTest` | Replace round-robin tests with: (1) sticky picks same entry when tied, (2) cumulative current breaks tie before sticky; add `useCumulative` param to all calls |
| `GapComputerTest` | Assert `useCumulativeTiebreaker == false` for Qty, `== true` for Ratio |
| `DispatchAlgorithmTest` | `null` → `emptyMap()` for `lastBomIds` at all 3 call sites |
| `SimulationEngineTest` | Rename Task 3 test + update comment to reflect per-site map; add tied-BOM scenario: two dispatches to same site with fully tied BOMs — second picks same bom as first (sticky) |

---

## Files Modified

| File | Change |
|---|---|
| `dispatch/usecase/service/algorithm/GapKernel.kt` | Add `current` to `GapEntry`; replace rank with sticky + cumulative |
| `dispatch/usecase/service/algorithm/GapBasedDispatchAlgorithm.kt` | Wire `useCumulative`, `lastBomIds` lookup, `current` in entries |
| `dispatch/usecase/port/inbound/algorithm/GapComputer.kt` | Add `useCumulativeTiebreaker` |
| `dispatch/usecase/port/inbound/algorithm/DispatchAlgorithm.kt` | `lastBomId: String?` → `lastBomIds: Map<String, String>` |
| `dispatch/usecase/service/algorithm/QtyGapComputer.kt` | Implement `useCumulativeTiebreaker = false` |
| `dispatch/usecase/service/algorithm/RatioGapComputer.kt` | Implement `useCumulativeTiebreaker = true` |
| `dispatch/model/SimulationContext.kt` | `lastBomId` → `lastBomIds` |
| `dispatch/usecase/service/simulation/SimulationEngine.kt` | Update call site + context update |
| `GapKernelTest.kt` | Replace round-robin tests; add sticky + cumulative tests |
| `GapComputerTest.kt` | Assert `useCumulativeTiebreaker` |
| `DispatchAlgorithmTest.kt` | `emptyMap()` for `lastBomIds` |
| `SimulationEngineTest.kt` | Rename Task 3 test; add tied-BOM sticky scenario |
