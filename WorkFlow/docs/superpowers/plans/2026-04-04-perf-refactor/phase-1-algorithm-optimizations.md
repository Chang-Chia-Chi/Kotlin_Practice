# Phase 1: Algorithm Optimizations

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate O(N^2) and unnecessary O(N log N) algorithmic inefficiencies in the dispatch simulation hot path and workflow model.

**Architecture:** Pure Kotlin logic changes — no SQL, no new ports. Each task modifies one file (plus its test) and is independently verifiable.

**Tech Stack:** Kotlin, JUnit 5

---

## Task 1: topologicalSort — eliminate O(N^2) from `add(0, name)`

**Files:**
- Modify: `src/main/kotlin/workflow/model/WorkflowDefinition.kt:48-66`
- Test: `src/test/kotlin/workflow/model/SequenceModelTest.kt` (existing, must still pass)

- [ ] **Step 1: Modify topologicalSort to append + reverse**

In `src/main/kotlin/workflow/model/WorkflowDefinition.kt`, replace the `topologicalSort` function:

```kotlin
internal fun topologicalSort(definition: WorkflowDefinition): List<String> {
    val permanent = mutableSetOf<String>()
    val temporary = mutableSetOf<String>()
    val result = mutableListOf<String>()

    fun visit(name: String) {
        if (name in permanent) return
        require(name !in temporary) { "Cycle detected involving activity '$name'" }
        temporary += name
        val activity = definition.activities[name] ?: return
        for (edge in activity.successors) visit(edge.target)
        temporary -= name
        permanent += name
        result.add(name)
    }

    visit(definition.start)
    return result.asReversed()
}
```

Changes:
- `result.add(0, name)` → `result.add(name)` (O(1) append instead of O(N) shift)
- `return result` → `return result.asReversed()` (O(1) reversed view)

- [ ] **Step 2: Run existing tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="SequenceModelTest,WorkflowModelsTest"`
Expected: All tests PASS — the topological order is unchanged.

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/workflow/model/WorkflowDefinition.kt
git commit -m "perf(workflow): use append+reverse in topologicalSort to avoid O(N^2) shifting"
```

---

## Task 2: selectByGap — return GapEntry with O(N) minWith instead of O(N log N) sort

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/service/algorithm/GapKernel.kt`
- Modify: `src/main/kotlin/dispatch/usecase/service/algorithm/GapBasedDispatchAlgorithm.kt`
- Test: `src/test/kotlin/dispatch/usecase/service/algorithm/GapKernelTest.kt`
- Test: `src/test/kotlin/dispatch/usecase/service/algorithm/DispatchAlgorithmTest.kt` (existing, must still pass)

- [ ] **Step 1: Update GapKernelTest to expect GapEntry return type**

In `src/test/kotlin/dispatch/usecase/service/algorithm/GapKernelTest.kt`, replace the full file:

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
            GapEntry("A", BigDecimal("-10"), BigDecimal("50")),
            GapEntry("B", BigDecimal("-20"), BigDecimal("50")),
        )
        val result = selectByGap(entries, null)
        assertEquals("B", result?.id)
        assertEquals(BigDecimal("-20"), result?.gap)
    }

    @Test
    fun `breaks tie by highest target`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-10"), BigDecimal("30")),
            GapEntry("B", BigDecimal("-10"), BigDecimal("50")),
        )
        assertEquals("B", selectByGap(entries, null)?.id)
    }

    @Test
    fun `breaks double tie with round-robin routing`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-10"), BigDecimal("50")),
            GapEntry("B", BigDecimal("-10"), BigDecimal("50")),
        )
        assertEquals("B", selectByGap(entries, "A")?.id)
        assertEquals("A", selectByGap(entries, "B")?.id)
    }

    @Test
    fun `returns null for empty entries`() {
        assertNull(selectByGap(emptyList(), null))
    }

    @Test
    fun `sticky routing does not override lower gap`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-5"), BigDecimal("50")),
            GapEntry("B", BigDecimal("-20"), BigDecimal("50")),
        )
        assertEquals("B", selectByGap(entries, "A")?.id)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="GapKernelTest"`
Expected: FAIL — `selectByGap` still returns `String?`, not `GapEntry?`.

- [ ] **Step 3: Update selectByGap to return GapEntry using minWithOrNull**

In `src/main/kotlin/dispatch/usecase/service/algorithm/GapKernel.kt`, replace the full file:

```kotlin
package com.workflow.dispatch.usecase.service.algorithm

import java.math.BigDecimal

data class GapEntry(
    val id: String,
    val gap: BigDecimal,
    val target: BigDecimal,
)

fun selectByGap(entries: List<GapEntry>, lastSelected: String?): GapEntry? {
    if (entries.isEmpty()) return null
    return entries.minWithOrNull(
        compareBy<GapEntry> { it.gap }
            .thenByDescending { it.target }
            .thenBy { it.id == lastSelected },
    )
}
```

- [ ] **Step 4: Run GapKernelTest**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="GapKernelTest"`
Expected: PASS

- [ ] **Step 5: Update GapBasedDispatchAlgorithm to use GapEntry result directly**

In `src/main/kotlin/dispatch/usecase/service/algorithm/GapBasedDispatchAlgorithm.kt`, replace the `selectTarget` method body:

```kotlin
    override fun selectTarget(
        siteTargets: List<SiteTarget>,
        siteCurrents: Map<String, BigDecimal>,
        bomMappings: Map<String, BomMapping>?,
        bomCurrents: Map<SiteBomKey, BigDecimal>,
        lastSiteId: String?,
        lastBomId: String?,
        total: BigDecimal,
    ): TargetSelection {
        val siteEntries = siteTargets.map { st ->
            val current = siteCurrents[st.siteId] ?: BigDecimal.ZERO
            GapEntry(st.siteId, gapComputer.computeGap(current, st.target, total), st.target)
        }
        val siteEntry = selectByGap(siteEntries, lastSiteId) ?: return TargetSelection.NoTarget

        val bomMapping = bomMappings?.get(siteEntry.id)
            ?: return TargetSelection.Selected(siteEntry.id, null, null, siteEntry.gap, null)

        val bomTotal = siteCurrents[siteEntry.id] ?: BigDecimal.ZERO
        val bomEntries = bomMapping.targetAllocations.map { alloc ->
            val bomCurrent = bomCurrents[SiteBomKey(siteEntry.id, alloc.targetBomId)] ?: BigDecimal.ZERO
            GapEntry(alloc.targetBomId, gapComputer.computeGap(bomCurrent, alloc.target, bomTotal), alloc.target)
        }
        val bomEntry = selectByGap(bomEntries, lastBomId) ?: return TargetSelection.NoTarget

        return TargetSelection.Selected(siteEntry.id, bomEntry.id, bomMapping.sourceBomId, siteEntry.gap, bomEntry.gap)
    }
```

Changes:
- `selectByGap` now returns `GapEntry?` instead of `String?`
- Eliminated `siteEntries.first { it.id == siteId }.gap` re-scans (was O(S) each)
- Use `siteEntry.id` / `siteEntry.gap` directly

- [ ] **Step 6: Run algorithm tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="GapKernelTest,DispatchAlgorithmTest,SimulationEngineTest"`
Expected: All PASS

- [ ] **Step 7: Commit**

```bash
git add src/main/kotlin/dispatch/usecase/service/algorithm/GapKernel.kt src/main/kotlin/dispatch/usecase/service/algorithm/GapBasedDispatchAlgorithm.kt src/test/kotlin/dispatch/usecase/service/algorithm/GapKernelTest.kt
git commit -m "perf(dispatch): selectByGap returns GapEntry with O(N) minWith, eliminates re-scan"
```

---

## Task 3: CandidateIndex — lazy pruning to eliminate O(N^2) scanning

**Files:**
- Modify: `src/main/kotlin/dispatch/model/CandidateIndex.kt`
- Test: `src/test/kotlin/dispatch/model/CandidateIndexTest.kt` (existing, must still pass)

- [ ] **Step 1: Add a test for large-scale consume-then-find pattern**

In `src/test/kotlin/dispatch/model/CandidateIndexTest.kt`, add this test at the end of the class:

```kotlin
    @Test
    fun `findFirst skips consumed entries efficiently with lazy pruning`() {
        val large = (1..100).map { CandidateProduct("p$it", "bom-A", 1) }
        val index = CandidateIndex(large)

        // Consume first 90 candidates
        for (i in 0 until 90) index.consume(i)

        // findFirst should still return the 91st candidate
        val idx = index.findFirst(null)
        assertEquals(90, idx)
        assertTrue(index.hasUnconsumed())

        // After consuming all, findFirst returns null
        for (i in 90 until 100) index.consume(i)
        assertNull(index.findFirst(null))
        assertFalse(index.hasUnconsumed())
    }
```

- [ ] **Step 2: Run tests to verify the new test passes with current impl**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="CandidateIndexTest"`
Expected: PASS (the test is correct with both old and new implementations; it's a regression guard)

- [ ] **Step 3: Refactor CandidateIndex with lazy-pruning iterator**

In `src/main/kotlin/dispatch/model/CandidateIndex.kt`, replace the full file:

```kotlin
package com.workflow.dispatch.model

import java.util.BitSet
import java.util.LinkedList

class CandidateIndex(private val candidates: List<CandidateProduct>) {

    private val bySourceBom: Map<String, LinkedList<Int>> =
        candidates.indices.groupBy { candidates[it].sourceBomId }
            .mapValues { (_, indices) -> LinkedList(indices) }

    private val allIndices: LinkedList<Int> = LinkedList(candidates.indices.toList())

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
        val iter = pool.iterator()
        while (iter.hasNext()) {
            val idx = iter.next()
            if (consumed[idx]) { iter.remove(); continue }
            if (predicate(candidates[idx])) return idx
        }
        return null
    }

    fun consume(index: Int) {
        consumed.set(index)
    }

    fun hasUnconsumed(): Boolean = consumed.cardinality() < candidates.size

    operator fun get(index: Int): CandidateProduct = candidates[index]
}
```

Changes:
- Pools changed from `List<Int>` to `LinkedList<Int>` (O(1) iterator removal)
- `findFirst` now lazily prunes consumed entries via `iter.remove()`
- Each consumed entry is removed from the pool it's encountered in, so it's never scanned again in that pool
- Amortized: each element visited at most once per pool it belongs to (global + BOM-specific), then removed — total work O(N) across all `findFirst` calls
- `consumed` BitSet retained for O(1) check + cross-pool tracking + `hasUnconsumed()`

- [ ] **Step 4: Run CandidateIndex and SimulationEngine tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="CandidateIndexTest,SimulationEngineTest,CandidateMatcherTest"`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/dispatch/model/CandidateIndex.kt src/test/kotlin/dispatch/model/CandidateIndexTest.kt
git commit -m "perf(dispatch): CandidateIndex lazy-prunes consumed entries for O(N) amortized findFirst"
```

---

## Task 4: SimulationEngine — pre-build lookup maps before hot loop

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/service/simulation/SimulationEngine.kt`
- Test: `src/test/kotlin/dispatch/usecase/service/simulation/SimulationEngineTest.kt` (existing, must still pass)

- [ ] **Step 1: Pre-build siteTargetMap and bomTargetMap before the while loop**

In `src/main/kotlin/dispatch/usecase/service/simulation/SimulationEngine.kt`, replace the `simulate` method:

```kotlin
    fun simulate(
        config: DispatchConfig,
        candidates: List<CandidateProduct>,
        baseline: Baseline,
    ): SimulationResult {
        config.bomMappings?.forEach { (siteId, mapping) ->
            require(mapping.sourceBomId.startsWith(config.sourceBomPrefix)) {
                "Site $siteId LV2 sourceBomId '${mapping.sourceBomId}' " +
                    "must start with LV1 prefix '${config.sourceBomPrefix}'"
            }
        }

        val algorithm = algorithmFactory.create(config.mode, config.algorithmId)
        val index = CandidateIndex(candidates)
        val context = SimulationContext(
            siteCurrents = baseline.siteAllocations.toMutableMap(),
            bomCurrents = baseline.bomAllocations.toMutableMap(),
            total = baseline.siteAllocations.values.fold(BigDecimal.ZERO, BigDecimal::add),
        )

        val siteTargetMap = config.siteTargets.associateBy { it.siteId }
        val bomTargetMap: Map<SiteBomKey, TargetBomAllocation> = config.bomMappings
            ?.flatMap { (siteId, mapping) ->
                mapping.targetAllocations.map { alloc ->
                    SiteBomKey(siteId, alloc.targetBomId) to alloc
                }
            }?.toMap() ?: emptyMap()

        val maxIterations = candidates.size * config.siteTargets.size
        var iterations = 0

        while (index.hasUnconsumed()) {
            if (++iterations > maxIterations) break

            val selection = algorithm.selectTarget(
                config.siteTargets, context.siteCurrents,
                config.bomMappings, context.bomCurrents,
                context.lastSiteId, context.lastBomId, context.total,
            )
            if (selection !is TargetSelection.Selected) break

            val siteTarget = siteTargetMap.getValue(selection.siteId)
            val bomTarget = if (selection.targetBomId != null) {
                bomTargetMap[SiteBomKey(selection.siteId, selection.targetBomId)]
            } else null
            val idx = algorithm.candidateMatcher.findCandidate(
                index, selection.sourceBomConstraint, context, siteTarget, bomTarget,
            )

            if (idx == null) {
                val decision = algorithm.terminationStrategy
                    .onNoCandidate(selection.siteId, selection.targetBomId, context)
                if (decision == TerminationDecision.STOP) break
                continue
            }

            val candidate = index[idx]
            val qty = candidate.qty.toBigDecimal()

            index.consume(idx)
            context.siteCurrents.merge(selection.siteId, qty, BigDecimal::add)
            if (selection.targetBomId != null) {
                context.bomCurrents.merge(
                    SiteBomKey(selection.siteId, selection.targetBomId), qty, BigDecimal::add,
                )
            }
            context.total += qty
            context.lastSiteId = selection.siteId
            context.lastBomId = selection.targetBomId

            context.decisions += DispatchDecision(
                dispatchOrder = context.decisions.size + 1,
                productId = candidate.productId,
                sourceBomId = candidate.sourceBomId,
                qty = candidate.qty,
                targetSiteId = selection.siteId,
                targetBomId = selection.targetBomId,
                siteGap = selection.siteGap,
                bomGap = selection.bomGap,
            )
        }

        return SimulationResult(
            decisions = context.decisions.toList(),
            finalSiteAllocations = context.siteCurrents.toMap(),
            finalBomAllocations = context.bomCurrents.toMap(),
        )
    }
```

Changes:
- Added `siteTargetMap` (`Map<String, SiteTarget>`) — O(1) lookup replaces O(S) linear scan
- Added `bomTargetMap` (`Map<SiteBomKey, TargetBomAllocation>`) — O(1) replaces O(A) linear scan
- `config.siteTargets.first { ... }` → `siteTargetMap.getValue(selection.siteId)`
- `config.bomMappings?.get(...)?.targetAllocations?.firstOrNull { ... }` → `bomTargetMap[SiteBomKey(...)]`
- Need to add import for `TargetBomAllocation` and `SiteBomKey` (already in scope via `import com.workflow.dispatch.model.*`)

- [ ] **Step 2: Run SimulationEngine tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="SimulationEngineTest"`
Expected: All PASS

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/dispatch/usecase/service/simulation/SimulationEngine.kt
git commit -m "perf(dispatch): pre-build siteTarget and bomTarget maps before simulation loop"
```

---

## Task 5: DefaultPhaseGate.successorsOf — pre-build seqByName map for O(1) lookup

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt`
- Test: `src/test/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGateTest.kt` (existing, must still pass)

- [ ] **Step 1: Add seqByName helper and refactor successorsOf**

In `src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt`, make these changes:

**a)** Replace the `successorsOf` method (around line 357):

```kotlin
    private fun successorsOf(
        seqInfo: SequenceInfo,
        seqByName: Map<String, SequenceInfo>,
        definition: WorkflowDefinition,
    ): List<SequenceInfo> {
        val actName = seqInfo.activityName.removeSuffix(".__parallel__")
        val activity = definition.activities[actName] ?: return emptyList()
        return activity.successors.mapNotNull { edge ->
            seqByName[edge.target]
        }.distinctBy { it.sequenceNumber }
    }
```

Changes: parameter `sequenceMap: Map<Int, SequenceInfo>` → `seqByName: Map<String, SequenceInfo>`. Each lookup is now O(1) map get instead of O(S) linear scan of `sequenceMap.values`.

**b)** In `onTaskCompleted`, after building `sequenceMap` (around line 79), add:

```kotlin
            val seqByName: Map<String, SequenceInfo> = sequenceMap.values
                .filter { it.phaseType != PhaseType.PARALLEL }
                .associateBy { it.activityName }
```

**c)** Update the call to `successorsOf` in `onTaskCompleted` (around line 157):

```kotlin
            evalQueue += successorsOf(seqInfo, seqByName, definition)
```

**d)** Update the cascade-skip call (around line 207):

```kotlin
                        evalQueue += successorsOf(successor, seqByName, definition)
```

**e)** In `recoverStuckWorkflow`, after building `sequenceMap` (around line 247), add:

```kotlin
            val seqByName: Map<String, SequenceInfo> = sequenceMap.values
                .filter { it.phaseType != PhaseType.PARALLEL }
                .associateBy { it.activityName }
```

**f)** In `recoverStuckWorkflow`, there's no direct call to `successorsOf` but `isAnyEdgeTaken` calls remain. No change needed for `isAnyEdgeTaken` in this task — that's addressed in Phase 3.

- [ ] **Step 2: Run DefaultPhaseGate tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="DefaultPhaseGateTest"`
Expected: All PASS

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt
git commit -m "perf(workflow): successorsOf uses pre-built seqByName map for O(1) lookup"
```
