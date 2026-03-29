# Dispatch Simulation System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a dispatch simulation system that allocates products to factories using a pluggable gap-based algorithm, integrated with the existing workflow engine for scheduled batch execution.

**Architecture:** Pure domain core (models -> algorithm -> simulation engine) with hexagonal port interfaces for external dependencies. Workflow handlers orchestrate the scatter/simulate/join lifecycle. Framework enhanced with idempotency key for cron deduplication.

**Tech Stack:** Kotlin 2.2, Quarkus 3.17 CDI, JDBI 3/Oracle, Jackson CSV, AWS SDK v2 S3

**Design Spec:** `docs/superpowers/specs/2026-03-29-dispatch-simulation-design.md`

---

## File Structure

```
src/main/kotlin/dispatch/
  model/
    DispatchModels.kt             # All domain data classes + enums
  algorithm/
    GapComputer.kt                # Interface + QtyGapComputer + RatioGapComputer
    SelectionKernel.kt            # SelectionEntry + selectByGap()
    CandidateMatcher.kt           # Interface + QtyCandidateMatcher + DefaultCandidateMatcher
    TerminationStrategy.kt        # Interface + FailFastTermination
    DispatchAlgorithm.kt          # Interface + DefaultDispatchAlgorithm
    DispatchAlgorithmDsl.kt       # AlgorithmBuilder + dispatchAlgorithm() DSL
    DispatchAlgorithmFactory.kt   # CDI factory
  simulation/
    CandidateIndex.kt             # Pre-grouped candidate lookup with BitSet
    SimulationContext.kt           # Mutable allocation state
    SimulationEngine.kt           # Main simulation loop
  port/
    DispatchPorts.kt              # All port interfaces
    DefaultCsvFormatter.kt        # Jackson CSV implementation
  adapter/
    S3StorageAdapter.kt           # StoragePort -> S3AsyncClient
    S3ClientProducer.kt           # CDI producer for S3AsyncClient
  handler/
    DispatchWorkflow.kt           # Workflow definition val
    DispatchScatterHandler.kt     # Scatter: query configs, emit fan-out items
    DispatchSimulationHandler.kt  # Simulate: per-config simulation + CSV upload
    DispatchJoinHandler.kt        # Join: merge results + parquet upload
    DispatchScheduler.kt          # Leader cron trigger

src/test/kotlin/dispatch/
  model/DispatchModelsTest.kt
  algorithm/
    GapComputerTest.kt
    SelectionKernelTest.kt
    CandidateMatcherTest.kt
    DispatchAlgorithmTest.kt
    DispatchAlgorithmDslTest.kt
  simulation/
    CandidateIndexTest.kt
    SimulationEngineTest.kt
  port/DefaultCsvFormatterTest.kt
  handler/DispatchHandlersTest.kt

src/main/resources/db/migration/
  V9__idempotency_key.sql
```

---

### Task 1: Domain Models

**Files:**
- Create: `src/main/kotlin/dispatch/model/DispatchModels.kt`
- Test: `src/test/kotlin/dispatch/model/DispatchModelsTest.kt`

- [ ] **Step 1: Write failing tests for model validation**

```kotlin
package com.workflow.dispatch.model

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal
import kotlin.test.assertEquals

class DispatchModelsTest {

    @Test
    fun `CandidateProduct rejects qty outside 1-25`() {
        assertThrows<IllegalArgumentException> {
            CandidateProduct("p1", "bom1", 0)
        }
        assertThrows<IllegalArgumentException> {
            CandidateProduct("p1", "bom1", 26)
        }
    }

    @Test
    fun `CandidateProduct accepts qty in valid range`() {
        val p = CandidateProduct("p1", "bom1", 5)
        assertEquals(5, p.qty)
    }

    @Test
    fun `SiteTarget rejects non-positive target`() {
        assertThrows<IllegalArgumentException> {
            SiteTarget("site1", BigDecimal.ZERO)
        }
    }

    @Test
    fun `SiteBomKey equality by siteId and targetBomId`() {
        val k1 = SiteBomKey("s1", "b1")
        val k2 = SiteBomKey("s1", "b1")
        assertEquals(k1, k2)
        assertEquals(k1.hashCode(), k2.hashCode())
    }

    @Test
    fun `DispatchMode has QTY and RATIO`() {
        assertEquals(2, DispatchMode.entries.size)
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchModelsTest" -pl .`
Expected: compilation failure (classes don't exist yet)

- [ ] **Step 3: Implement domain models**

```kotlin
package com.workflow.dispatch.model

import java.math.BigDecimal

enum class DispatchMode { QTY, RATIO }

data class DispatchConfig(
    val id: String,
    val mode: DispatchMode,
    val algorithmId: String,
    val siteTargets: List<SiteTarget>,
    val bomMappings: Map<String, BomMapping>?,
)

data class SiteTarget(
    val siteId: String,
    val target: BigDecimal,
) {
    init {
        require(target > BigDecimal.ZERO) { "target must be positive, got $target" }
    }
}

data class BomMapping(
    val sourceBomId: String,
    val targetAllocations: List<TargetBomAllocation>,
)

data class TargetBomAllocation(
    val targetBomId: String,
    val target: BigDecimal,
)

data class CandidateProduct(
    val productId: String,
    val sourceBomId: String,
    val qty: Int,
) {
    init {
        require(qty in 1..25) { "qty must be 1-25, got $qty" }
    }
}

data class DispatchDecision(
    val dispatchOrder: Int,
    val productId: String,
    val sourceBomId: String,
    val qty: Int,
    val targetSiteId: String,
    val targetBomId: String?,
    val siteGap: BigDecimal,
    val bomGap: BigDecimal?,
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

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchModelsTest" -pl .`
Expected: all 5 tests pass

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/dispatch/model/DispatchModels.kt src/test/kotlin/dispatch/model/DispatchModelsTest.kt
git commit -m "feat(dispatch): add domain models"
```

---

### Task 2: Port Interfaces

**Files:**
- Create: `src/main/kotlin/dispatch/port/DispatchPorts.kt`

No tests needed — pure interface contracts.

- [ ] **Step 1: Create port interfaces**

```kotlin
package com.workflow.dispatch.port

import com.workflow.dispatch.model.CandidateProduct
import com.workflow.dispatch.model.Baseline
import com.workflow.dispatch.model.DispatchConfig
import com.workflow.dispatch.model.DispatchDecision
import java.time.LocalDateTime

interface DispatchConfigRepository {
    suspend fun findActiveConfigs(asOf: LocalDateTime): List<DispatchConfig>
    suspend fun findById(configId: String): DispatchConfig
}

interface CandidateQueryPort {
    suspend fun queryCandidates(config: DispatchConfig): List<CandidateProduct>
}

interface BaselineProvider {
    suspend fun loadBaseline(config: DispatchConfig): Baseline
}

interface SimulationResultStore {
    suspend fun saveDecisions(batchToken: String, configId: String, decisions: List<DispatchDecision>)
    suspend fun findByBatchToken(batchToken: String): List<DispatchDecision>
}

interface StoragePort {
    suspend fun uploadCsv(path: String, content: ByteArray)
    suspend fun uploadParquet(path: String, content: ByteArray)
}

interface CsvFormatter {
    fun format(batchToken: String, configId: String, decisions: List<DispatchDecision>): ByteArray
}

interface ParquetFormatter {
    fun format(decisions: List<DispatchDecision>): ByteArray
}
```

- [ ] **Step 2: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl .`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/dispatch/port/DispatchPorts.kt
git commit -m "feat(dispatch): add port interfaces"
```

---

### Task 3: Gap Computers + Selection Kernel

**Files:**
- Create: `src/main/kotlin/dispatch/algorithm/GapComputer.kt`
- Create: `src/main/kotlin/dispatch/algorithm/SelectionKernel.kt`
- Test: `src/test/kotlin/dispatch/algorithm/GapComputerTest.kt`
- Test: `src/test/kotlin/dispatch/algorithm/SelectionKernelTest.kt`

- [ ] **Step 1: Write failing tests for GapComputer**

```kotlin
package com.workflow.dispatch.algorithm

import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals

class GapComputerTest {

    @Test
    fun `QtyGapComputer returns current minus target`() {
        val gc = QtyGapComputer()
        // current=30, target=50 -> gap=-20
        assertEquals(
            BigDecimal("-20"),
            gc.computeGap(BigDecimal("30"), BigDecimal("50"), BigDecimal("100")),
        )
    }

    @Test
    fun `QtyGapComputer ignores total`() {
        val gc = QtyGapComputer()
        val gap1 = gc.computeGap(BigDecimal("30"), BigDecimal("50"), BigDecimal("100"))
        val gap2 = gc.computeGap(BigDecimal("30"), BigDecimal("50"), BigDecimal("999"))
        assertEquals(gap1, gap2)
    }

    @Test
    fun `RatioGapComputer returns ratio difference`() {
        val gc = RatioGapComputer()
        // current=30, total=100 -> ratio=0.30, target=50% -> gap=0.30-0.50=-0.20
        val gap = gc.computeGap(BigDecimal("30"), BigDecimal("50"), BigDecimal("100"))
        assertEquals(0, gap.compareTo(BigDecimal("-0.20")))
    }

    @Test
    fun `RatioGapComputer returns zero ratio when total is zero`() {
        val gc = RatioGapComputer()
        // total=0 -> currentRatio=0, target=50% -> gap=0-0.50=-0.50
        val gap = gc.computeGap(BigDecimal.ZERO, BigDecimal("50"), BigDecimal.ZERO)
        assertEquals(0, gap.compareTo(BigDecimal("-0.50")))
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="GapComputerTest" -pl .`
Expected: compilation failure

- [ ] **Step 3: Implement GapComputer**

```kotlin
package com.workflow.dispatch.algorithm

import java.math.BigDecimal
import java.math.RoundingMode

interface GapComputer {
    fun computeGap(current: BigDecimal, target: BigDecimal, total: BigDecimal): BigDecimal
}

class QtyGapComputer : GapComputer {
    override fun computeGap(current: BigDecimal, target: BigDecimal, total: BigDecimal): BigDecimal =
        current - target
}

class RatioGapComputer : GapComputer {
    override fun computeGap(current: BigDecimal, target: BigDecimal, total: BigDecimal): BigDecimal {
        val currentRatio = if (total > BigDecimal.ZERO) {
            current.divide(total, 10, RoundingMode.HALF_UP)
        } else {
            BigDecimal.ZERO
        }
        val targetRatio = target.divide(BigDecimal(100), 10, RoundingMode.HALF_UP)
        return currentRatio - targetRatio
    }
}
```

- [ ] **Step 4: Run GapComputer tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="GapComputerTest" -pl .`
Expected: all 4 tests pass

- [ ] **Step 5: Write failing tests for SelectionKernel**

```kotlin
package com.workflow.dispatch.algorithm

import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SelectionKernelTest {

    @Test
    fun `selects entry with lowest gap`() {
        val entries = listOf(
            SelectionEntry("A", BigDecimal("-10"), BigDecimal("50")),
            SelectionEntry("B", BigDecimal("-20"), BigDecimal("50")),
        )
        assertEquals("B", selectByGap(entries, null))
    }

    @Test
    fun `breaks tie by highest target`() {
        val entries = listOf(
            SelectionEntry("A", BigDecimal("-10"), BigDecimal("30")),
            SelectionEntry("B", BigDecimal("-10"), BigDecimal("50")),
        )
        assertEquals("B", selectByGap(entries, null))
    }

    @Test
    fun `breaks double tie with sticky routing`() {
        val entries = listOf(
            SelectionEntry("A", BigDecimal("-10"), BigDecimal("50")),
            SelectionEntry("B", BigDecimal("-10"), BigDecimal("50")),
        )
        assertEquals("A", selectByGap(entries, "A"))
        assertEquals("B", selectByGap(entries, "B"))
    }

    @Test
    fun `returns null for empty entries`() {
        assertNull(selectByGap(emptyList(), null))
    }

    @Test
    fun `sticky routing does not override lower gap`() {
        val entries = listOf(
            SelectionEntry("A", BigDecimal("-5"), BigDecimal("50")),
            SelectionEntry("B", BigDecimal("-20"), BigDecimal("50")),
        )
        // B has lower gap, sticky on A should not override
        assertEquals("B", selectByGap(entries, "A"))
    }
}
```

- [ ] **Step 6: Implement SelectionKernel**

```kotlin
package com.workflow.dispatch.algorithm

import java.math.BigDecimal

data class SelectionEntry(
    val id: String,
    val gap: BigDecimal,
    val target: BigDecimal,
)

fun selectByGap(entries: List<SelectionEntry>, lastSelected: String?): String? {
    if (entries.isEmpty()) return null
    return entries
        .sortedWith(
            compareBy<SelectionEntry> { it.gap }
                .thenByDescending { it.target }
                .thenByDescending { it.id == lastSelected },
        )
        .first()
        .id
}
```

- [ ] **Step 7: Run all algorithm tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="GapComputerTest,SelectionKernelTest" -pl .`
Expected: all 9 tests pass

- [ ] **Step 8: Commit**

```bash
git add src/main/kotlin/dispatch/algorithm/GapComputer.kt src/main/kotlin/dispatch/algorithm/SelectionKernel.kt src/test/kotlin/dispatch/algorithm/GapComputerTest.kt src/test/kotlin/dispatch/algorithm/SelectionKernelTest.kt
git commit -m "feat(dispatch): add gap computers and selection kernel"
```

---

### Task 4: CandidateIndex + SimulationContext

**Files:**
- Create: `src/main/kotlin/dispatch/simulation/CandidateIndex.kt`
- Create: `src/main/kotlin/dispatch/simulation/SimulationContext.kt`
- Test: `src/test/kotlin/dispatch/simulation/CandidateIndexTest.kt`

- [ ] **Step 1: Write failing tests for CandidateIndex**

```kotlin
package com.workflow.dispatch.simulation

import com.workflow.dispatch.model.CandidateProduct
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class CandidateIndexTest {

    private val candidates = listOf(
        CandidateProduct("p1", "bom-A", 5),
        CandidateProduct("p2", "bom-B", 3),
        CandidateProduct("p3", "bom-A", 2),
    )

    @Test
    fun `findFirst returns first unconsumed candidate`() {
        val index = CandidateIndex(candidates)
        val idx = index.findFirst(null)
        assertEquals(0, idx)
        assertEquals("p1", index[idx!!].productId)
    }

    @Test
    fun `findFirst with sourceBom constraint filters correctly`() {
        val index = CandidateIndex(candidates)
        val idx = index.findFirst("bom-B")
        assertEquals(1, idx)
    }

    @Test
    fun `findFirst returns null for unknown sourceBom`() {
        val index = CandidateIndex(candidates)
        assertNull(index.findFirst("bom-Z"))
    }

    @Test
    fun `consume marks candidate as used`() {
        val index = CandidateIndex(candidates)
        index.consume(0)
        // Next findFirst(null) should skip index 0
        assertEquals(1, index.findFirst(null))
    }

    @Test
    fun `consume marks bom-specific candidate as used`() {
        val index = CandidateIndex(candidates)
        index.consume(0) // consume first bom-A
        val idx = index.findFirst("bom-A")
        assertEquals(2, idx) // second bom-A candidate
    }

    @Test
    fun `hasUnconsumed returns false when all consumed`() {
        val index = CandidateIndex(candidates)
        index.consume(0)
        index.consume(1)
        index.consume(2)
        assertFalse(index.hasUnconsumed())
    }

    @Test
    fun `hasUnconsumed returns true when some remain`() {
        val index = CandidateIndex(candidates)
        index.consume(0)
        assertTrue(index.hasUnconsumed())
    }

    @Test
    fun `findFirst with predicate filters candidates`() {
        val index = CandidateIndex(candidates)
        // Only accept candidates with qty >= 4
        val idx = index.findFirst(null) { it.qty >= 4 }
        assertEquals(0, idx) // p1 has qty=5
    }

    @Test
    fun `findFirst with predicate skips non-matching`() {
        val index = CandidateIndex(candidates)
        // Only accept qty >= 4; skip p1 by consuming it
        index.consume(0)
        val idx = index.findFirst(null) { it.qty >= 4 }
        assertNull(idx) // p2=3, p3=2 — none >= 4
    }

    @Test
    fun `empty candidates list`() {
        val index = CandidateIndex(emptyList())
        assertFalse(index.hasUnconsumed())
        assertNull(index.findFirst(null))
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="CandidateIndexTest" -pl .`
Expected: compilation failure

- [ ] **Step 3: Implement CandidateIndex**

```kotlin
package com.workflow.dispatch.simulation

import com.workflow.dispatch.model.CandidateProduct
import java.util.BitSet

class CandidateIndex(private val candidates: List<CandidateProduct>) {

    private val bySourceBom: Map<String, List<Int>> =
        candidates.indices.groupBy { candidates[it].sourceBomId }

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

    fun consume(index: Int) {
        consumed.set(index)
    }

    fun hasUnconsumed(): Boolean = consumed.cardinality() < candidates.size

    operator fun get(index: Int): CandidateProduct = candidates[index]
}
```

- [ ] **Step 4: Implement SimulationContext**

```kotlin
package com.workflow.dispatch.simulation

import com.workflow.dispatch.model.DispatchDecision
import com.workflow.dispatch.model.SiteBomKey
import java.math.BigDecimal

class SimulationContext(
    val siteCurrents: MutableMap<String, BigDecimal>,
    val bomCurrents: MutableMap<SiteBomKey, BigDecimal>,
    var lastSiteId: String? = null,
    var lastBomId: String? = null,
    val decisions: MutableList<DispatchDecision> = mutableListOf(),
    var total: BigDecimal,
)
```

- [ ] **Step 5: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="CandidateIndexTest" -pl .`
Expected: all 10 tests pass

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/dispatch/simulation/CandidateIndex.kt src/main/kotlin/dispatch/simulation/SimulationContext.kt src/test/kotlin/dispatch/simulation/CandidateIndexTest.kt
git commit -m "feat(dispatch): add CandidateIndex and SimulationContext"
```

---

### Task 5: CandidateMatcher + TerminationStrategy

**Files:**
- Create: `src/main/kotlin/dispatch/algorithm/CandidateMatcher.kt`
- Create: `src/main/kotlin/dispatch/algorithm/TerminationStrategy.kt`
- Test: `src/test/kotlin/dispatch/algorithm/CandidateMatcherTest.kt`

- [ ] **Step 1: Write failing tests for CandidateMatcher**

```kotlin
package com.workflow.dispatch.algorithm

import com.workflow.dispatch.model.CandidateProduct
import com.workflow.dispatch.model.SiteTarget
import com.workflow.dispatch.simulation.CandidateIndex
import com.workflow.dispatch.simulation.SimulationContext
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CandidateMatcherTest {

    private fun makeContext(siteCurrents: Map<String, BigDecimal>): SimulationContext =
        SimulationContext(
            siteCurrents = siteCurrents.toMutableMap(),
            bomCurrents = mutableMapOf(),
            total = BigDecimal.ZERO,
        )

    @Test
    fun `DefaultCandidateMatcher returns first matching sourceBom`() {
        val candidates = listOf(
            CandidateProduct("p1", "bom-A", 5),
            CandidateProduct("p2", "bom-B", 3),
        )
        val index = CandidateIndex(candidates)
        val matcher = DefaultCandidateMatcher()
        val ctx = makeContext(emptyMap())
        val target = SiteTarget("s1", BigDecimal("100"))

        assertEquals(1, matcher.findCandidate(index, "bom-B", ctx, target))
    }

    @Test
    fun `DefaultCandidateMatcher returns first candidate when no constraint`() {
        val candidates = listOf(CandidateProduct("p1", "bom-A", 5))
        val index = CandidateIndex(candidates)
        val matcher = DefaultCandidateMatcher()
        val ctx = makeContext(emptyMap())
        val target = SiteTarget("s1", BigDecimal("100"))

        assertEquals(0, matcher.findCandidate(index, null, ctx, target))
    }

    @Test
    fun `QtyCandidateMatcher rejects candidate that exceeds target`() {
        val candidates = listOf(CandidateProduct("p1", "bom-A", 10))
        val index = CandidateIndex(candidates)
        val matcher = QtyCandidateMatcher()
        // site already at 95, target is 100, candidate qty=10 -> 105 > 100 -> reject
        val ctx = makeContext(mapOf("s1" to BigDecimal("95")))
        val target = SiteTarget("s1", BigDecimal("100"))

        assertNull(matcher.findCandidate(index, null, ctx, target))
    }

    @Test
    fun `QtyCandidateMatcher accepts candidate within capacity`() {
        val candidates = listOf(CandidateProduct("p1", "bom-A", 5))
        val index = CandidateIndex(candidates)
        val matcher = QtyCandidateMatcher()
        // site at 90, target=100, candidate qty=5 -> 95 <= 100 -> accept
        val ctx = makeContext(mapOf("s1" to BigDecimal("90")))
        val target = SiteTarget("s1", BigDecimal("100"))

        assertEquals(0, matcher.findCandidate(index, null, ctx, target))
    }

    @Test
    fun `QtyCandidateMatcher skips first candidate and finds second`() {
        val candidates = listOf(
            CandidateProduct("p1", "bom-A", 10), // too big
            CandidateProduct("p2", "bom-A", 3),  // fits
        )
        val index = CandidateIndex(candidates)
        val matcher = QtyCandidateMatcher()
        val ctx = makeContext(mapOf("s1" to BigDecimal("95")))
        val target = SiteTarget("s1", BigDecimal("100"))

        assertEquals(1, matcher.findCandidate(index, null, ctx, target))
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="CandidateMatcherTest" -pl .`
Expected: compilation failure

- [ ] **Step 3: Implement CandidateMatcher**

```kotlin
package com.workflow.dispatch.algorithm

import com.workflow.dispatch.model.SiteTarget
import com.workflow.dispatch.simulation.CandidateIndex
import com.workflow.dispatch.simulation.SimulationContext
import java.math.BigDecimal

interface CandidateMatcher {
    fun findCandidate(
        index: CandidateIndex,
        sourceBomConstraint: String?,
        context: SimulationContext,
        siteTarget: SiteTarget,
    ): Int?
}

class DefaultCandidateMatcher : CandidateMatcher {
    override fun findCandidate(
        index: CandidateIndex,
        sourceBomConstraint: String?,
        context: SimulationContext,
        siteTarget: SiteTarget,
    ): Int? = index.findFirst(sourceBomConstraint)
}

class QtyCandidateMatcher : CandidateMatcher {
    override fun findCandidate(
        index: CandidateIndex,
        sourceBomConstraint: String?,
        context: SimulationContext,
        siteTarget: SiteTarget,
    ): Int? {
        val currentQty = context.siteCurrents[siteTarget.siteId] ?: BigDecimal.ZERO
        return index.findFirst(sourceBomConstraint) { candidate ->
            currentQty + candidate.qty.toBigDecimal() <= siteTarget.target
        }
    }
}
```

- [ ] **Step 4: Implement TerminationStrategy**

```kotlin
package com.workflow.dispatch.algorithm

import com.workflow.dispatch.simulation.SimulationContext

enum class TerminationDecision { STOP, SKIP_SITE }

interface TerminationStrategy {
    fun onNoCandidate(
        siteId: String,
        targetBomId: String?,
        context: SimulationContext,
    ): TerminationDecision
}

class FailFastTermination : TerminationStrategy {
    override fun onNoCandidate(
        siteId: String,
        targetBomId: String?,
        context: SimulationContext,
    ): TerminationDecision = TerminationDecision.STOP
}
```

- [ ] **Step 5: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="CandidateMatcherTest" -pl .`
Expected: all 5 tests pass

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/dispatch/algorithm/CandidateMatcher.kt src/main/kotlin/dispatch/algorithm/TerminationStrategy.kt src/test/kotlin/dispatch/algorithm/CandidateMatcherTest.kt
git commit -m "feat(dispatch): add candidate matcher and termination strategy"
```

---

### Task 6: DispatchAlgorithm + DSL + Factory

**Files:**
- Create: `src/main/kotlin/dispatch/algorithm/DispatchAlgorithm.kt`
- Create: `src/main/kotlin/dispatch/algorithm/DispatchAlgorithmDsl.kt`
- Create: `src/main/kotlin/dispatch/algorithm/DispatchAlgorithmFactory.kt`
- Test: `src/test/kotlin/dispatch/algorithm/DispatchAlgorithmTest.kt`
- Test: `src/test/kotlin/dispatch/algorithm/DispatchAlgorithmDslTest.kt`

- [ ] **Step 1: Write failing tests for DispatchAlgorithm**

```kotlin
package com.workflow.dispatch.algorithm

import com.workflow.dispatch.model.BomMapping
import com.workflow.dispatch.model.SiteBomKey
import com.workflow.dispatch.model.SiteTarget
import com.workflow.dispatch.model.TargetBomAllocation
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

class DispatchAlgorithmTest {

    private fun qtyAlgorithm(): DefaultDispatchAlgorithm = DefaultDispatchAlgorithm(
        gapComputer = QtyGapComputer(),
        candidateMatcher = QtyCandidateMatcher(),
        terminationStrategy = FailFastTermination(),
    )

    @Test
    fun `lv1 only selects site with lowest gap`() {
        val algo = qtyAlgorithm()
        val targets = listOf(
            SiteTarget("A", BigDecimal("100")),
            SiteTarget("B", BigDecimal("100")),
        )
        // A is at 80, B is at 60 -> B has lower gap (-40 vs -20)
        val currents = mapOf("A" to BigDecimal("80"), "B" to BigDecimal("60"))

        val result = algo.selectTarget(
            targets, currents, null, emptyMap(), null, null, BigDecimal("140"),
        )

        assertIs<TargetSelection.Selected>(result)
        assertEquals("B", result.siteId)
        assertNull(result.targetBomId)
        assertNull(result.sourceBomConstraint)
    }

    @Test
    fun `lv2 selects site and targetBomId`() {
        val algo = qtyAlgorithm()
        val targets = listOf(SiteTarget("A", BigDecimal("100")))
        val currents = mapOf("A" to BigDecimal("50"))
        val bomMappings = mapOf(
            "A" to BomMapping(
                sourceBomId = "src-bom-1",
                targetAllocations = listOf(
                    TargetBomAllocation("tgt-1", BigDecimal("60")),
                    TargetBomAllocation("tgt-2", BigDecimal("40")),
                ),
            ),
        )
        val bomCurrents = mapOf(
            SiteBomKey("A", "tgt-1") to BigDecimal("50"),
            SiteBomKey("A", "tgt-2") to BigDecimal("0"),
        )

        val result = algo.selectTarget(
            targets, currents, bomMappings, bomCurrents, null, null, BigDecimal("50"),
        )

        assertIs<TargetSelection.Selected>(result)
        assertEquals("A", result.siteId)
        assertEquals("tgt-2", result.targetBomId) // tgt-2 has gap -40, tgt-1 has gap -10
        assertEquals("src-bom-1", result.sourceBomConstraint)
    }

    @Test
    fun `returns NoTarget when no sites`() {
        val algo = qtyAlgorithm()
        val result = algo.selectTarget(
            emptyList(), emptyMap(), null, emptyMap(), null, null, BigDecimal.ZERO,
        )
        assertIs<TargetSelection.NoTarget>(result)
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchAlgorithmTest" -pl .`
Expected: compilation failure

- [ ] **Step 3: Implement DispatchAlgorithm**

```kotlin
package com.workflow.dispatch.algorithm

import com.workflow.dispatch.model.BomMapping
import com.workflow.dispatch.model.SiteBomKey
import com.workflow.dispatch.model.SiteTarget
import java.math.BigDecimal

sealed interface TargetSelection {
    data class Selected(
        val siteId: String,
        val targetBomId: String?,
        val sourceBomConstraint: String?,
        val siteGap: BigDecimal,
        val bomGap: BigDecimal?,
    ) : TargetSelection

    data object NoTarget : TargetSelection
}

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

class DefaultDispatchAlgorithm(
    private val gapComputer: GapComputer,
    override val candidateMatcher: CandidateMatcher,
    override val terminationStrategy: TerminationStrategy,
) : DispatchAlgorithm {

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
            SelectionEntry(st.siteId, gapComputer.computeGap(current, st.target, total), st.target)
        }
        val siteId = selectByGap(siteEntries, lastSiteId) ?: return TargetSelection.NoTarget
        val siteGap = siteEntries.first { it.id == siteId }.gap

        val bomMapping = bomMappings?.get(siteId)
            ?: return TargetSelection.Selected(siteId, null, null, siteGap, null)

        val bomTotal = siteCurrents[siteId] ?: BigDecimal.ZERO
        val bomEntries = bomMapping.targetAllocations.map { alloc ->
            val bomCurrent = bomCurrents[SiteBomKey(siteId, alloc.targetBomId)] ?: BigDecimal.ZERO
            SelectionEntry(alloc.targetBomId, gapComputer.computeGap(bomCurrent, alloc.target, bomTotal), alloc.target)
        }
        val targetBomId = selectByGap(bomEntries, lastBomId) ?: return TargetSelection.NoTarget
        val bomGap = bomEntries.first { it.id == targetBomId }.gap

        return TargetSelection.Selected(siteId, targetBomId, bomMapping.sourceBomId, siteGap, bomGap)
    }
}
```

- [ ] **Step 4: Run algorithm tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchAlgorithmTest" -pl .`
Expected: all 3 tests pass

- [ ] **Step 5: Write failing tests for DSL**

```kotlin
package com.workflow.dispatch.algorithm

import com.workflow.dispatch.model.DispatchMode
import org.junit.jupiter.api.Test
import kotlin.test.assertIs

class DispatchAlgorithmDslTest {

    @Test
    fun `QTY mode creates algorithm with QtyGapComputer and QtyCandidateMatcher`() {
        val algo = dispatchAlgorithm(DispatchMode.QTY) as DefaultDispatchAlgorithm
        assertIs<QtyCandidateMatcher>(algo.candidateMatcher)
        assertIs<FailFastTermination>(algo.terminationStrategy)
    }

    @Test
    fun `RATIO mode creates algorithm with DefaultCandidateMatcher`() {
        val algo = dispatchAlgorithm(DispatchMode.RATIO) as DefaultDispatchAlgorithm
        assertIs<DefaultCandidateMatcher>(algo.candidateMatcher)
    }

    @Test
    fun `DSL allows overriding termination strategy`() {
        val algo = dispatchAlgorithm(DispatchMode.QTY) {
            terminationStrategy = object : TerminationStrategy {
                override fun onNoCandidate(siteId: String, targetBomId: String?,
                    context: com.workflow.dispatch.simulation.SimulationContext,
                ) = TerminationDecision.SKIP_SITE
            }
        } as DefaultDispatchAlgorithm
        assertIs<QtyCandidateMatcher>(algo.candidateMatcher)
        // terminationStrategy was overridden — not FailFastTermination
    }
}
```

- [ ] **Step 6: Implement DSL + Factory**

```kotlin
// DispatchAlgorithmDsl.kt
package com.workflow.dispatch.algorithm

import com.workflow.dispatch.model.DispatchMode

class AlgorithmBuilder(mode: DispatchMode) {
    var gapComputer: GapComputer = when (mode) {
        DispatchMode.QTY -> QtyGapComputer()
        DispatchMode.RATIO -> RatioGapComputer()
    }
    var candidateMatcher: CandidateMatcher = when (mode) {
        DispatchMode.QTY -> QtyCandidateMatcher()
        DispatchMode.RATIO -> DefaultCandidateMatcher()
    }
    var terminationStrategy: TerminationStrategy = FailFastTermination()
}

fun dispatchAlgorithm(
    mode: DispatchMode,
    block: AlgorithmBuilder.() -> Unit = {},
): DispatchAlgorithm {
    val builder = AlgorithmBuilder(mode).apply(block)
    return DefaultDispatchAlgorithm(
        gapComputer = builder.gapComputer,
        candidateMatcher = builder.candidateMatcher,
        terminationStrategy = builder.terminationStrategy,
    )
}
```

```kotlin
// DispatchAlgorithmFactory.kt
package com.workflow.dispatch.algorithm

import com.workflow.dispatch.model.DispatchMode
import jakarta.enterprise.context.ApplicationScoped

interface DispatchAlgorithmFactory {
    fun create(mode: DispatchMode, algorithmId: String): DispatchAlgorithm
}

@ApplicationScoped
class DefaultDispatchAlgorithmFactory : DispatchAlgorithmFactory {
    override fun create(mode: DispatchMode, algorithmId: String): DispatchAlgorithm =
        when (algorithmId) {
            "default" -> dispatchAlgorithm(mode)
            else -> throw IllegalArgumentException("Unknown algorithm: $algorithmId")
        }
}
```

- [ ] **Step 7: Run all algorithm tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchAlgorithmTest,DispatchAlgorithmDslTest" -pl .`
Expected: all 6 tests pass

- [ ] **Step 8: Commit**

```bash
git add src/main/kotlin/dispatch/algorithm/DispatchAlgorithm.kt src/main/kotlin/dispatch/algorithm/DispatchAlgorithmDsl.kt src/main/kotlin/dispatch/algorithm/DispatchAlgorithmFactory.kt src/test/kotlin/dispatch/algorithm/DispatchAlgorithmTest.kt src/test/kotlin/dispatch/algorithm/DispatchAlgorithmDslTest.kt
git commit -m "feat(dispatch): add dispatch algorithm with DSL and factory"
```

---

### Task 7: SimulationEngine

**Files:**
- Create: `src/main/kotlin/dispatch/simulation/SimulationEngine.kt`
- Test: `src/test/kotlin/dispatch/simulation/SimulationEngineTest.kt`

- [ ] **Step 1: Write failing tests for SimulationEngine**

```kotlin
package com.workflow.dispatch.simulation

import com.workflow.dispatch.algorithm.DefaultDispatchAlgorithmFactory
import com.workflow.dispatch.model.*
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SimulationEngineTest {

    private val factory = DefaultDispatchAlgorithmFactory()
    private val engine = SimulationEngine(factory)

    @Test
    fun `lv1 only QTY mode distributes to site with lowest gap`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            siteTargets = listOf(
                SiteTarget("A", BigDecimal("100")),
                SiteTarget("B", BigDecimal("100")),
            ),
            bomMappings = null,
        )
        val candidates = listOf(
            CandidateProduct("p1", "bom1", 10),
            CandidateProduct("p2", "bom1", 10),
        )
        val baseline = Baseline(
            siteAllocations = mapOf("A" to BigDecimal("80"), "B" to BigDecimal("60")),
            bomAllocations = emptyMap(),
        )

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(2, result.decisions.size)
        // B is behind (-40 gap), so first product goes to B
        assertEquals("B", result.decisions[0].targetSiteId)
        // After B gets +10: A gap=-20, B gap=-30 -> B still behind
        assertEquals("B", result.decisions[1].targetSiteId)
    }

    @Test
    fun `empty candidates produces empty result`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            siteTargets = listOf(SiteTarget("A", BigDecimal("100"))),
            bomMappings = null,
        )

        val result = engine.simulate(config, emptyList(), Baseline(emptyMap(), emptyMap()))

        assertTrue(result.decisions.isEmpty())
    }

    @Test
    fun `QTY mode stops when candidate exceeds site capacity`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            siteTargets = listOf(SiteTarget("A", BigDecimal("10"))),
            bomMappings = null,
        )
        val candidates = listOf(
            CandidateProduct("p1", "bom1", 5),
            CandidateProduct("p2", "bom1", 5),
            CandidateProduct("p3", "bom1", 5), // would exceed 10
        )
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(2, result.decisions.size)
        assertEquals(BigDecimal("10"), result.finalSiteAllocations["A"])
    }

    @Test
    fun `lv2 BOM mapping constrains sourceBomId`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            siteTargets = listOf(SiteTarget("A", BigDecimal("100"))),
            bomMappings = mapOf(
                "A" to BomMapping(
                    sourceBomId = "src-1",
                    targetAllocations = listOf(TargetBomAllocation("tgt-1", BigDecimal("100"))),
                ),
            ),
        )
        val candidates = listOf(
            CandidateProduct("p1", "src-1", 5),
            CandidateProduct("p2", "other-bom", 5), // won't match constraint
            CandidateProduct("p3", "src-1", 5),
        )
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        // p2 is skipped because sourceBom doesn't match — terminates (fail-fast)
        // Actually: p1 dispatched, then algorithm selects A again, matcher finds p2 but
        // sourceBomConstraint="src-1" so p2 is skipped, p3 is found
        assertEquals(2, result.decisions.size)
        assertEquals("p1", result.decisions[0].productId)
        assertEquals("p3", result.decisions[1].productId)
    }

    @Test
    fun `dispatch order is 1-based sequential`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            siteTargets = listOf(SiteTarget("A", BigDecimal("100"))),
            bomMappings = null,
        )
        val candidates = listOf(
            CandidateProduct("p1", "bom1", 1),
            CandidateProduct("p2", "bom1", 1),
            CandidateProduct("p3", "bom1", 1),
        )
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(listOf(1, 2, 3), result.decisions.map { it.dispatchOrder })
    }

    @Test
    fun `RATIO mode distributes proportionally`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.RATIO, algorithmId = "default",
            siteTargets = listOf(
                SiteTarget("A", BigDecimal("60")), // 60%
                SiteTarget("B", BigDecimal("40")), // 40%
            ),
            bomMappings = null,
        )
        // 10 candidates, each qty=1
        val candidates = (1..10).map { CandidateProduct("p$it", "bom1", 1) }
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(10, result.decisions.size)
        val aCt = result.decisions.count { it.targetSiteId == "A" }
        val bCt = result.decisions.count { it.targetSiteId == "B" }
        // Should be approximately 6A/4B
        assertEquals(6, aCt)
        assertEquals(4, bCt)
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SimulationEngineTest" -pl .`
Expected: compilation failure

- [ ] **Step 3: Implement SimulationEngine**

```kotlin
package com.workflow.dispatch.simulation

import com.workflow.dispatch.algorithm.DispatchAlgorithmFactory
import com.workflow.dispatch.algorithm.TargetSelection
import com.workflow.dispatch.algorithm.TerminationDecision
import com.workflow.dispatch.model.*
import jakarta.enterprise.context.ApplicationScoped
import java.math.BigDecimal

@ApplicationScoped
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
            siteCurrents = baseline.siteAllocations.toMutableMap(),
            bomCurrents = baseline.bomAllocations.toMutableMap(),
            total = baseline.siteAllocations.values.fold(BigDecimal.ZERO, BigDecimal::add),
        )

        while (index.hasUnconsumed()) {
            val selection = algorithm.selectTarget(
                config.siteTargets, context.siteCurrents,
                config.bomMappings, context.bomCurrents,
                context.lastSiteId, context.lastBomId, context.total,
            )
            if (selection !is TargetSelection.Selected) break

            val siteTarget = config.siteTargets.first { it.siteId == selection.siteId }
            val idx = algorithm.candidateMatcher.findCandidate(
                index, selection.sourceBomConstraint, context, siteTarget,
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
}
```

- [ ] **Step 4: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SimulationEngineTest" -pl .`
Expected: all 6 tests pass

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/dispatch/simulation/SimulationEngine.kt src/test/kotlin/dispatch/simulation/SimulationEngineTest.kt
git commit -m "feat(dispatch): add simulation engine with gap-based allocation"
```

---

### Task 8: DefaultCsvFormatter + Maven Dependency

**Files:**
- Modify: `pom.xml` (add jackson-dataformat-csv)
- Create: `src/main/kotlin/dispatch/port/DefaultCsvFormatter.kt`
- Test: `src/test/kotlin/dispatch/port/DefaultCsvFormatterTest.kt`

- [ ] **Step 1: Add jackson-dataformat-csv dependency to pom.xml**

Add in the `<dependencies>` section, after the existing `jackson-dataformat-yaml`:

```xml
<dependency>
    <groupId>com.fasterxml.jackson.dataformat</groupId>
    <artifactId>jackson-dataformat-csv</artifactId>
</dependency>
```

Version is managed by the Quarkus BOM.

- [ ] **Step 2: Write failing tests**

```kotlin
package com.workflow.dispatch.port

import com.workflow.dispatch.model.DispatchDecision
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DefaultCsvFormatterTest {

    private val formatter = DefaultCsvFormatter()

    @Test
    fun `formats decisions to CSV with header row`() {
        val decisions = listOf(
            DispatchDecision(
                dispatchOrder = 1, productId = "p1", sourceBomId = "bom1", qty = 5,
                targetSiteId = "A", targetBomId = "tgt1",
                siteGap = BigDecimal("-20"), bomGap = BigDecimal("-10"),
            ),
        )

        val csv = String(formatter.format("2026-03-29T06:00:00", "cfg1", decisions))
        val lines = csv.trim().lines()

        assertEquals(2, lines.size) // header + 1 row
        assertTrue(lines[0].contains("batch_token"))
        assertTrue(lines[0].contains("dispatch_order"))
        assertTrue(lines[1].contains("p1"))
        assertTrue(lines[1].contains("2026-03-29T06:00:00"))
    }

    @Test
    fun `null targetBomId and bomGap render as empty`() {
        val decisions = listOf(
            DispatchDecision(
                dispatchOrder = 1, productId = "p1", sourceBomId = "bom1", qty = 5,
                targetSiteId = "A", targetBomId = null,
                siteGap = BigDecimal("-20"), bomGap = null,
            ),
        )

        val csv = String(formatter.format("batch1", "cfg1", decisions))
        val dataLine = csv.trim().lines()[1]
        // target_bom_id and bom_gap columns should be empty
        assertTrue(dataLine.contains(",,") || dataLine.endsWith(","))
    }

    @Test
    fun `empty decisions list produces header only`() {
        val csv = String(formatter.format("batch1", "cfg1", emptyList()))
        val lines = csv.trim().lines()
        assertEquals(1, lines.size) // header only
    }
}
```

- [ ] **Step 3: Run to verify failure**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DefaultCsvFormatterTest" -pl .`
Expected: compilation failure

- [ ] **Step 4: Implement DefaultCsvFormatter**

```kotlin
package com.workflow.dispatch.port

import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dispatch.model.DispatchDecision
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class DefaultCsvFormatter : CsvFormatter {

    private val csvMapper = CsvMapper().apply {
        registerModule(KotlinModule.Builder().build())
    }

    private val schema = CsvSchema.builder()
        .addColumn("batch_token")
        .addColumn("config_id")
        .addColumn("dispatch_order", CsvSchema.ColumnType.NUMBER)
        .addColumn("product_id")
        .addColumn("source_bom_id")
        .addColumn("qty", CsvSchema.ColumnType.NUMBER)
        .addColumn("target_site_id")
        .addColumn("target_bom_id")
        .addColumn("site_gap", CsvSchema.ColumnType.NUMBER)
        .addColumn("bom_gap", CsvSchema.ColumnType.NUMBER)
        .build()
        .withHeader()

    override fun format(
        batchToken: String,
        configId: String,
        decisions: List<DispatchDecision>,
    ): ByteArray {
        val rows = decisions.map { d ->
            mapOf(
                "batch_token" to batchToken,
                "config_id" to configId,
                "dispatch_order" to d.dispatchOrder,
                "product_id" to d.productId,
                "source_bom_id" to d.sourceBomId,
                "qty" to d.qty,
                "target_site_id" to d.targetSiteId,
                "target_bom_id" to (d.targetBomId ?: ""),
                "site_gap" to d.siteGap,
                "bom_gap" to (d.bomGap ?: ""),
            )
        }
        return csvMapper.writer(schema).writeValueAsBytes(rows)
    }
}
```

- [ ] **Step 5: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DefaultCsvFormatterTest" -pl .`
Expected: all 3 tests pass

- [ ] **Step 6: Commit**

```bash
git add pom.xml src/main/kotlin/dispatch/port/DefaultCsvFormatter.kt src/test/kotlin/dispatch/port/DefaultCsvFormatterTest.kt
git commit -m "feat(dispatch): add DefaultCsvFormatter with jackson-dataformat-csv"
```

---

### Task 9: Framework Enhancement — Idempotency Key

**Files:**
- Create: `src/main/resources/db/migration/V9__idempotency_key.sql`
- Modify: `src/main/kotlin/engine/WorkflowModels.kt` (add StartResult)
- Modify: `src/main/kotlin/engine/WorkflowRepository.kt` (add mergeIdempotent method)
- Modify: `src/main/kotlin/engine/WorkflowEngine.kt` (accept idempotencyKey, return StartResult)
- Modify: `src/test/kotlin/engine/OracleTestContainer.kt` (run V9 migration)
- Test: `src/test/kotlin/engine/IdempotencyKeyTest.kt`

**Note:** `startWorkflow` return type changes from `String` to `StartResult`. Add `.workflowId` extension to minimize caller updates.

- [ ] **Step 1: Create the V9 migration**

```sql
-- V9__idempotency_key.sql
ALTER TABLE workflow ADD idempotency_key VARCHAR2(255);
CREATE UNIQUE INDEX uk_workflow_idempotency ON workflow(idempotency_key);
```

- [ ] **Step 2: Add StartResult to WorkflowModels.kt**

Add at the end of `src/main/kotlin/engine/WorkflowModels.kt`:

```kotlin
sealed interface StartResult {
    data class Created(val workflowId: String) : StartResult
    data class AlreadyExists(val workflowId: String) : StartResult
}

val StartResult.workflowId: String
    get() = when (this) {
        is StartResult.Created -> workflowId
        is StartResult.AlreadyExists -> workflowId
    }
```

- [ ] **Step 3: Add mergeIdempotent method to WorkflowRepository**

Add to `src/main/kotlin/engine/WorkflowRepository.kt`:

```kotlin
fun mergeIdempotentWithHandle(handle: Handle, run: WorkflowRun, idempotencyKey: String): Pair<String, Boolean> {
    val count = handle.createUpdate(
        """
        MERGE INTO workflow w
        USING (SELECT :idemKey AS idem_key FROM dual) src
        ON (w.idempotency_key = src.idem_key)
        WHEN NOT MATCHED THEN INSERT
            (id, idempotency_key, definition, current_sequence, version, status, created_at, updated_at, deadline_at)
        VALUES (:id, :idemKey, :definition, :currentSequence, :version, :status, :createdAt, :updatedAt, :deadlineAt)
        """,
    )
        .bind("idemKey", idempotencyKey)
        .bind("id", run.id)
        .bind("definition", run.definitionJson)
        .bind("currentSequence", run.currentSequence)
        .bind("version", run.version)
        .bind("status", run.status.name)
        .bind("createdAt", LocalDateTime.ofInstant(run.createdAt, ZoneOffset.UTC))
        .bind("updatedAt", LocalDateTime.ofInstant(run.updatedAt, ZoneOffset.UTC))
        .bind("deadlineAt", LocalDateTime.ofInstant(run.deadlineAt, ZoneOffset.UTC))
        .execute()

    if (count == 1) return run.id to true

    val existingId = handle.createQuery("SELECT id FROM workflow WHERE idempotency_key = :key")
        .bind("key", idempotencyKey)
        .mapTo(String::class.java)
        .one()
    return existingId to false
}
```

- [ ] **Step 4: Modify WorkflowEngine.startWorkflow**

Replace the existing `startWorkflow` method in `src/main/kotlin/engine/WorkflowEngine.kt`:

```kotlin
suspend fun startWorkflow(
    definition: WorkflowDefinition,
    idempotencyKey: String? = null,
): StartResult {
    require(definition.activities.isNotEmpty()) { "WorkflowDefinition must have at least one activity" }

    val workflowId = UUID.randomUUID().toString()
    val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
    val definitionJson = objectMapper.writeValueAsString(definition)

    val run = WorkflowRun(
        id = workflowId,
        definitionJson = definitionJson,
        currentSequence = 1,
        version = 0,
        status = WorkflowStatus.RUNNING,
        createdAt = now,
        updatedAt = now,
        deadlineAt = now.plus(definition.deadline),
    )

    if (idempotencyKey == null) {
        val queueName = jdbi.inTransactionSuspend<String, Exception> { handle ->
            workflowRepo.insertWithHandle(handle, run)
            val firstActivity = definition.activities.first()
            val task = createTaskForActivity(workflowId, 1, firstActivity, now)
            taskRepo.insertBatchWithHandle(handle, listOf(task))
            firstActivity.queue
        }
        notifier.signal(queueName)
        log.info("Started workflow {} with {} activities", workflowId, definition.activities.size)
        return StartResult.Created(workflowId)
    }

    val (mergeId, created, queueName) = jdbi.inTransactionSuspend<Triple<String, Boolean, String?>, Exception> { handle ->
        val (mId, isNew) = workflowRepo.mergeIdempotentWithHandle(handle, run, idempotencyKey)
        if (isNew) {
            val firstActivity = definition.activities.first()
            val task = createTaskForActivity(mId, 1, firstActivity, now)
            taskRepo.insertBatchWithHandle(handle, listOf(task))
            Triple(mId, true, firstActivity.queue)
        } else {
            Triple(mId, false, null)
        }
    }

    if (queueName != null) {
        notifier.signal(queueName)
        log.info("Started workflow {} (idempotent, key={}) with {} activities", mergeId, idempotencyKey, definition.activities.size)
    } else {
        log.info("Workflow already exists for key {}: {}", idempotencyKey, mergeId)
    }

    return if (created) StartResult.Created(mergeId) else StartResult.AlreadyExists(mergeId)
}
```

- [ ] **Step 5: Update OracleTestContainer to run V9**

Add to `src/test/kotlin/engine/OracleTestContainer.kt`, after the V8 migration line:

```kotlin
handle.createScript(loader.getResource("db/migration/V9__idempotency_key.sql")!!.readText()).execute()
```

- [ ] **Step 6: Update all existing callers of startWorkflow**

Search for `engine.startWorkflow(` and `startWorkflow(` across test files. Replace:

```kotlin
// Before:
val workflowId = engine.startWorkflow(definition)
// After:
val workflowId = engine.startWorkflow(definition).workflowId
```

Files to update (grep for `startWorkflow` calls):
- `src/test/kotlin/engine/WorkflowEngineTest.kt`
- `src/test/kotlin/engine/WorkflowIntegrationTest.kt`
- `src/test/kotlin/engine/BarrierServiceTest.kt`
- `src/test/kotlin/engine/SweeperTest.kt`
- `src/test/kotlin/stress/StressTestBase.kt` (and subclasses)
- `src/test/kotlin/benchmark/BenchmarkHarness.kt`

- [ ] **Step 7: Write idempotency-specific tests**

```kotlin
package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dsl.workflow
import com.workflow.worker.FakeDispatchNotifier
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IdempotencyKeyTest {

    private lateinit var engine: WorkflowEngine
    private lateinit var workflowRepo: WorkflowRepository
    private lateinit var taskRepo: TaskRepository
    private lateinit var notifier: FakeDispatchNotifier
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())
    private val jdbi = OracleTestContainer.jdbi

    @BeforeAll
    fun setup() {
        workflowRepo = WorkflowRepository(jdbi)
        taskRepo = TaskRepository(jdbi)
        notifier = FakeDispatchNotifier()
        engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
    }

    @AfterEach
    fun cleanup() {
        jdbi.useHandle<Exception> { h ->
            h.execute("DELETE FROM task")
            h.execute("DELETE FROM workflow")
        }
    }

    private val definition = workflow {
        activity("step1") { transition("test-handler") }
    }

    @Test
    fun `first call with idempotencyKey returns Created`() = runTest {
        val result = engine.startWorkflow(definition, "test-key-1")
        assertIs<StartResult.Created>(result)
    }

    @Test
    fun `second call with same key returns AlreadyExists with same workflowId`() = runTest {
        val first = engine.startWorkflow(definition, "test-key-2")
        val second = engine.startWorkflow(definition, "test-key-2")

        assertIs<StartResult.Created>(first)
        assertIs<StartResult.AlreadyExists>(second)
        assertEquals(first.workflowId, second.workflowId)
    }

    @Test
    fun `different keys create different workflows`() = runTest {
        val r1 = engine.startWorkflow(definition, "key-A")
        val r2 = engine.startWorkflow(definition, "key-B")

        assertIs<StartResult.Created>(r1)
        assertIs<StartResult.Created>(r2)
        assertNotEquals(r1.workflowId, r2.workflowId)
    }

    @Test
    fun `null idempotencyKey always creates`() = runTest {
        val r1 = engine.startWorkflow(definition)
        val r2 = engine.startWorkflow(definition)

        assertIs<StartResult.Created>(r1)
        assertIs<StartResult.Created>(r2)
        assertNotEquals(r1.workflowId, r2.workflowId)
    }
}
```

- [ ] **Step 8: Run idempotency tests (requires Docker for Oracle container)**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="IdempotencyKeyTest" -pl .`
Expected: all 4 tests pass

- [ ] **Step 9: Run full existing test suite to verify no regressions**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl .`
Expected: all tests pass (existing callers updated in step 6)

- [ ] **Step 10: Commit**

```bash
git add src/main/resources/db/migration/V9__idempotency_key.sql src/main/kotlin/engine/WorkflowModels.kt src/main/kotlin/engine/WorkflowRepository.kt src/main/kotlin/engine/WorkflowEngine.kt src/test/kotlin/engine/OracleTestContainer.kt src/test/kotlin/engine/IdempotencyKeyTest.kt
git commit -m "feat(engine): add idempotency key support for workflow deduplication"
```

Note: also `git add` any test files modified in step 6.

---

### Task 10: S3 Storage Adapter + Configuration

**Files:**
- Modify: `pom.xml` (add AWS SDK S3)
- Create: `src/main/kotlin/dispatch/adapter/S3ClientProducer.kt`
- Create: `src/main/kotlin/dispatch/adapter/S3StorageAdapter.kt`
- Modify: `src/main/resources/application.properties` (add storage config)
- Test: `src/test/kotlin/dispatch/adapter/S3StorageAdapterTest.kt`

- [ ] **Step 1: Add AWS SDK S3 dependencies to pom.xml**

Add a property:
```xml
<aws.sdk.version>2.29.51</aws.sdk.version>
```

Add to `<dependencyManagement>` section (before the Quarkus BOM import):
```xml
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>bom</artifactId>
    <version>${aws.sdk.version}</version>
    <type>pom</type>
    <scope>import</scope>
</dependency>
```

Add to `<dependencies>`:
```xml
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>s3</artifactId>
</dependency>
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>netty-nio-client</artifactId>
</dependency>
```

- [ ] **Step 2: Add storage configuration to application.properties**

```properties
# =============================================================================
# Storage (MinIO/S3)
# =============================================================================
storage.endpoint=${STORAGE_ENDPOINT:http://localhost:9000}
storage.region=${STORAGE_REGION:us-east-1}
storage.bucket=${STORAGE_BUCKET:dispatch}
storage.access-key=${STORAGE_ACCESS_KEY:minioadmin}
storage.secret-key=${STORAGE_SECRET_KEY:minioadmin}
```

- [ ] **Step 3: Implement S3ClientProducer**

```kotlin
package com.workflow.dispatch.adapter

import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.eclipse.microprofile.config.inject.ConfigProperty
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import java.net.URI

@ApplicationScoped
class S3ClientProducer {

    @Produces
    @ApplicationScoped
    fun s3AsyncClient(
        @ConfigProperty(name = "storage.endpoint") endpoint: String,
        @ConfigProperty(name = "storage.region") region: String,
        @ConfigProperty(name = "storage.access-key") accessKey: String,
        @ConfigProperty(name = "storage.secret-key") secretKey: String,
    ): S3AsyncClient = S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.of(region))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)),
        )
        .forcePathStyleAccess(true)
        .build()
}
```

- [ ] **Step 4: Implement S3StorageAdapter**

```kotlin
package com.workflow.dispatch.adapter

import com.workflow.dispatch.port.StoragePort
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.future.await
import org.eclipse.microprofile.config.inject.ConfigProperty
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest

@ApplicationScoped
class S3StorageAdapter(
    private val client: S3AsyncClient,
    @ConfigProperty(name = "storage.bucket") private val bucket: String,
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
        ).await()
    }
}
```

- [ ] **Step 5: Write tests for S3StorageAdapter**

```kotlin
package com.workflow.dispatch.adapter

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import java.util.concurrent.CompletableFuture
import kotlin.test.assertEquals

class S3StorageAdapterTest {

    private val mockClient = mock<S3AsyncClient>()
    private val adapter = S3StorageAdapter(mockClient, "test-bucket")

    @Test
    fun `uploadCsv calls putObject with csv content type`() = runTest {
        whenever(mockClient.putObject(any<PutObjectRequest>(), any<AsyncRequestBody>()))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()))

        adapter.uploadCsv("path/to/file.csv", "data".toByteArray())

        val requestCaptor = argumentCaptor<PutObjectRequest>()
        verify(mockClient).putObject(requestCaptor.capture(), any<AsyncRequestBody>())

        assertEquals("test-bucket", requestCaptor.firstValue.bucket())
        assertEquals("path/to/file.csv", requestCaptor.firstValue.key())
        assertEquals("text/csv", requestCaptor.firstValue.contentType())
    }

    @Test
    fun `uploadParquet calls putObject with octet-stream content type`() = runTest {
        whenever(mockClient.putObject(any<PutObjectRequest>(), any<AsyncRequestBody>()))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()))

        adapter.uploadParquet("path/to/file.parquet", byteArrayOf(1, 2, 3))

        val requestCaptor = argumentCaptor<PutObjectRequest>()
        verify(mockClient).putObject(requestCaptor.capture(), any<AsyncRequestBody>())

        assertEquals("application/octet-stream", requestCaptor.firstValue.contentType())
    }
}
```

- [ ] **Step 6: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="S3StorageAdapterTest" -pl .`
Expected: all 2 tests pass

- [ ] **Step 7: Commit**

```bash
git add pom.xml src/main/resources/application.properties src/main/kotlin/dispatch/adapter/S3ClientProducer.kt src/main/kotlin/dispatch/adapter/S3StorageAdapter.kt src/test/kotlin/dispatch/adapter/S3StorageAdapterTest.kt
git commit -m "feat(dispatch): add S3 storage adapter with MinIO support"
```

---

### Task 11: Workflow Handlers + Definition + Scheduler

**Files:**
- Create: `src/main/kotlin/dispatch/handler/DispatchWorkflow.kt`
- Create: `src/main/kotlin/dispatch/handler/DispatchScatterHandler.kt`
- Create: `src/main/kotlin/dispatch/handler/DispatchSimulationHandler.kt`
- Create: `src/main/kotlin/dispatch/handler/DispatchJoinHandler.kt`
- Create: `src/main/kotlin/dispatch/handler/DispatchScheduler.kt`
- Test: `src/test/kotlin/dispatch/handler/DispatchHandlersTest.kt`

**Design note:** The existing DSL's `fanOut` takes only a target name (not a nested builder). The workflow definition uses the existing pattern with separate activities. The join handler resolves `batchToken` from the simulate activity's aggregated outputs since the scatter result is a JSON array consumed by fan-out.

- [ ] **Step 1: Create workflow definition**

```kotlin
package com.workflow.dispatch.handler

import com.workflow.dsl.JoinPolicy
import com.workflow.dsl.WorkflowDefinition
import com.workflow.dsl.workflow
import java.time.Duration

val dispatchWorkflow: WorkflowDefinition = workflow {
    deadline(Duration.ofHours(2))

    activity("scatter") {
        transition("dispatch.scatter")
        fanOut("simulate")
    }

    activity("simulate") {
        transition("dispatch.simulate")
        retries(2)
        deadline(Duration.ofMinutes(30))
        joinPolicy(JoinPolicy.All)
    }

    activity("join") {
        transition("dispatch.join")
        deadline(Duration.ofMinutes(10))
        inputs {
            "batchToken" from "simulate.batchToken"
        }
    }
}
```

- [ ] **Step 2: Create scatter handler**

```kotlin
package com.workflow.dispatch.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.port.DispatchConfigRepository
import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import jakarta.enterprise.context.ApplicationScoped
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

@ApplicationScoped
class DispatchScatterHandler(
    private val configRepo: DispatchConfigRepository,
    private val objectMapper: ObjectMapper,
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

- [ ] **Step 3: Create simulation handler**

```kotlin
package com.workflow.dispatch.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.port.BaselineProvider
import com.workflow.dispatch.port.CandidateQueryPort
import com.workflow.dispatch.port.CsvFormatter
import com.workflow.dispatch.port.DispatchConfigRepository
import com.workflow.dispatch.port.SimulationResultStore
import com.workflow.dispatch.port.StoragePort
import com.workflow.dispatch.simulation.SimulationEngine
import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class DispatchSimulationHandler(
    private val configRepo: DispatchConfigRepository,
    private val candidateQuery: CandidateQueryPort,
    private val baselineProvider: BaselineProvider,
    private val simulationEngine: SimulationEngine,
    private val resultStore: SimulationResultStore,
    private val storage: StoragePort,
    private val csvFormatter: CsvFormatter,
    private val objectMapper: ObjectMapper,
) : TransitionHandler {

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val item = objectMapper.readTree(input.item!!)
        val configId = item["configId"].asText()
        val batchToken = item["batchToken"].asText()

        val config = configRepo.findById(configId)

        val result = simulationEngine.simulate(
            config = config,
            candidates = candidateQuery.queryCandidates(config),
            baseline = baselineProvider.loadBaseline(config),
        )

        resultStore.saveDecisions(batchToken, configId, result.decisions)

        val csv = csvFormatter.format(batchToken, configId, result.decisions)
        storage.uploadCsv("dispatch/$batchToken/simulation/$configId.csv", csv)

        return HandlerOutput(
            objectMapper.writeValueAsString(
                mapOf("configId" to configId, "batchToken" to batchToken),
            ),
        )
    }
}
```

- [ ] **Step 4: Create join handler**

```kotlin
package com.workflow.dispatch.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.port.ParquetFormatter
import com.workflow.dispatch.port.SimulationResultStore
import com.workflow.dispatch.port.StoragePort
import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class DispatchJoinHandler(
    private val resultStore: SimulationResultStore,
    private val storage: StoragePort,
    private val parquetFormatter: ParquetFormatter,
    private val objectMapper: ObjectMapper,
) : TransitionHandler {

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val inputsNode = objectMapper.readTree(input.inputs!!)
        val batchTokenNode = inputsNode["batchToken"]
        // InputResolver aggregates parallel task outputs into an array
        val batchToken = if (batchTokenNode.isArray) {
            batchTokenNode[0].asText()
        } else {
            batchTokenNode.asText()
        }

        val allDecisions = resultStore.findByBatchToken(batchToken)
        val parquet = parquetFormatter.format(allDecisions)
        storage.uploadParquet("dispatch/$batchToken/result.parquet", parquet)

        return HandlerOutput(null)
    }
}
```

- [ ] **Step 5: Create scheduler**

```kotlin
package com.workflow.dispatch.handler

import com.workflow.engine.WorkflowEngine
import com.workflow.engine.workflowId
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

@ApplicationScoped
class DispatchScheduler(
    private val workflowEngine: WorkflowEngine,
) {
    private val log = LoggerFactory.getLogger(DispatchScheduler::class.java)

    // Quarkus @Scheduled cron — configured via application.properties
    // dispatch.cron = 0 0 0,6,12,18 * * ?  (4x/day)
    @io.quarkus.scheduler.Scheduled(cron = "{dispatch.cron}")
    fun trigger() {
        val batchToken = LocalDateTime.now()
            .truncatedTo(ChronoUnit.HOURS)
            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

        runBlocking {
            val result = workflowEngine.startWorkflow(
                definition = dispatchWorkflow,
                idempotencyKey = "dispatch-$batchToken",
            )
            log.info("Dispatch trigger: batchToken={}, result={}", batchToken, result)
        }
    }
}
```

Add cron config to `src/main/resources/application.properties`:
```properties
# =============================================================================
# Dispatch Scheduling
# =============================================================================
dispatch.cron=${DISPATCH_CRON:0 0 0,6,12,18 * * ?}
```

- [ ] **Step 6: Write handler tests**

```kotlin
package com.workflow.dispatch.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dispatch.model.*
import com.workflow.dispatch.port.*
import com.workflow.dispatch.simulation.SimulationEngine
import com.workflow.worker.HandlerInput
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import java.math.BigDecimal
import java.time.LocalDateTime
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class DispatchHandlersTest {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())

    @Test
    fun `scatter handler returns JSON array of config items`() = runTest {
        val configRepo = mock<DispatchConfigRepository>()
        val config = DispatchConfig("cfg1", DispatchMode.QTY, "default",
            listOf(SiteTarget("A", BigDecimal("100"))), null)
        whenever(configRepo.findActiveConfigs(any())).thenReturn(listOf(config))

        val handler = DispatchScatterHandler(configRepo, objectMapper)
        val output = handler.execute(
            HandlerInput("t1", "w1", 1, null, null),
        )

        assertNotNull(output.result)
        val arr = objectMapper.readTree(output.result)
        assertTrue(arr.isArray)
        assertTrue(arr[0].has("configId"))
        assertTrue(arr[0].has("batchToken"))
    }

    @Test
    fun `simulation handler calls engine and uploads CSV`() = runTest {
        val configRepo = mock<DispatchConfigRepository>()
        val candidateQuery = mock<CandidateQueryPort>()
        val baselineProvider = mock<BaselineProvider>()
        val simulationEngine = mock<SimulationEngine>()
        val resultStore = mock<SimulationResultStore>()
        val storage = mock<StoragePort>()
        val csvFormatter = mock<CsvFormatter>()

        val config = DispatchConfig("cfg1", DispatchMode.QTY, "default",
            listOf(SiteTarget("A", BigDecimal("100"))), null)
        whenever(configRepo.findById("cfg1")).thenReturn(config)
        whenever(candidateQuery.queryCandidates(config)).thenReturn(emptyList())
        whenever(baselineProvider.loadBaseline(config)).thenReturn(Baseline(emptyMap(), emptyMap()))
        whenever(simulationEngine.simulate(eq(config), any(), any())).thenReturn(
            SimulationResult(emptyList(), emptyMap(), emptyMap()),
        )
        whenever(csvFormatter.format(any(), any(), any())).thenReturn(byteArrayOf())

        val handler = DispatchSimulationHandler(
            configRepo, candidateQuery, baselineProvider, simulationEngine,
            resultStore, storage, csvFormatter, objectMapper,
        )

        val item = objectMapper.writeValueAsString(mapOf("configId" to "cfg1", "batchToken" to "2026-03-29T06:00:00"))
        val output = handler.execute(
            HandlerInput("t1", "w1", 2, null, item),
        )

        verify(resultStore).saveDecisions(eq("2026-03-29T06:00:00"), eq("cfg1"), any())
        verify(storage).uploadCsv(eq("dispatch/2026-03-29T06:00:00/simulation/cfg1.csv"), any())
        assertNotNull(output.result)
    }

    @Test
    fun `join handler uploads parquet with merged results`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val storage = mock<StoragePort>()
        val parquetFormatter = mock<ParquetFormatter>()

        whenever(resultStore.findByBatchToken("2026-03-29T06:00:00")).thenReturn(emptyList())
        whenever(parquetFormatter.format(any())).thenReturn(byteArrayOf())

        val handler = DispatchJoinHandler(resultStore, storage, parquetFormatter, objectMapper)

        // Simulate aggregated input from parallel simulate tasks
        val inputs = objectMapper.writeValueAsString(
            mapOf("batchToken" to listOf("2026-03-29T06:00:00", "2026-03-29T06:00:00")),
        )
        handler.execute(
            HandlerInput("t1", "w1", 3, inputs, null),
        )

        verify(storage).uploadParquet(eq("dispatch/2026-03-29T06:00:00/result.parquet"), any())
    }
}
```

- [ ] **Step 7: Run handler tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchHandlersTest" -pl .`
Expected: all 3 tests pass

- [ ] **Step 8: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl .`
Expected: all tests pass

- [ ] **Step 9: Commit**

```bash
git add src/main/kotlin/dispatch/handler/ src/test/kotlin/dispatch/handler/ src/main/resources/application.properties
git commit -m "feat(dispatch): add workflow handlers, definition, and scheduler"
```
