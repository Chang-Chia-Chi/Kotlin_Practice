# Naming Convention Overhaul Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename 19 classes to follow the domain-intent naming convention defined in `docs/superpowers/specs/2026-03-31-naming-convention-design.md`.

**Architecture:** Pure mechanical rename — no behavioral changes. Each task renames files, updates class names, and fixes all imports/references. Grouped by domain to keep related changes together and enable incremental compilation checks.

**Tech Stack:** Kotlin, git mv, sed, Maven

**Key constraint:** Replacement order matters when one name is a substring of another. Each task notes ordering requirements.

---

### Task 1: Advancement Strategy Family (4 renames)

Renames `PhaseStrategy` → `AdvancementStrategy` and all derivatives. Since `PhaseStrategy` is a substring of all four names, a single sed handles the cascade.

**Files:**
- Rename: `src/main/kotlin/workflow/usecase/port/inbound/phase/PhaseStrategy.kt` → `AdvancementStrategy.kt`
- Rename: `src/main/kotlin/workflow/usecase/service/phase/LinearPhaseStrategy.kt` → `LinearAdvancementStrategy.kt`
- Rename: `src/main/kotlin/workflow/usecase/service/phase/ParallelPhaseStrategy.kt` → `ParallelAdvancementStrategy.kt`
- Rename: `src/main/kotlin/workflow/usecase/service/phase/PhaseStrategyRegistry.kt` → `AdvancementStrategyRegistry.kt`
- Rename: `src/test/kotlin/workflow/usecase/service/phase/LinearPhaseStrategyTest.kt` → `LinearAdvancementStrategyTest.kt`
- Rename: `src/test/kotlin/workflow/usecase/service/phase/ParallelPhaseStrategyTest.kt` → `ParallelAdvancementStrategyTest.kt`
- Rename: `src/test/kotlin/workflow/usecase/service/phase/PhaseStrategyRegistryTest.kt` → `AdvancementStrategyRegistryTest.kt`
- Modify (via sed cascade): All .kt files referencing `PhaseStrategy`

- [ ] **Step 1: Rename source files**

```bash
cd src/main/kotlin/workflow/usecase
git mv port/inbound/phase/PhaseStrategy.kt port/inbound/phase/AdvancementStrategy.kt
git mv service/phase/LinearPhaseStrategy.kt service/phase/LinearAdvancementStrategy.kt
git mv service/phase/ParallelPhaseStrategy.kt service/phase/ParallelAdvancementStrategy.kt
git mv service/phase/PhaseStrategyRegistry.kt service/phase/AdvancementStrategyRegistry.kt
```

- [ ] **Step 2: Rename test files**

```bash
cd src/test/kotlin/workflow/usecase/service/phase
git mv LinearPhaseStrategyTest.kt LinearAdvancementStrategyTest.kt
git mv ParallelPhaseStrategyTest.kt ParallelAdvancementStrategyTest.kt
git mv PhaseStrategyRegistryTest.kt AdvancementStrategyRegistryTest.kt
```

- [ ] **Step 3: Replace class name in all .kt files**

Single replace cascades to all four names (`PhaseStrategy` → `AdvancementStrategy` also transforms `LinearPhaseStrategy` → `LinearAdvancementStrategy`, etc.):

```bash
find src -name '*.kt' -exec sed -i 's/PhaseStrategy/AdvancementStrategy/g' {} +
```

- [ ] **Step 4: Compile**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -q
```

Expected: BUILD SUCCESS

- [ ] **Step 5: Run affected tests**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="LinearAdvancementStrategyTest,ParallelAdvancementStrategyTest,AdvancementStrategyRegistryTest,BarrierServiceTest" -pl WorkFlow
```

Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "rename: PhaseStrategy → AdvancementStrategy family"
```

---

### Task 2: Phase Gate + Workflow Lifecycle (3 renames)

Renames `BarrierOperations` → `PhaseGate`, `BarrierService` → `DefaultPhaseGate`, `WorkflowOperations` → `WorkflowLifecycle`. No substring conflicts between these.

**Files:**
- Rename: `src/main/kotlin/workflow/usecase/port/inbound/orchestration/BarrierOperations.kt` → `PhaseGate.kt`
- Rename: `src/main/kotlin/workflow/usecase/port/inbound/orchestration/WorkflowOperations.kt` → `WorkflowLifecycle.kt`
- Rename: `src/main/kotlin/workflow/usecase/service/orchestration/BarrierService.kt` → `DefaultPhaseGate.kt`
- Rename: `src/test/kotlin/workflow/usecase/service/orchestration/BarrierServiceTest.kt` → `DefaultPhaseGateTest.kt`
- Modify (via sed): All .kt files referencing these names

- [ ] **Step 1: Rename source files**

```bash
cd src/main/kotlin/workflow/usecase
git mv port/inbound/orchestration/BarrierOperations.kt port/inbound/orchestration/PhaseGate.kt
git mv port/inbound/orchestration/WorkflowOperations.kt port/inbound/orchestration/WorkflowLifecycle.kt
git mv service/orchestration/BarrierService.kt service/orchestration/DefaultPhaseGate.kt
```

- [ ] **Step 2: Rename test file**

```bash
cd src/test/kotlin/workflow/usecase/service/orchestration
git mv BarrierServiceTest.kt DefaultPhaseGateTest.kt
```

- [ ] **Step 3: Replace class names in all .kt files**

Order does not matter — no substring conflicts:

```bash
find src -name '*.kt' -exec sed -i 's/BarrierOperations/PhaseGate/g' {} +
find src -name '*.kt' -exec sed -i 's/BarrierService/DefaultPhaseGate/g' {} +
find src -name '*.kt' -exec sed -i 's/WorkflowOperations/WorkflowLifecycle/g' {} +
```

- [ ] **Step 4: Fix variable names for readability**

Variables named `barrierService` or `barrierOperations` should follow the new class names:

```bash
find src -name '*.kt' -exec sed -i 's/barrierService/phaseGate/g' {} +
find src -name '*.kt' -exec sed -i 's/barrierOperations/phaseGate/g' {} +
```

- [ ] **Step 5: Compile**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -q
```

Expected: BUILD SUCCESS

- [ ] **Step 6: Run affected tests**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DefaultPhaseGateTest,WorkflowEngineTest,SweeperTest,WorkerLoopTest,WorkflowIntegrationTest" -pl WorkFlow
```

Expected: All tests PASS

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "rename: BarrierOperations → PhaseGate, BarrierService → DefaultPhaseGate, WorkflowOperations → WorkflowLifecycle"
```

---

### Task 3: Watchdog + Activity Input Resolver (3 renames)

Renames `Sweeper` → `WorkflowWatchdog`, `SweeperConfig` → `WatchdogConfig`, `InputResolver` → `ActivityInputResolver`.

**Ordering constraint:** Replace `SweeperConfig` before `Sweeper` to prevent `SweeperConfig` from becoming `WorkflowWatchdogConfig`.

**Files:**
- Rename: `src/main/kotlin/workflow/usecase/service/orchestration/Sweeper.kt` → `WorkflowWatchdog.kt`
- Rename: `src/main/kotlin/workflow/config/SweeperConfig.kt` → `WatchdogConfig.kt`
- Rename: `src/main/kotlin/workflow/usecase/service/orchestration/InputResolver.kt` → `ActivityInputResolver.kt`
- Rename: `src/test/kotlin/workflow/usecase/service/orchestration/SweeperTest.kt` → `WorkflowWatchdogTest.kt`
- Rename: `src/test/kotlin/workflow/usecase/service/orchestration/InputResolverTest.kt` → `ActivityInputResolverTest.kt`
- Modify (via sed): All .kt files referencing these names

- [ ] **Step 1: Rename source files**

```bash
git mv src/main/kotlin/workflow/usecase/service/orchestration/Sweeper.kt src/main/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdog.kt
git mv src/main/kotlin/workflow/config/SweeperConfig.kt src/main/kotlin/workflow/config/WatchdogConfig.kt
git mv src/main/kotlin/workflow/usecase/service/orchestration/InputResolver.kt src/main/kotlin/workflow/usecase/service/orchestration/ActivityInputResolver.kt
```

- [ ] **Step 2: Rename test files**

```bash
git mv src/test/kotlin/workflow/usecase/service/orchestration/SweeperTest.kt src/test/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdogTest.kt
git mv src/test/kotlin/workflow/usecase/service/orchestration/InputResolverTest.kt src/test/kotlin/workflow/usecase/service/orchestration/ActivityInputResolverTest.kt
```

- [ ] **Step 3: Replace class names in all .kt files**

**Order matters** — replace `SweeperConfig` before `Sweeper`:

```bash
find src -name '*.kt' -exec sed -i 's/SweeperConfig/WatchdogConfig/g' {} +
find src -name '*.kt' -exec sed -i 's/Sweeper/WorkflowWatchdog/g' {} +
find src -name '*.kt' -exec sed -i 's/InputResolver/ActivityInputResolver/g' {} +
```

- [ ] **Step 4: Fix variable names**

```bash
find src -name '*.kt' -exec sed -i 's/sweeperConfig/watchdogConfig/g' {} +
find src -name '*.kt' -exec sed -i 's/sweeper/watchdog/g' {} +
find src -name '*.kt' -exec sed -i 's/inputResolver/activityInputResolver/g' {} +
```

- [ ] **Step 5: Compile**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -q
```

Expected: BUILD SUCCESS

- [ ] **Step 6: Run affected tests**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowWatchdogTest,ActivityInputResolverTest,WorkerLoopTest,FrameworkConfigTest,WorkflowIntegrationTest" -pl WorkFlow
```

Expected: All tests PASS

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "rename: Sweeper → WorkflowWatchdog, SweeperConfig → WatchdogConfig, InputResolver → ActivityInputResolver"
```

---

### Task 4: Dispatch Domain Renames (7 renames)

Renames `DefaultCsvFormatter` → `DispatchCsvFormatter`, `StoragePort` → `StorageGateway`, `CandidateQueryPort` → `CandidateRepository`, `SelectionEntry` → `GapEntry`, `SelectionKernel` (file) → `GapKernel`, `DefaultCandidateMatcher` → `FirstFitCandidateMatcher`, `DefaultDispatchAlgorithm` → `GapBasedDispatchAlgorithm`.

**Ordering constraint:** `DefaultDispatchAlgorithm` is a substring of `DefaultDispatchAlgorithmFactory`. The factory must NOT be renamed. Protect it with a temp placeholder before replacing.

**Files:**
- Rename: `src/main/kotlin/dispatch/adapter/storage/DefaultCsvFormatter.kt` → `DispatchCsvFormatter.kt`
- Rename: `src/main/kotlin/dispatch/usecase/port/outbound/storage/StoragePort.kt` → `StorageGateway.kt`
- Rename: `src/main/kotlin/dispatch/usecase/port/outbound/persistence/CandidateQueryPort.kt` → `CandidateRepository.kt`
- Rename: `src/main/kotlin/dispatch/usecase/service/algorithm/SelectionKernel.kt` → `GapKernel.kt`
- Rename: `src/main/kotlin/dispatch/usecase/service/algorithm/DefaultCandidateMatcher.kt` → `FirstFitCandidateMatcher.kt`
- Rename: `src/main/kotlin/dispatch/usecase/service/algorithm/DefaultDispatchAlgorithm.kt` → `GapBasedDispatchAlgorithm.kt`
- Rename: `src/test/kotlin/dispatch/adapter/storage/DefaultCsvFormatterTest.kt` → `DispatchCsvFormatterTest.kt`
- Rename: `src/test/kotlin/dispatch/usecase/service/algorithm/SelectionKernelTest.kt` → `GapKernelTest.kt`
- Modify (via sed): All .kt files referencing these names

- [ ] **Step 1: Rename source files**

```bash
git mv src/main/kotlin/dispatch/adapter/storage/DefaultCsvFormatter.kt src/main/kotlin/dispatch/adapter/storage/DispatchCsvFormatter.kt
git mv src/main/kotlin/dispatch/usecase/port/outbound/storage/StoragePort.kt src/main/kotlin/dispatch/usecase/port/outbound/storage/StorageGateway.kt
git mv src/main/kotlin/dispatch/usecase/port/outbound/persistence/CandidateQueryPort.kt src/main/kotlin/dispatch/usecase/port/outbound/persistence/CandidateRepository.kt
git mv src/main/kotlin/dispatch/usecase/service/algorithm/SelectionKernel.kt src/main/kotlin/dispatch/usecase/service/algorithm/GapKernel.kt
git mv src/main/kotlin/dispatch/usecase/service/algorithm/DefaultCandidateMatcher.kt src/main/kotlin/dispatch/usecase/service/algorithm/FirstFitCandidateMatcher.kt
git mv src/main/kotlin/dispatch/usecase/service/algorithm/DefaultDispatchAlgorithm.kt src/main/kotlin/dispatch/usecase/service/algorithm/GapBasedDispatchAlgorithm.kt
```

- [ ] **Step 2: Rename test files**

```bash
git mv src/test/kotlin/dispatch/adapter/storage/DefaultCsvFormatterTest.kt src/test/kotlin/dispatch/adapter/storage/DispatchCsvFormatterTest.kt
git mv src/test/kotlin/dispatch/usecase/service/algorithm/SelectionKernelTest.kt src/test/kotlin/dispatch/usecase/service/algorithm/GapKernelTest.kt
```

- [ ] **Step 3: Replace class names in all .kt files**

**Order matters for `DefaultDispatchAlgorithm`** — protect the Factory first:

```bash
# Protect DefaultDispatchAlgorithmFactory from the substring replace
find src -name '*.kt' -exec sed -i 's/DefaultDispatchAlgorithmFactory/TEMP_FACTORY_GUARD/g' {} +

# Now safe to replace (no substring conflicts for the rest)
find src -name '*.kt' -exec sed -i 's/DefaultDispatchAlgorithm/GapBasedDispatchAlgorithm/g' {} +

# Restore the factory name
find src -name '*.kt' -exec sed -i 's/TEMP_FACTORY_GUARD/DefaultDispatchAlgorithmFactory/g' {} +

# Remaining renames (no ordering issues)
find src -name '*.kt' -exec sed -i 's/DefaultCsvFormatter/DispatchCsvFormatter/g' {} +
find src -name '*.kt' -exec sed -i 's/StoragePort/StorageGateway/g' {} +
find src -name '*.kt' -exec sed -i 's/CandidateQueryPort/CandidateRepository/g' {} +
find src -name '*.kt' -exec sed -i 's/SelectionEntry/GapEntry/g' {} +
find src -name '*.kt' -exec sed -i 's/SelectionKernel/GapKernel/g' {} +
find src -name '*.kt' -exec sed -i 's/DefaultCandidateMatcher/FirstFitCandidateMatcher/g' {} +
```

- [ ] **Step 4: Fix variable names**

```bash
find src -name '*.kt' -exec sed -i 's/storagePort/storageGateway/g' {} +
find src -name '*.kt' -exec sed -i 's/candidateQueryPort/candidateRepository/g' {} +
```

- [ ] **Step 5: Compile**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -q
```

Expected: BUILD SUCCESS

- [ ] **Step 6: Run affected tests**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchCsvFormatterTest,GapKernelTest,DispatchAlgorithmTest,CandidateMatcherTest,DispatchHandlersTest,DispatchAlgorithmDslTest,SimulationEngineTest,S3StorageAdapterTest" -pl WorkFlow
```

Expected: All tests PASS

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "rename: dispatch domain — DispatchCsvFormatter, StorageGateway, CandidateRepository, GapEntry, GapKernel, FirstFitCandidateMatcher, GapBasedDispatchAlgorithm"
```

---

### Task 5: Worker + Infrastructure Renames (2 renames)

Renames `DispatchNotifierImpl` → `HttpDispatchNotifier`, `QueryExporterBean` → `QueryExporterLifecycle`.

**Files:**
- Rename: `src/main/kotlin/worker/adapter/http/DispatchNotifierImpl.kt` → `HttpDispatchNotifier.kt`
- Rename: `src/main/kotlin/infrastructure/queryexporter/bootstrap/QueryExporterBean.kt` → `QueryExporterLifecycle.kt`
- Modify (via sed): All .kt files referencing these names

- [ ] **Step 1: Rename source files**

```bash
git mv src/main/kotlin/worker/adapter/http/DispatchNotifierImpl.kt src/main/kotlin/worker/adapter/http/HttpDispatchNotifier.kt
git mv src/main/kotlin/infrastructure/queryexporter/bootstrap/QueryExporterBean.kt src/main/kotlin/infrastructure/queryexporter/bootstrap/QueryExporterLifecycle.kt
```

- [ ] **Step 2: Replace class names in all .kt files**

```bash
find src -name '*.kt' -exec sed -i 's/DispatchNotifierImpl/HttpDispatchNotifier/g' {} +
find src -name '*.kt' -exec sed -i 's/QueryExporterBean/QueryExporterLifecycle/g' {} +
```

- [ ] **Step 3: Compile**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -q
```

Expected: BUILD SUCCESS

- [ ] **Step 4: Run affected tests**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchNotifierTest,DispatchNotifyResourceTest,StressTestBase" -pl WorkFlow
```

Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "rename: DispatchNotifierImpl → HttpDispatchNotifier, QueryExporterBean → QueryExporterLifecycle"
```

---

### Task 6: Update README

Update `README.md` to use new class names. Historical docs in `docs/superpowers/specs/` and `docs/superpowers/plans/` are frozen records and should NOT be updated.

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Replace old names in README**

```bash
sed -i 's/PhaseStrategy/AdvancementStrategy/g' README.md
sed -i 's/BarrierService/DefaultPhaseGate/g' README.md
sed -i 's/BarrierOperations/PhaseGate/g' README.md
sed -i 's/WorkflowOperations/WorkflowLifecycle/g' README.md
sed -i 's/SweeperConfig/WatchdogConfig/g' README.md
sed -i 's/Sweeper/WorkflowWatchdog/g' README.md
sed -i 's/InputResolver/ActivityInputResolver/g' README.md
sed -i 's/DefaultCsvFormatter/DispatchCsvFormatter/g' README.md
sed -i 's/StoragePort/StorageGateway/g' README.md
sed -i 's/CandidateQueryPort/CandidateRepository/g' README.md
sed -i 's/DefaultCandidateMatcher/FirstFitCandidateMatcher/g' README.md
sed -i 's/DefaultDispatchAlgorithm\b/GapBasedDispatchAlgorithm/g' README.md
sed -i 's/DispatchNotifierImpl/HttpDispatchNotifier/g' README.md
sed -i 's/QueryExporterBean/QueryExporterLifecycle/g' README.md
sed -i 's/SelectionEntry/GapEntry/g' README.md
```

- [ ] **Step 2: Review README for correctness**

Read `README.md` and verify the renames read naturally in context. Fix any awkward phrasing caused by mechanical replacement.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: update README with new class names"
```

---

### Task 7: Full Build Verification

Run the complete test suite to confirm no references were missed.

- [ ] **Step 1: Run full test suite**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow
```

Expected: BUILD SUCCESS, all tests pass

- [ ] **Step 2: Search for any remaining old names in source code**

```bash
grep -r "PhaseStrategy\|BarrierOperations\|BarrierService\|WorkflowOperations\|SweeperConfig\|InputResolver\|DefaultCsvFormatter\|StoragePort\|CandidateQueryPort\|SelectionEntry\|SelectionKernel\|DefaultCandidateMatcher\|DefaultDispatchAlgorithm[^F]\|DispatchNotifierImpl\|QueryExporterBean" src --include='*.kt' -l
```

Expected: No matches (empty output). If any files appear, update them manually.

- [ ] **Step 3: Verify grep for tricky substring — ensure DefaultDispatchAlgorithmFactory was NOT renamed**

```bash
grep -r "DefaultDispatchAlgorithmFactory" src --include='*.kt' -l
```

Expected: At least 1 file (the factory definition and its references still use the old name).

- [ ] **Step 4: Commit verification result or any final fixes**

If step 2 found stragglers, fix them and commit:

```bash
git add -A
git commit -m "rename: fix remaining stale references"
```
