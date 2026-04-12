# Phase 2 — Repository Signature Widening

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional `categories: Set<DispatchCategory>` parameter to `DispatchConfigRepository.findActiveConfigs`. Default it to `emptySet()` so production call sites stay source-compatible. Preemptively widen every Mockito stub / `verify` on `findActiveConfigs` from single-arg to two-arg form — Mockito throws `InvalidUseOfMatchersException` if a matcher is mixed with a literal default value, so this must be done in the same phase as the interface change.

**Architecture:** One interface file in `main/`, plus mechanical matcher widening across three test files. Production code (`DispatchScatterHandler.handleCronTrigger`, `DispatchDryRunResource`) needs no change — the Kotlin default argument fills in `emptySet()` automatically at call sites.

**Tech Stack:** Kotlin default parameter values, Mockito Kotlin matchers.

---

## Task 1 — Widen `findActiveConfigs` signature

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/port/outbound/persistence/DispatchConfigRepository.kt`

- [ ] **Step 1: Replace the interface**

```kotlin
package com.workflow.dispatch.usecase.port.outbound.persistence

import com.workflow.dispatch.model.DispatchCategory
import com.workflow.dispatch.model.DispatchConfig
import java.time.LocalDateTime

interface DispatchConfigRepository {
    /**
     * Return all active configs as of [asOf], optionally filtered by [categories].
     *
     * @param categories When empty, no category predicate is applied (returns all active
     *   configs). When non-empty, narrows the result to configs whose [DispatchConfig.category]
     *   is in the given set (SQL-equivalent `AND category IN (...)`).
     */
    suspend fun findActiveConfigs(
        asOf: LocalDateTime,
        categories: Set<DispatchCategory> = emptySet(),
    ): List<DispatchConfig>

    suspend fun findById(configId: String): DispatchConfig
}
```

- [ ] **Step 2: Verify production code still compiles**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o compile -q`
Expected: BUILD SUCCESS. The production callers `DispatchScatterHandler.handleCronTrigger` (at `configRepo.findActiveConfigs(LocalDateTime.now())`) and `DispatchDryRunResource` (at `configRepo.findActiveConfigs(LocalDateTime.now()).map { it.id }`) still compile — Kotlin's default parameter fills the second arg with `emptySet()` automatically.

- [ ] **Step 3: Attempt to run the test suite — expect failure**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test -q`
Expected: FAIL. Tests using `whenever(configRepo.findActiveConfigs(any())).thenReturn(...)` or `verify(configRepo).findActiveConfigs(any())` throw `InvalidUseOfMatchersException` at runtime because the Mockito stub mixes a matcher (`any()`) with a literal default (`emptySet()`) supplied by Kotlin. Task 2 fixes this.

---

## Task 2 — Widen Mockito stubs and verifies in `DispatchHandlersTest.kt`

**Files:**
- Modify: `src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt`

This file has four call sites using the single-arg form at roughly lines 45, 103, 121, 135.

- [ ] **Step 1: Replace every single-arg `findActiveConfigs(any())` stub/verify with `findActiveConfigs(any(), any())`**

Apply replace-all from `findActiveConfigs(any())` to `findActiveConfigs(any(), any())` within this file. Example before/after:

Before:
```kotlin
whenever(configRepo.findActiveConfigs(any())).thenReturn(listOf(config))
```

After:
```kotlin
whenever(configRepo.findActiveConfigs(any(), any())).thenReturn(listOf(config))
```

Before:
```kotlin
verify(configRepo, never()).findActiveConfigs(any())
```

After:
```kotlin
verify(configRepo, never()).findActiveConfigs(any(), any())
```

Before:
```kotlin
verify(configRepo).findActiveConfigs(any())
```

After:
```kotlin
verify(configRepo).findActiveConfigs(any(), any())
```

- [ ] **Step 2: Run the file's tests**

Run:
```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test -q -Dtest=DispatchHandlersTest
```
Expected: BUILD SUCCESS for this test class.

---

## Task 3 — Widen stubs in `DispatchDryRunResourceTest.kt`

**Files:**
- Modify: `src/test/kotlin/dispatch/adapter/http/DispatchDryRunResourceTest.kt`

This file has six call sites at roughly lines 63, 122, 129, 151, 179, 205.

- [ ] **Step 1: Apply the same mechanical replacement**

Replace every `findActiveConfigs(any())` with `findActiveConfigs(any(), any())` in this file. The pattern matches `whenever(...)` and `verify(...)` alike.

- [ ] **Step 2: Run the file's tests**

Run:
```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test -q -Dtest=DispatchDryRunResourceTest
```
Expected: BUILD SUCCESS for this test class.

---

## Task 4 — Widen the stub in `DispatchE2EHappyPathTest.kt`

**Files:**
- Modify: `src/test/kotlin/dispatch/DispatchE2EHappyPathTest.kt:124`

- [ ] **Step 1: Widen the single stub at line ~124**

Before:
```kotlin
whenever(configRepo.findActiveConfigs(any<LocalDateTime>())).thenReturn(configs)
```

After:
```kotlin
whenever(configRepo.findActiveConfigs(any<LocalDateTime>(), any())).thenReturn(configs)
```

Keep the explicit `any<LocalDateTime>()` type parameter on the first matcher — it disambiguates the Kotlin overload resolution. The second `any()` is untyped because `Set<DispatchCategory>` is already unambiguous at that position.

- [ ] **Step 2: Run the E2E test class (requires Docker Desktop running)**

Run:
```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test -q -Dtest=DispatchE2EHappyPathTest
```
Expected: BUILD SUCCESS.

**If Docker Desktop is not running**, this test will error on test-container startup. That's an environment issue, not a plan failure — skip to Task 5 and run the full suite once Docker is up.

---

## Task 5 — Full test suite green

- [ ] **Step 1: Run every test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test -q`
Expected: BUILD SUCCESS. No `InvalidUseOfMatchersException`; no other regressions.

---

## Task 6 — Commit

- [ ] **Step 1: Stage every modified file**

Run each command in its own invocation — no chaining.

```bash
git add src/main/kotlin/dispatch/usecase/port/outbound/persistence/DispatchConfigRepository.kt
```
```bash
git add src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt
```
```bash
git add src/test/kotlin/dispatch/adapter/http/DispatchDryRunResourceTest.kt
```
```bash
git add src/test/kotlin/dispatch/DispatchE2EHappyPathTest.kt
```

- [ ] **Step 2: Commit**

```bash
git commit -m "♻️ refactor(dispatch): add categories filter to DispatchConfigRepository

Widens findActiveConfigs to accept a Set<DispatchCategory> that defaults
to emptySet() (meaning 'no category filter — return all active configs').
Production call sites stay source-compatible via Kotlin default arg.
Mockito stubs and verifies are widened to the two-arg matcher form to
avoid InvalidUseOfMatchersException — mixing a matcher with a literal
default at the call site is a runtime error. No behavior change yet;
the scatter handler still passes no categories. Sets the contract that
Phase 3 will start using."
```

- [ ] **Step 3: Verify**

```bash
git status
```
Expected: working tree clean of Phase 2 changes.
