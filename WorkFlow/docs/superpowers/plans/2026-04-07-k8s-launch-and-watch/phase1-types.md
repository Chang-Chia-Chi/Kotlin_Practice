# K8s Launch-and-Watch: Phase 1 — New Types

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `HandlerResult.LaunchK8sJob`, `K8sJobTypes.kt` (shared adapter constants), `KubernetesJobPort` (outbound port), and `launchK8sJob` helper. No implementation code yet — types and contracts only.

**Architecture:** TDD — write failing tests first for each new type, then add the type to make them pass.

**Tech Stack:** Kotlin, kotlinx-coroutines-test

---

### Task 1: HandlerResult.LaunchK8sJob — Test then Implement

**Files:**
- Modify: `src/test/kotlin/worker/usecase/port/inbound/execution/HandlerResultTest.kt`
- Modify: `src/main/kotlin/worker/usecase/port/inbound/execution/HandlerResult.kt`

- [ ] **Step 1: Add failing tests for LaunchK8sJob to HandlerResultTest**

Append these tests to `HandlerResultTest`:

```kotlin
@Test
fun `HandlerResult LaunchK8sJob carries job spec`() {
    val result = HandlerResult.LaunchK8sJob(
        jobName = "dispatch-join-abc",
        namespace = "default",
        image = "my-registry/dispatch-join:latest",
        args = listOf("--mode=join"),
        env = mapOf("BATCH_TOKEN" to "abc"),
    )
    assertEquals("dispatch-join-abc", result.jobName)
    assertEquals("default", result.namespace)
    assertEquals("my-registry/dispatch-join:latest", result.image)
    assertEquals(listOf("--mode=join"), result.args)
    assertEquals(mapOf("BATCH_TOKEN" to "abc"), result.env)
}

@Test
fun `HandlerResult LaunchK8sJob defaults args and env to empty`() {
    val result = HandlerResult.LaunchK8sJob(
        jobName = "j1",
        namespace = "ns",
        image = "img:tag",
    )
    assertTrue(result.args.isEmpty())
    assertTrue(result.env.isEmpty())
}
```

Also update the `exhaustive when on HandlerResult` test to include `LaunchK8sJob`:

```kotlin
@Test
fun `exhaustive when on HandlerResult`() {
    val results: List<HandlerResult> = listOf(
        HandlerResult.Completed(result = "done"),
        HandlerResult.LaunchK8sJob(jobName = "j", namespace = "ns", image = "img:1"),
    )
    val labels = results.map { hr ->
        when (hr) {
            is HandlerResult.Completed -> "completed"
            is HandlerResult.LaunchK8sJob -> "launch-k8s"
        }
    }
    assertEquals(listOf("completed", "launch-k8s"), labels)
}
```

- [ ] **Step 2: Run HandlerResultTest — expect compile error / failures**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow`
Expected: Compile error — `LaunchK8sJob` does not exist yet.

- [ ] **Step 3: Add LaunchK8sJob to HandlerResult**

Replace `src/main/kotlin/worker/usecase/port/inbound/execution/HandlerResult.kt`:

```kotlin
package com.workflow.worker.usecase.port.inbound.execution

sealed interface HandlerResult {
    data class Completed(
        val result: String?,
        val items: String? = null,
    ) : HandlerResult

    data class LaunchK8sJob(
        val jobName: String,
        val namespace: String,
        val image: String,
        val args: List<String> = emptyList(),
        val env: Map<String, String> = emptyMap(),
    ) : HandlerResult
}
```

- [ ] **Step 4: Run HandlerResultTest — confirm all pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="HandlerResultTest"`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/worker/usecase/port/inbound/execution/HandlerResult.kt
git add src/test/kotlin/worker/usecase/port/inbound/execution/HandlerResultTest.kt
git commit -m "feat: add HandlerResult.LaunchK8sJob sealed subtype"
```

---

### Task 2: Create K8sJobTypes.kt — Shared Adapter Constants

**Files:**
- Create: `src/main/kotlin/worker/adapter/trigger/K8sJobTypes.kt`

- [ ] **Step 1: Create the file**

```kotlin
package com.workflow.worker.adapter.trigger

/** Deserialized form of the `trigger_meta` JSON for k8s-job triggers. */
data class K8sJobMeta(val jobName: String, val namespace: String)

/** Label key/value applied to every Job created by this framework. The informer filters by this label. */
object K8sLabels {
    const val WORKFLOW_MANAGED = "workflow-managed"
    const val WORKFLOW_MANAGED_VALUE = "true"
}
```

- [ ] **Step 2: Remove K8sJobMeta from K8sJobTriggerDriver.kt**

In `src/main/kotlin/worker/adapter/trigger/K8sJobTriggerDriver.kt`, delete the existing `data class K8sJobMeta(...)` declaration (it now lives in `K8sJobTypes.kt` in the same package — no import needed).

Also delete `data class TrackedJob(...)` — it is no longer used after the informer rewrite (Phase 3).

- [ ] **Step 3: Compile check**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/worker/adapter/trigger/K8sJobTypes.kt
git add src/main/kotlin/worker/adapter/trigger/K8sJobTriggerDriver.kt
git commit -m "feat: add K8sJobTypes.kt with K8sJobMeta and K8sLabels constants"
```

---

### Task 3: Create KubernetesJobPort — Outbound Port

**Files:**
- Create: `src/main/kotlin/worker/usecase/port/outbound/KubernetesJobPort.kt`

- [ ] **Step 1: Create the interface**

```kotlin
package com.workflow.worker.usecase.port.outbound

import com.workflow.worker.usecase.port.inbound.execution.HandlerResult

/** Outbound port: creates a Kubernetes Job from a [HandlerResult.LaunchK8sJob] spec. */
interface KubernetesJobPort {
    suspend fun launch(spec: HandlerResult.LaunchK8sJob)
}
```

- [ ] **Step 2: Compile check**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/worker/usecase/port/outbound/KubernetesJobPort.kt
git commit -m "feat: add KubernetesJobPort outbound port interface"
```

---

### Task 4: launchK8sJob Helper — Test then Implement

**Files:**
- Modify: `src/test/kotlin/worker/usecase/port/inbound/trigger/TriggerTypesTest.kt`
- Modify: `src/main/kotlin/worker/usecase/port/inbound/trigger/TriggerTypes.kt`

- [ ] **Step 1: Add failing test for launchK8sJob helper**

Append to `TriggerTypesTest`:

```kotlin
@Test
fun `launchK8sJob creates LaunchK8sJob with correct fields`() {
    val result = launchK8sJob(
        jobName = "dispatch-join-abc",
        namespace = "prod-ns",
        image = "registry/dispatch-join:v1",
        args = listOf("--mode=join"),
        env = mapOf("TOKEN" to "abc"),
    )
    assertEquals("dispatch-join-abc", result.jobName)
    assertEquals("prod-ns", result.namespace)
    assertEquals("registry/dispatch-join:v1", result.image)
    assertEquals(listOf("--mode=join"), result.args)
    assertEquals(mapOf("TOKEN" to "abc"), result.env)
}

@Test
fun `launchK8sJob defaults args and env`() {
    val result = launchK8sJob(jobName = "j", namespace = "ns", image = "img:1")
    assertTrue(result.args.isEmpty())
    assertTrue(result.env.isEmpty())
}
```

- [ ] **Step 2: Run compile — expect error**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow`
Expected: Compile error — `launchK8sJob` does not exist yet.

- [ ] **Step 3: Add launchK8sJob to TriggerTypes.kt**

Replace `src/main/kotlin/worker/usecase/port/inbound/trigger/TriggerTypes.kt`:

```kotlin
package com.workflow.worker.usecase.port.inbound.trigger

import com.workflow.worker.usecase.port.inbound.execution.HandlerResult

object TriggerTypes {
    const val K8S_JOB = "k8s-job"
}

fun launchK8sJob(
    jobName: String,
    namespace: String,
    image: String,
    args: List<String> = emptyList(),
    env: Map<String, String> = emptyMap(),
): HandlerResult.LaunchK8sJob =
    HandlerResult.LaunchK8sJob(
        jobName = jobName,
        namespace = namespace,
        image = image,
        args = args,
        env = env,
    )
```

- [ ] **Step 4: Run TriggerTypesTest — confirm all pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="TriggerTypesTest"`
Expected: All PASS.

- [ ] **Step 5: Run full suite — confirm nothing broken**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/worker/usecase/port/inbound/trigger/TriggerTypes.kt
git add src/test/kotlin/worker/usecase/port/inbound/trigger/TriggerTypesTest.kt
git commit -m "feat: add launchK8sJob helper to TriggerTypes"
```
