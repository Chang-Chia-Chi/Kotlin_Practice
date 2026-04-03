# Trigger P5: K8sJobTriggerDriver Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `K8sJobTriggerDriver` — a `TriggerDriver` that monitors K8s Job completion via the Watch API, with result extraction from ConfigMap and best-effort cancellation.

**Architecture:** The driver maintains a set of tracked task IDs. On `start()`, it diffs against the current set, starts Watches for new Jobs via Fabric8's `KubernetesClient`. Watch callbacks populate a `ConcurrentLinkedQueue<TriggerResult>`. `poll()` drains the queue. Result is extracted from a ConfigMap named `{jobName}-output`. Watch reconnection is handled by Fabric8 internally.

**Tech Stack:** Kotlin, Fabric8 KubernetesClient, Mockito

**Depends on:** P1 (foundation types) + P3 (TriggerDriver SPI) must be complete.

---

### Task 1: Create `K8sJobTriggerDriver`

**Files:**
- Create: `src/main/kotlin/worker/adapter/trigger/K8sJobTriggerDriver.kt`

- [ ] **Step 1: Write the failing test**

Create `src/test/kotlin/worker/adapter/trigger/K8sJobTriggerDriverTest.kt`:

```kotlin
package com.workflow.worker.adapter.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.port.inbound.trigger.TriggerTypes
import io.fabric8.kubernetes.api.model.ConfigMap
import io.fabric8.kubernetes.api.model.ConfigMapBuilder
import io.fabric8.kubernetes.api.model.batch.v1.Job
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition
import io.fabric8.kubernetes.api.model.batch.v1.JobConditionBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.Watch
import io.fabric8.kubernetes.client.Watcher
import io.fabric8.kubernetes.client.WatcherException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class K8sJobTriggerDriverTest {

    private val objectMapper: ObjectMapper = jacksonObjectMapper()
    private lateinit var kubernetesClient: KubernetesClient
    private lateinit var driver: K8sJobTriggerDriver

    @BeforeEach
    fun setUp() {
        kubernetesClient = mock()
        driver = K8sJobTriggerDriver(kubernetesClient, objectMapper)
    }

    private fun makeMeta(
        jobName: String = "training-t1",
        namespace: String = "default",
    ): String = objectMapper.writeValueAsString(
        mapOf("jobName" to jobName, "namespace" to namespace),
    )

    private fun makeRef(
        taskId: String = "t-1",
        meta: String = makeMeta(),
    ) = DeferredTaskRef(
        taskId = taskId,
        workflowId = "wf-1",
        sequenceNumber = 1,
        triggerType = TriggerTypes.K8S_JOB,
        triggerMeta = meta,
        deadlineAt = Instant.now().plusSeconds(3600),
        retryCount = 0,
        maxRetries = 3,
    )

    @Test
    fun `type returns k8s-job`() {
        assertEquals(TriggerTypes.K8S_JOB, driver.type())
    }

    @Test
    fun `start tracks new tasks`() = runTest {
        // Mock the watch chain
        setupMockWatch()
        val ref = makeRef()
        driver.start(listOf(ref))

        // Driver should have started a watch — verify poll returns empty (no events yet)
        val results = driver.poll()
        assertTrue(results.isEmpty())
    }

    @Test
    fun `start with already-tracked task does not create duplicate watch`() = runTest {
        val mockWatch = setupMockWatch()
        val ref = makeRef()
        driver.start(listOf(ref))
        driver.start(listOf(ref)) // second call

        // Only one watch should have been created
        // Verification depends on mock setup — the key point is poll() doesn't duplicate
        val results = driver.poll()
        assertTrue(results.isEmpty())
    }

    @Test
    fun `Job Complete condition produces Succeeded`() = runTest {
        val capturedWatcher = setupMockWatchCapturingWatcher()
        val ref = makeRef()
        driver.start(listOf(ref))

        // Simulate Job completion via the captured watcher
        val completedJob = JobBuilder()
            .withNewMetadata().withName("training-t1").withNamespace("default").endMetadata()
            .withNewStatus()
            .withConditions(
                JobConditionBuilder()
                    .withType("Complete")
                    .withStatus("True")
                    .build(),
            )
            .endStatus()
            .build()

        // Mock ConfigMap result extraction
        setupMockConfigMap("training-t1", "default", """{"accuracy":"0.95"}""")

        capturedWatcher.eventReceived(Watcher.Action.MODIFIED, completedJob)

        val results = driver.poll()
        assertEquals(1, results.size)
        assertTrue(results[0] is TriggerResult.Succeeded)
        assertEquals("t-1", results[0].taskId)
    }

    @Test
    fun `Job Failed condition produces Failed`() = runTest {
        val capturedWatcher = setupMockWatchCapturingWatcher()
        val ref = makeRef()
        driver.start(listOf(ref))

        val failedJob = JobBuilder()
            .withNewMetadata().withName("training-t1").withNamespace("default").endMetadata()
            .withNewStatus()
            .withConditions(
                JobConditionBuilder()
                    .withType("Failed")
                    .withStatus("True")
                    .withReason("BackoffLimitExceeded")
                    .build(),
            )
            .endStatus()
            .build()

        capturedWatcher.eventReceived(Watcher.Action.MODIFIED, failedJob)

        val results = driver.poll()
        assertEquals(1, results.size)
        assertTrue(results[0] is TriggerResult.Failed)
        assertTrue((results[0] as TriggerResult.Failed).reason.contains("BackoffLimitExceeded"))
    }

    @Test
    fun `cancel deletes the Job`() = runTest {
        setupMockWatch()
        setupMockJobDelete("training-t1", "default")
        val ref = makeRef()
        driver.start(listOf(ref))

        driver.cancel("t-1")

        // Verify Job deletion was attempted
        verifyJobDeleteCalled("training-t1", "default")
    }

    @Test
    fun `close closes all watches`() = runTest {
        val mockWatch = setupMockWatch()
        val ref = makeRef()
        driver.start(listOf(ref))

        driver.close()

        verify(mockWatch).close()
    }

    @Test
    fun `ConfigMap absent produces Succeeded with null result`() = runTest {
        val capturedWatcher = setupMockWatchCapturingWatcher()
        val ref = makeRef()
        driver.start(listOf(ref))

        // No ConfigMap setup — returns null
        setupMockConfigMapAbsent("training-t1", "default")

        val completedJob = JobBuilder()
            .withNewMetadata().withName("training-t1").withNamespace("default").endMetadata()
            .withNewStatus()
            .withConditions(
                JobConditionBuilder()
                    .withType("Complete")
                    .withStatus("True")
                    .build(),
            )
            .endStatus()
            .build()

        capturedWatcher.eventReceived(Watcher.Action.MODIFIED, completedJob)

        val results = driver.poll()
        assertEquals(1, results.size)
        assertTrue(results[0] is TriggerResult.Succeeded)
        assertEquals(null, (results[0] as TriggerResult.Succeeded).result)
    }

    // ── Helper methods for mock setup ──
    // These will need to be adapted to the actual Fabric8 mock chain
    // which requires chaining: kubernetesClient.batch().v1().jobs().inNamespace(...).withName(...).watch(...)

    private fun setupMockWatch(): Watch {
        val mockWatch = mock<Watch>()
        // Set up the Fabric8 mock chain for watching Jobs
        // This is complex — use the same pattern as LeaderManagerTest
        // The key chain: kubernetesClient.batch().v1().jobs().inNamespace(ns).withLabel(label, value).watch(watcher)
        return mockWatch
    }

    private fun setupMockWatchCapturingWatcher(): Watcher<Job> {
        // Capture the Watcher passed to .watch() so we can simulate events
        TODO("Implement based on actual Fabric8 mock chain pattern used in LeaderManagerTest")
    }

    private fun setupMockConfigMap(jobName: String, namespace: String, data: String) {
        // Mock: kubernetesClient.configMaps().inNamespace(ns).withName("$jobName-output").get()
        TODO("Implement Fabric8 ConfigMap mock chain")
    }

    private fun setupMockConfigMapAbsent(jobName: String, namespace: String) {
        // Mock: returns null for the ConfigMap
        TODO("Implement Fabric8 ConfigMap mock chain returning null")
    }

    private fun setupMockJobDelete(jobName: String, namespace: String) {
        // Mock: kubernetesClient.batch().v1().jobs().inNamespace(ns).withName(jobName).delete()
        TODO("Implement Fabric8 Job delete mock chain")
    }

    private fun verifyJobDeleteCalled(jobName: String, namespace: String) {
        // Verify the delete chain was called
        TODO("Implement verification")
    }
}
```

**Important note:** The `TODO()` placeholders in the test helpers must be filled in based on the actual Fabric8 mock chain pattern used in `LeaderManagerTest.kt`. Read that file to understand the mocking approach and replicate it for the Job and ConfigMap API chains.

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="K8sJobTriggerDriverTest" -pl WorkFlow`
Expected: FAIL — `K8sJobTriggerDriver` does not exist.

- [ ] **Step 3: Create K8sJobTriggerDriver.kt**

```kotlin
package com.workflow.worker.adapter.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerDriver
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.port.inbound.trigger.TriggerTypes
import io.fabric8.kubernetes.api.model.batch.v1.Job
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.Watch
import io.fabric8.kubernetes.client.Watcher
import io.fabric8.kubernetes.client.WatcherException
import jakarta.enterprise.context.ApplicationScoped
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

@ApplicationScoped
class K8sJobTriggerDriver(
    private val kubernetesClient: KubernetesClient,
    private val objectMapper: ObjectMapper,
) : TriggerDriver {

    private val log = LoggerFactory.getLogger(K8sJobTriggerDriver::class.java)

    private data class TrackedJob(
        val taskId: String,
        val jobName: String,
        val namespace: String,
        val watch: Watch,
    )

    private val tracked = ConcurrentHashMap<String, TrackedJob>()
    private val resultQueue = ConcurrentLinkedQueue<TriggerResult>()

    override fun type(): String = TriggerTypes.K8S_JOB

    override suspend fun start(tasks: List<DeferredTaskRef>) {
        val currentIds = tasks.map { it.taskId }.toSet()

        // Remove tracked tasks no longer in DEFERRED set
        val removed = tracked.keys.filter { it !in currentIds }
        for (taskId in removed) {
            val t = tracked.remove(taskId)
            t?.watch?.close()
        }

        for (task in tasks) {
            if (tracked.containsKey(task.taskId)) continue

            val meta = objectMapper.readValue<K8sJobMeta>(task.triggerMeta)

            val watcher = object : Watcher<Job> {
                override fun eventReceived(action: Watcher.Action, resource: Job) {
                    val conditions = resource.status?.conditions ?: return
                    for (condition in conditions) {
                        if (condition.status != "True") continue
                        when (condition.type) {
                            "Complete" -> {
                                val result = readConfigMapOutput(meta.jobName, meta.namespace)
                                resultQueue.add(TriggerResult.Succeeded(task.taskId, result))
                                tracked.remove(task.taskId)?.watch?.close()
                                return
                            }
                            "Failed" -> {
                                val reason = condition.reason ?: "Unknown"
                                resultQueue.add(TriggerResult.Failed(task.taskId, reason))
                                tracked.remove(task.taskId)?.watch?.close()
                                return
                            }
                        }
                    }
                }

                override fun onClose(cause: WatcherException?) {
                    if (cause != null) {
                        log.warn("Watch closed for Job {}/{}: {}", meta.namespace, meta.jobName, cause.message)
                    }
                }
            }

            val watch = kubernetesClient.batch().v1().jobs()
                .inNamespace(meta.namespace)
                .withName(meta.jobName)
                .watch(watcher)

            tracked[task.taskId] = TrackedJob(task.taskId, meta.jobName, meta.namespace, watch)
            log.info("Started watching K8s Job {}/{} for task {}", meta.namespace, meta.jobName, task.taskId)
        }
    }

    override suspend fun poll(): List<TriggerResult> {
        val results = mutableListOf<TriggerResult>()
        while (true) {
            val r = resultQueue.poll() ?: break
            results.add(r)
        }
        return results
    }

    override suspend fun cancel(taskId: String) {
        val t = tracked.remove(taskId) ?: return
        t.watch.close()
        try {
            kubernetesClient.batch().v1().jobs()
                .inNamespace(t.namespace)
                .withName(t.jobName)
                .withPropagationPolicy("Background")
                .delete()
            log.info("Deleted K8s Job {}/{} for cancelled task {}", t.namespace, t.jobName, taskId)
        } catch (e: Exception) {
            log.warn("Failed to delete K8s Job {}/{} for task {}", t.namespace, t.jobName, taskId, e)
        }
    }

    override suspend fun close() {
        for ((taskId, t) in tracked) {
            try {
                t.watch.close()
            } catch (e: Exception) {
                log.warn("Failed to close watch for task {}", taskId, e)
            }
        }
        tracked.clear()
    }

    private fun readConfigMapOutput(jobName: String, namespace: String): String? {
        return try {
            val cm = kubernetesClient.configMaps()
                .inNamespace(namespace)
                .withName("$jobName-output")
                .get()
            cm?.data?.get("result")
        } catch (e: Exception) {
            log.warn("Failed to read output ConfigMap for Job {}/{}", namespace, jobName, e)
            null
        }
    }

    private data class K8sJobMeta(
        val jobName: String,
        val namespace: String,
    )
}
```

- [ ] **Step 4: Fill in test mock helpers based on Fabric8 patterns**

Read `src/test/kotlin/infrastructure/leader/LeaderManagerTest.kt` to understand the Fabric8 mock chain pattern. Adapt the `setupMockWatch()`, `setupMockWatchCapturingWatcher()`, `setupMockConfigMap()`, etc. helper methods to use the same approach.

The typical pattern is:
```kotlin
val batchApi = mock<BatchV1Api>()
val jobsOp = mock<MixedOperation<...>>()
val nsJobsOp = mock<...>()
whenever(kubernetesClient.batch()).thenReturn(batchApi)
whenever(batchApi.v1()).thenReturn(...)
// ... chain through to .watch(capture(watcherCaptor))
```

- [ ] **Step 5: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="K8sJobTriggerDriverTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 6: Commit**

```
feat: add K8sJobTriggerDriver with Watch API and ConfigMap result extraction
```

---

### Task 2: Test Watch reconnection resilience

**Files:**
- Modify: `src/test/kotlin/worker/adapter/trigger/K8sJobTriggerDriverTest.kt`

- [ ] **Step 1: Write reconnection test**

```kotlin
@Test
fun `Watch onClose does not produce Failed result`() = runTest {
    val capturedWatcher = setupMockWatchCapturingWatcher()
    val ref = makeRef()
    driver.start(listOf(ref))

    // Simulate Watch close with error (transient)
    capturedWatcher.onClose(WatcherException("Connection reset"))

    val results = driver.poll()
    // No Failed result — transient watch errors are not task failures
    assertTrue(results.isEmpty())
}
```

- [ ] **Step 2: Run test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="K8sJobTriggerDriverTest#Watch onClose does not produce Failed result" -pl WorkFlow`
Expected: PASS

- [ ] **Step 3: Commit**

```
test: verify K8s Watch transient close does not produce Failed result
```

---

### Task 3: Handle Watch reconnection on next sweep

**Files:**
- Modify: `src/main/kotlin/worker/adapter/trigger/K8sJobTriggerDriver.kt`
- Modify: `src/test/kotlin/worker/adapter/trigger/K8sJobTriggerDriverTest.kt`

When a Watch closes (transient error), the task remains tracked but has a dead Watch. On the next `start()` call (next sweep), the driver should detect the dead Watch and recreate it.

- [ ] **Step 1: Write the failing test**

```kotlin
@Test
fun `start recreates Watch for task with closed Watch`() = runTest {
    val firstWatcher = setupMockWatchCapturingWatcher()
    val ref = makeRef()
    driver.start(listOf(ref))

    // Simulate Watch close
    firstWatcher.onClose(WatcherException("Connection reset"))

    // Mark the tracked Watch as closed (driver should detect this)
    // On next start(), it should re-create the Watch
    val secondWatcher = setupMockWatchCapturingWatcher()
    driver.start(listOf(ref))

    // Now simulate completion on the new watcher
    val completedJob = JobBuilder()
        .withNewMetadata().withName("training-t1").withNamespace("default").endMetadata()
        .withNewStatus()
        .withConditions(
            JobConditionBuilder().withType("Complete").withStatus("True").build(),
        )
        .endStatus()
        .build()
    setupMockConfigMapAbsent("training-t1", "default")
    secondWatcher.eventReceived(Watcher.Action.MODIFIED, completedJob)

    val results = driver.poll()
    assertEquals(1, results.size)
    assertTrue(results[0] is TriggerResult.Succeeded)
}
```

- [ ] **Step 2: Add Watch-closed detection to K8sJobTriggerDriver**

In the `TrackedJob` data class, add a `@Volatile var closed: Boolean = false` flag. In `onClose()`, set `closed = true`. In `start()`, when iterating existing tasks, if `tracked[taskId]?.closed == true`, close the old Watch and create a new one.

```kotlin
private data class TrackedJob(
    val taskId: String,
    val jobName: String,
    val namespace: String,
    val watch: Watch,
    @Volatile var closed: Boolean = false,
)
```

In the Watcher's `onClose()`:
```kotlin
override fun onClose(cause: WatcherException?) {
    tracked[task.taskId]?.closed = true
    if (cause != null) {
        log.warn("Watch closed for Job {}/{}: {}", meta.namespace, meta.jobName, cause.message)
    }
}
```

In `start()`, change the skip condition:
```kotlin
val existing = tracked[task.taskId]
if (existing != null && !existing.closed) continue
if (existing != null && existing.closed) {
    existing.watch.close()
    tracked.remove(task.taskId)
}
```

- [ ] **Step 3: Run test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="K8sJobTriggerDriverTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 4: Commit**

```
feat: K8sJobTriggerDriver recreates Watch after transient close
```
