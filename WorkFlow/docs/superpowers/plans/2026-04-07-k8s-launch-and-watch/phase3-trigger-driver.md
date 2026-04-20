# K8s Launch-and-Watch: Phase 3 — K8sJobTriggerDriver Rewrite

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite `K8sJobTriggerDriver` to use a single `SharedIndexInformer` (operator pattern) instead of per-job `Watch` instances. Replace the per-job watch unit tests with integration-style tests using `KubernetesMockServer` in CRUD mode.

**Architecture:** The informer watches all jobs with `workflow-managed=true` label across all namespaces. It starts once and auto-reconnects internally. `trackedJobs` maps `"namespace/jobName"` → `taskId`. `readConfigMapOutputWithRetry` retries up to 3 times with 500 ms delay to handle the race where the Job completes just before the ConfigMap is written.

**Tech Stack:** Kotlin, Fabric8 `SharedIndexInformer`, `kubernetes-server-mock` CRUD mode, Awaitility, kotlinx-coroutines

**Prerequisite:** Phase 1 complete — `K8sJobMeta`, `K8sLabels` exist in `K8sJobTypes.kt`.

---

### Task 1: Write New K8sJobTriggerDriverTest

**Files:**
- Modify: `src/test/kotlin/worker/adapter/trigger/K8sJobTriggerDriverTest.kt`

- [ ] **Step 1: Replace the entire test file**

The old test used mocked Fabric8 chains for per-job Watches. The new driver uses `SharedIndexInformer`, which requires a live Watch stream — CRUD mock server provides this.

```kotlin
package com.workflow.worker.adapter.trigger

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.port.inbound.trigger.TriggerTypes
import io.fabric8.kubernetes.api.model.ConfigMapBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobConditionBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobStatusBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.server.mock.KubernetesCrudDispatcher
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer
import io.fabric8.mockwebserver.Context
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.MockWebServer
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class K8sJobTriggerDriverTest {

    private lateinit var server: KubernetesMockServer
    private lateinit var client: KubernetesClient
    private lateinit var driver: K8sJobTriggerDriver
    private val objectMapper = jacksonObjectMapper()

    @BeforeEach
    fun setUp() {
        server = KubernetesMockServer(
            Context(),
            MockWebServer(),
            HashMap(),
            KubernetesCrudDispatcher(),
            false,
        )
        server.init()
        client = server.createClient()
        driver = K8sJobTriggerDriver(client, objectMapper)
    }

    @AfterEach
    fun tearDown() = runBlocking {
        driver.close()
        server.destroy()
    }

    // ── Factories ─────────────────────────────────────────────────────────

    private fun makeRef(
        taskId: String = "t-1",
        jobName: String = "test-job",
        namespace: String = "test-ns",
    ) = DeferredTaskRef(
        taskId = taskId,
        workflowId = "wf-1",
        sequenceNumber = 1,
        triggerType = TriggerTypes.K8S_JOB,
        triggerMeta = objectMapper.writeValueAsString(K8sJobMeta(jobName, namespace)),
        deadlineAt = Instant.now().plusSeconds(3600),
        retryCount = 0,
        maxRetries = 3,
    )

    private fun createCompletedJob(jobName: String = "test-job", namespace: String = "test-ns") {
        client.batch().v1().jobs().inNamespace(namespace).resource(
            JobBuilder()
                .withNewMetadata()
                    .withName(jobName)
                    .withNamespace(namespace)
                    .withLabels(mapOf(K8sLabels.WORKFLOW_MANAGED to K8sLabels.WORKFLOW_MANAGED_VALUE))
                .endMetadata()
                .withStatus(
                    JobStatusBuilder()
                        .withConditions(
                            JobConditionBuilder()
                                .withType("Complete")
                                .withStatus("True")
                                .build(),
                        )
                        .build(),
                )
                .build(),
        ).create()
    }

    private fun createFailedJob(
        jobName: String = "test-job",
        namespace: String = "test-ns",
        reason: String = "BackoffLimitExceeded",
    ) {
        client.batch().v1().jobs().inNamespace(namespace).resource(
            JobBuilder()
                .withNewMetadata()
                    .withName(jobName)
                    .withNamespace(namespace)
                    .withLabels(mapOf(K8sLabels.WORKFLOW_MANAGED to K8sLabels.WORKFLOW_MANAGED_VALUE))
                .endMetadata()
                .withStatus(
                    JobStatusBuilder()
                        .withConditions(
                            JobConditionBuilder()
                                .withType("Failed")
                                .withStatus("True")
                                .withReason(reason)
                                .build(),
                        )
                        .build(),
                )
                .build(),
        ).create()
    }

    private fun createOutputConfigMap(
        jobName: String = "test-job",
        namespace: String = "test-ns",
        result: String = """{"output":"done"}""",
    ) {
        client.configMaps().inNamespace(namespace).resource(
            ConfigMapBuilder()
                .withNewMetadata()
                    .withName("$jobName-output")
                    .withNamespace(namespace)
                .endMetadata()
                .addToData("result", result)
                .build(),
        ).create()
    }

    // ── 1. type() ─────────────────────────────────────────────────────────

    @Test
    fun `type returns k8s-job`() {
        assertEquals(TriggerTypes.K8S_JOB, driver.type())
    }

    // ── 2. Job Complete → Succeeded with ConfigMap result ─────────────────

    @Test
    fun `Job Complete condition produces Succeeded with ConfigMap result`() = runBlocking {
        val ref = makeRef()
        driver.start(listOf(ref))

        // Create ConfigMap first so it's available when the informer fires
        createOutputConfigMap(result = """{"accuracy":"0.95"}""")
        createCompletedJob()

        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            val results = runBlocking { driver.poll() }
            assertEquals(1, results.size)
            val s = results[0] as TriggerResult.Succeeded
            assertEquals("t-1", s.taskId)
            assertEquals("""{"accuracy":"0.95"}""", s.result)
        }
    }

    // ── 3. Job Failed → Failed with reason ────────────────────────────────

    @Test
    fun `Job Failed condition produces Failed with reason`() = runBlocking {
        val ref = makeRef()
        driver.start(listOf(ref))

        createFailedJob(reason = "BackoffLimitExceeded")

        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            val results = runBlocking { driver.poll() }
            assertEquals(1, results.size)
            val f = results[0] as TriggerResult.Failed
            assertEquals("t-1", f.taskId)
            assertTrue(f.reason.contains("BackoffLimitExceeded"))
        }
    }

    // ── 4. ConfigMap absent → Succeeded with null result ──────────────────

    @Test
    fun `ConfigMap absent produces Succeeded with null result`() = runBlocking {
        val ref = makeRef()
        driver.start(listOf(ref))

        // No ConfigMap created — driver retries 3 times then returns null
        createCompletedJob()

        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            val results = runBlocking { driver.poll() }
            assertEquals(1, results.size)
            val s = results[0] as TriggerResult.Succeeded
            assertNull(s.result)
        }
    }

    // ── 5. Unmanaged Job is ignored ───────────────────────────────────────

    @Test
    fun `Job without workflow-managed label is ignored`() = runBlocking {
        val ref = makeRef()
        driver.start(listOf(ref))

        // Job without the managed label
        client.batch().v1().jobs().inNamespace("test-ns").resource(
            JobBuilder()
                .withNewMetadata()
                    .withName("test-job")
                    .withNamespace("test-ns")
                    // NO label
                .endMetadata()
                .withStatus(
                    JobStatusBuilder()
                        .withConditions(
                            JobConditionBuilder().withType("Complete").withStatus("True").build(),
                        )
                        .build(),
                )
                .build(),
        ).create()

        // Wait briefly — no results expected
        Thread.sleep(500)
        val results = driver.poll()
        assertTrue(results.isEmpty(), "Unmanaged job should produce no results")
    }

    // ── 6. Untracked Job is ignored ───────────────────────────────────────

    @Test
    fun `Job for unregistered taskId is ignored`() = runBlocking {
        // Do NOT register any task
        driver.start(emptyList())

        createCompletedJob(jobName = "some-other-job")

        Thread.sleep(500)
        val results = driver.poll()
        assertTrue(results.isEmpty(), "Job for untracked task should produce no result")
    }

    // ── 7. cancel() → Job deleted, trackedCount == 0 ─────────────────────

    @Test
    fun `cancel removes task from tracked and deletes Job`() = runBlocking {
        val ref = makeRef()
        driver.start(listOf(ref))
        assertEquals(1, driver.trackedCount())

        // Create the job so delete has something to delete
        createCompletedJob()

        driver.cancel("t-1")
        assertEquals(0, driver.trackedCount())
    }

    // ── 8. cancel unknown taskId is no-op ─────────────────────────────────

    @Test
    fun `cancel on unknown taskId is a no-op`() = runBlocking {
        driver.cancel("nonexistent")
        assertEquals(0, driver.trackedCount())
    }

    // ── 9. start() removes stale tasks ────────────────────────────────────

    @Test
    fun `start removes tracked tasks no longer in deferred list`() = runBlocking {
        val ref = makeRef()
        driver.start(listOf(ref))
        assertEquals(1, driver.trackedCount())

        driver.start(emptyList())
        assertEquals(0, driver.trackedCount())
    }

    // ── 10. start() idempotent for already-tracked ────────────────────────

    @Test
    fun `start with already-tracked task does not double-register`() = runBlocking {
        val ref = makeRef()
        driver.start(listOf(ref))
        driver.start(listOf(ref))
        assertEquals(1, driver.trackedCount())
    }

    // ── 11. close() → trackedCount == 0 ──────────────────────────────────

    @Test
    fun `close clears all tracked tasks and stops informer`() = runBlocking {
        val ref = makeRef()
        driver.start(listOf(ref))
        assertEquals(1, driver.trackedCount())

        driver.close()
        assertEquals(0, driver.trackedCount())
    }

    // ── 12. Duplicate terminal event produces only one result ─────────────

    @Test
    fun `duplicate complete events produce only one Succeeded`() = runBlocking {
        val ref = makeRef()
        driver.start(listOf(ref))

        createOutputConfigMap()
        createCompletedJob()

        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            val results = runBlocking { driver.poll() }
            assertEquals(1, results.size)
            assertTrue(results[0] is TriggerResult.Succeeded)
        }

        // Second poll: queue already drained
        val results2 = driver.poll()
        assertTrue(results2.isEmpty())
    }

    // ── 13. poll with no events returns empty ─────────────────────────────

    @Test
    fun `poll with no events returns empty list`() = runBlocking {
        val results = driver.poll()
        assertTrue(results.isEmpty())
    }

    // ── 14. readConfigMapOutputWithRetry ─────────────────────────────────

    @Test
    fun `readConfigMapOutputWithRetry returns result when ConfigMap present`() = runBlocking {
        createOutputConfigMap(result = """{"output":"done"}""")
        val result = driver.readConfigMapOutputWithRetry("test-job", "test-ns")
        assertEquals("""{"output":"done"}""", result)
    }

    @Test
    fun `readConfigMapOutputWithRetry returns null when ConfigMap absent after retries`() = runBlocking {
        val result = driver.readConfigMapOutputWithRetry("missing-job", "test-ns")
        assertNull(result)
    }
}
```

- [ ] **Step 2: Run test-compile — expect error (new driver not written yet)**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow`
Expected: Compile errors referencing missing methods on `K8sJobTriggerDriver` (e.g., `readConfigMapOutputWithRetry`).

---

### Task 2: Rewrite K8sJobTriggerDriver

**Files:**
- Modify: `src/main/kotlin/worker/adapter/trigger/K8sJobTriggerDriver.kt`

- [ ] **Step 1: Replace the entire driver implementation**

```kotlin
package com.workflow.worker.adapter.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerDriver
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.port.inbound.trigger.TriggerTypes
import io.fabric8.kubernetes.api.model.DeletionPropagation
import io.fabric8.kubernetes.api.model.batch.v1.Job
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.informers.ResourceEventHandler
import io.fabric8.kubernetes.client.informers.SharedIndexInformer
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Trigger driver that monitors Kubernetes Job completion via a single
 * [SharedIndexInformer] filtered by the [K8sLabels.WORKFLOW_MANAGED] label.
 *
 * The informer is started once on the first [start] call and auto-reconnects
 * internally — no manual reconnection logic is needed.
 *
 * [trackedJobs] maps "namespace/jobName" → taskId. [trackedTaskIds] provides
 * O(1) membership check. Watch callbacks enqueue lightweight [WatchEvent]
 * markers only — no blocking I/O in callbacks. ConfigMap reads are deferred
 * to [poll] on [Dispatchers.IO].
 */
@ApplicationScoped
class K8sJobTriggerDriver(
    private val kubernetesClient: KubernetesClient,
    private val objectMapper: ObjectMapper,
) : TriggerDriver {

    private val log = LoggerFactory.getLogger(K8sJobTriggerDriver::class.java)

    private sealed interface WatchEvent {
        val taskId: String
        val jobName: String
        val namespace: String

        data class Completed(
            override val taskId: String,
            override val jobName: String,
            override val namespace: String,
        ) : WatchEvent

        data class Failed(
            override val taskId: String,
            override val jobName: String,
            override val namespace: String,
            val reason: String,
        ) : WatchEvent
    }

    /** "namespace/jobName" → taskId */
    private val trackedJobs = ConcurrentHashMap<String, String>()

    /** O(1) taskId membership — kept in sync with [trackedJobs]. */
    private val trackedTaskIds: MutableSet<String> = ConcurrentHashMap.newKeySet()

    private val settledTaskIds: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val eventQueue = ConcurrentLinkedQueue<WatchEvent>()

    @Volatile
    private var informer: SharedIndexInformer<Job>? = null

    override fun type(): String = TriggerTypes.K8S_JOB

    override suspend fun start(tasks: List<DeferredTaskRef>) {
        val incomingIds = tasks.map { it.taskId }.toSet()

        // Remove stale tracked entries
        trackedJobs.entries.removeIf { (_, taskId) ->
            if (taskId !in incomingIds) {
                trackedTaskIds.remove(taskId)
                true
            } else {
                false
            }
        }

        for (task in tasks) {
            if (task.taskId in settledTaskIds || task.taskId in trackedTaskIds) continue
            val meta = objectMapper.readValue<K8sJobMeta>(task.triggerMeta)
            val jobKey = "${meta.namespace}/${meta.jobName}"
            trackedJobs[jobKey] = task.taskId
            trackedTaskIds.add(task.taskId)
        }

        if (informer == null && trackedJobs.isNotEmpty()) {
            startInformer()
        }
    }

    private fun startInformer() {
        val handler = object : ResourceEventHandler<Job> {
            override fun onAdd(job: Job) = evaluateJob(job)
            override fun onUpdate(oldJob: Job, newJob: Job) = evaluateJob(newJob)
            override fun onDelete(job: Job, deletedFinalStateUnknown: Boolean) {}
        }

        informer = kubernetesClient.batch().v1().jobs()
            .inAnyNamespace()
            .withLabel(K8sLabels.WORKFLOW_MANAGED, K8sLabels.WORKFLOW_MANAGED_VALUE)
            .inform(handler)

        log.info("Started SharedIndexInformer for K8s Jobs (label={}={})", K8sLabels.WORKFLOW_MANAGED, K8sLabels.WORKFLOW_MANAGED_VALUE)
    }

    private fun evaluateJob(job: Job) {
        val namespace = job.metadata.namespace
        val jobName = job.metadata.name
        val jobKey = "$namespace/$jobName"

        val taskId = trackedJobs[jobKey] ?: return
        if (taskId in settledTaskIds) return

        val conditions = job.status?.conditions ?: return
        for (condition in conditions) {
            if (condition.status != "True") continue
            when (condition.type) {
                "Complete" -> {
                    if (settledTaskIds.add(taskId)) {
                        eventQueue.add(WatchEvent.Completed(taskId, jobName, namespace))
                    }
                    return
                }
                "Failed" -> {
                    if (settledTaskIds.add(taskId)) {
                        eventQueue.add(
                            WatchEvent.Failed(taskId, jobName, namespace, condition.reason ?: "Unknown"),
                        )
                    }
                    return
                }
            }
        }
    }

    override suspend fun poll(): List<TriggerResult> {
        val events = mutableListOf<WatchEvent>()
        while (true) {
            events.add(eventQueue.poll() ?: break)
        }
        if (events.isEmpty()) return emptyList()

        return withContext(Dispatchers.IO) {
            events.map { event ->
                val jobKey = "${event.namespace}/${event.jobName}"
                trackedJobs.remove(jobKey)
                trackedTaskIds.remove(event.taskId)
                settledTaskIds.remove(event.taskId)

                when (event) {
                    is WatchEvent.Completed -> {
                        val result = readConfigMapOutputWithRetry(event.jobName, event.namespace)
                        TriggerResult.Succeeded(event.taskId, result)
                    }
                    is WatchEvent.Failed -> TriggerResult.Failed(event.taskId, event.reason)
                }
            }
        }
    }

    override suspend fun cancel(taskId: String) {
        val entry = trackedJobs.entries.find { it.value == taskId } ?: return
        val (jobKey, _) = entry
        val parts = jobKey.split("/", limit = 2)
        val namespace = parts[0]
        val jobName = parts[1]

        trackedJobs.remove(jobKey)
        trackedTaskIds.remove(taskId)
        settledTaskIds.remove(taskId)

        try {
            withContext(Dispatchers.IO) {
                kubernetesClient.batch().v1().jobs()
                    .inNamespace(namespace)
                    .withName(jobName)
                    .withPropagationPolicy(DeletionPropagation.BACKGROUND)
                    .delete()
            }
            log.info("Deleted K8s Job {}/{} for cancelled task {}", namespace, jobName, taskId)
        } catch (e: Exception) {
            log.warn("Failed to delete K8s Job {}/{} for task {}", namespace, jobName, taskId, e)
        }
    }

    override suspend fun close() {
        informer?.close()
        informer = null
        trackedJobs.clear()
        trackedTaskIds.clear()
        settledTaskIds.clear()
        eventQueue.clear()
    }

    /**
     * Reads ConfigMap `{jobName}-output`, key `"result"`.
     * Retries up to 3 times with 500 ms delay to handle the race where the Job
     * marks itself complete just before writing the output ConfigMap.
     * Returns null if the ConfigMap is absent after all attempts.
     */
    internal suspend fun readConfigMapOutputWithRetry(jobName: String, namespace: String): String? {
        repeat(3) { attempt ->
            try {
                val result = kubernetesClient.configMaps()
                    .inNamespace(namespace)
                    .withName("$jobName-output")
                    .get()
                    ?.data
                    ?.get("result")
                if (result != null) return result
            } catch (e: Exception) {
                log.warn("Failed to read output ConfigMap for Job {}/{}, attempt {}", namespace, jobName, attempt + 1, e)
            }
            delay(500)
        }
        return null
    }

    /** Test accessor. */
    internal fun trackedCount(): Int = trackedJobs.size
}
```

- [ ] **Step 2: Run K8sJobTriggerDriverTest — confirm all pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="K8sJobTriggerDriverTest"`
Expected: All PASS. Note: `ConfigMap absent` test takes ~1.5 s (3 retries × 500 ms).

- [ ] **Step 3: Run full suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: All PASS.

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/worker/adapter/trigger/K8sJobTriggerDriver.kt
git add src/test/kotlin/worker/adapter/trigger/K8sJobTriggerDriverTest.kt
git commit -m "refactor: rewrite K8sJobTriggerDriver to use SharedIndexInformer"
```
