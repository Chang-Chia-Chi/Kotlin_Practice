# K8s Launch-and-Watch: Phase 2 — K8sJobLauncherAdapter

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `K8sJobLauncherAdapter` — the Fabric8-backed adapter that creates a Kubernetes Job from a `HandlerResult.LaunchK8sJob` spec, stamped with the `workflow-managed=true` label.

**Architecture:** TDD — write `K8sJobLauncherAdapterTest` against a Fabric8 `KubernetesMockServer` in CRUD mode, then implement the adapter to make it pass.

**Tech Stack:** Kotlin, Fabric8 `kubernetes-server-mock` (CRUD mode), `kotlinx-coroutines`, JUnit 5

**Prerequisite:** Phase 1 complete — `KubernetesJobPort`, `K8sLabels`, `HandlerResult.LaunchK8sJob` all exist.

---

### Task 1: Write Failing K8sJobLauncherAdapterTest

**Files:**
- Create: `src/test/kotlin/worker/adapter/trigger/K8sJobLauncherAdapterTest.kt`

- [ ] **Step 1: Create the test file**

```kotlin
package com.workflow.worker.adapter.trigger

import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.server.mock.KubernetesCrudDispatcher
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer
import io.fabric8.mockwebserver.Context
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class K8sJobLauncherAdapterTest {

    private lateinit var server: KubernetesMockServer
    private lateinit var client: KubernetesClient
    private lateinit var adapter: K8sJobLauncherAdapter

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
        adapter = K8sJobLauncherAdapter(client)
    }

    @AfterEach
    fun tearDown() {
        server.destroy()
    }

    private fun minimalSpec(
        jobName: String = "test-job",
        namespace: String = "test-ns",
    ) = HandlerResult.LaunchK8sJob(
        jobName = jobName,
        namespace = namespace,
        image = "my-registry/worker:latest",
    )

    @Test
    fun `launch creates Job in the correct namespace with correct name`() = runBlocking {
        adapter.launch(minimalSpec())

        val job = client.batch().v1().jobs().inNamespace("test-ns").withName("test-job").get()
        assertNotNull(job, "Job should exist after launch")
        assertEquals("test-job", job.metadata.name)
        assertEquals("test-ns", job.metadata.namespace)
    }

    @Test
    fun `launch stamps Job with workflow-managed label`() = runBlocking {
        adapter.launch(minimalSpec())

        val job = client.batch().v1().jobs().inNamespace("test-ns").withName("test-job").get()
        assertNotNull(job)
        assertEquals(
            K8sLabels.WORKFLOW_MANAGED_VALUE,
            job.metadata.labels[K8sLabels.WORKFLOW_MANAGED],
            "Job must have workflow-managed=true label",
        )
    }

    @Test
    fun `launch sets image on container`() = runBlocking {
        adapter.launch(minimalSpec())

        val job = client.batch().v1().jobs().inNamespace("test-ns").withName("test-job").get()
        assertNotNull(job)
        val container = job.spec.template.spec.containers.first()
        assertEquals("my-registry/worker:latest", container.image)
    }

    @Test
    fun `launch sets args on container`() = runBlocking {
        val spec = HandlerResult.LaunchK8sJob(
            jobName = "test-job",
            namespace = "test-ns",
            image = "img:1",
            args = listOf("--mode=join", "--batch=abc"),
        )
        adapter.launch(spec)

        val job = client.batch().v1().jobs().inNamespace("test-ns").withName("test-job").get()
        assertNotNull(job)
        val container = job.spec.template.spec.containers.first()
        assertEquals(listOf("--mode=join", "--batch=abc"), container.args)
    }

    @Test
    fun `launch sets env vars on container`() = runBlocking {
        val spec = HandlerResult.LaunchK8sJob(
            jobName = "test-job",
            namespace = "test-ns",
            image = "img:1",
            env = mapOf("FOO" to "bar", "BATCH_TOKEN" to "abc123"),
        )
        adapter.launch(spec)

        val job = client.batch().v1().jobs().inNamespace("test-ns").withName("test-job").get()
        assertNotNull(job)
        val envVars = job.spec.template.spec.containers.first().env
            .associate { it.name to it.value }
        assertEquals("bar", envVars["FOO"])
        assertEquals("abc123", envVars["BATCH_TOKEN"])
    }

    @Test
    fun `launch sets backoffLimit to 0`() = runBlocking {
        adapter.launch(minimalSpec())

        val job = client.batch().v1().jobs().inNamespace("test-ns").withName("test-job").get()
        assertNotNull(job)
        assertEquals(0, job.spec.backoffLimit)
    }

    @Test
    fun `launch sets restartPolicy to Never`() = runBlocking {
        adapter.launch(minimalSpec())

        val job = client.batch().v1().jobs().inNamespace("test-ns").withName("test-job").get()
        assertNotNull(job)
        assertEquals("Never", job.spec.template.spec.restartPolicy)
    }

    @Test
    fun `launch with empty args creates container with no args`() = runBlocking {
        adapter.launch(minimalSpec())

        val job = client.batch().v1().jobs().inNamespace("test-ns").withName("test-job").get()
        assertNotNull(job)
        val container = job.spec.template.spec.containers.first()
        assertTrue(container.args.isNullOrEmpty())
    }

    @Test
    fun `launch with empty env creates container with no env vars`() = runBlocking {
        adapter.launch(minimalSpec())

        val job = client.batch().v1().jobs().inNamespace("test-ns").withName("test-job").get()
        assertNotNull(job)
        val container = job.spec.template.spec.containers.first()
        assertTrue(container.env.isNullOrEmpty())
    }
}
```

- [ ] **Step 2: Compile — expect error (K8sJobLauncherAdapter does not exist)**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow`
Expected: Compile error referencing `K8sJobLauncherAdapter`.

---

### Task 2: Implement K8sJobLauncherAdapter

**Files:**
- Create: `src/main/kotlin/worker/adapter/trigger/K8sJobLauncherAdapter.kt`

- [ ] **Step 1: Create the adapter**

```kotlin
package com.workflow.worker.adapter.trigger

import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import com.workflow.worker.usecase.port.outbound.KubernetesJobPort
import io.fabric8.kubernetes.api.model.EnvVarBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

@ApplicationScoped
class K8sJobLauncherAdapter(
    private val kubernetesClient: KubernetesClient,
) : KubernetesJobPort {

    override suspend fun launch(spec: HandlerResult.LaunchK8sJob) = withContext(Dispatchers.IO) {
        val envVars = spec.env.map { (k, v) ->
            EnvVarBuilder().withName(k).withValue(v).build()
        }

        val job = JobBuilder()
            .withNewMetadata()
                .withName(spec.jobName)
                .withNamespace(spec.namespace)
                .withLabels(mapOf(K8sLabels.WORKFLOW_MANAGED to K8sLabels.WORKFLOW_MANAGED_VALUE))
            .endMetadata()
            .withNewSpec()
                .withBackoffLimit(0)
                .withNewTemplate()
                    .withNewSpec()
                        .withRestartPolicy("Never")
                        .addNewContainer()
                            .withName("job")
                            .withImage(spec.image)
                            .apply { if (spec.args.isNotEmpty()) withArgs(spec.args) }
                            .apply { if (envVars.isNotEmpty()) withEnv(envVars) }
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build()

        kubernetesClient.batch().v1().jobs()
            .inNamespace(spec.namespace)
            .resource(job)
            .create()
    }
}
```

- [ ] **Step 2: Run K8sJobLauncherAdapterTest — confirm all pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="K8sJobLauncherAdapterTest"`
Expected: All PASS.

- [ ] **Step 3: Run full suite — confirm nothing broken**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: All PASS.

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/worker/adapter/trigger/K8sJobLauncherAdapter.kt
git add src/test/kotlin/worker/adapter/trigger/K8sJobLauncherAdapterTest.kt
git commit -m "feat: implement K8sJobLauncherAdapter with Fabric8 JobBuilder"
```
