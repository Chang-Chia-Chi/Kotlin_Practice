# Event-Driven Task Dispatch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace idle polling in WorkerLoop with event-driven dispatch via in-process SharedFlow signaling + cross-pod HTTP broadcast with K8s Endpoints Watch for peer discovery.

**Architecture:** When new PENDING tasks are inserted (by BarrierService after phase advance, WorkflowEngine on startWorkflow, or Sweeper on recovery), a `DispatchNotifier` wakes local workers via `MutableSharedFlow` and broadcasts to peer pods via fire-and-forget HTTP POST. Workers suspend on `awaitWork()` instead of `delay(pollInterval)`. A 5s fallback timeout catches missed signals. `PeerRegistry` maintains a live peer list via K8s Endpoints Watch.

**Tech Stack:** Kotlin Coroutines (`MutableSharedFlow`, `withTimeoutOrNull`), Ktor `HttpClient` (async coroutine-native, CIO engine), Fabric8 `KubernetesClient` (Endpoints Watch), Quarkus REST (`@Path` endpoint).

**Spec:** `docs/superpowers/specs/2026-03-29-event-driven-dispatch-design.md`

---

## File Structure

| File | Responsibility |
|------|---------------|
| **New:** `src/main/kotlin/worker/DispatchNotifier.kt` | SharedFlow-based local signal + HTTP broadcast to peers |
| **New:** `src/main/kotlin/worker/PeerRegistry.kt` | K8s Endpoints Watch, maintains live peer IP list |
| **New:** `src/main/kotlin/worker/DispatchNotifyResource.kt` | Internal HTTP endpoint receiving remote signals |
| **New:** `src/test/kotlin/worker/DispatchNotifierTest.kt` | Unit tests for signal/await/coalescing/multi-queue |
| **New:** `src/test/kotlin/worker/PeerRegistryTest.kt` | Unit tests for watch events, self-exclusion |
| **Modify:** `src/main/kotlin/config/FrameworkConfig.kt:14-23` | Add fallbackPollInterval, maxBatchSize, serviceName, podIp |
| **Modify:** `src/main/kotlin/worker/WorkerLoop.kt:87-97,187-217` | Inject notifier, replace delay→awaitWork, use maxBatchSize |
| **Modify:** `src/main/kotlin/engine/BarrierService.kt:24-44,66-81` | Inject notifier, return queue name from evaluateAndAdvance, signal after advance |
| **Modify:** `src/main/kotlin/engine/WorkflowEngine.kt:22-55` | Inject notifier, signal after startWorkflow |
| **Modify:** `src/main/resources/application.properties:19-20` | Add new config defaults |
| **Modify:** `k8s/rbac.yaml:6-9` | Add Endpoints get/list/watch |
| **Modify:** `src/test/kotlin/worker/WorkerLoopTest.kt:63-114` | Add notifier mock, update config stubs |

---

### Task 1: Add New Config Properties to FrameworkConfig

**Files:**
- Modify: `src/main/kotlin/config/FrameworkConfig.kt:14-23`
- Modify: `src/main/resources/application.properties:19-20`
- Modify: `src/test/kotlin/config/FrameworkConfigTest.kt`

- [ ] **Step 1: Add new properties to WorkerConfig interface**

In `src/main/kotlin/config/FrameworkConfig.kt`, add the new config methods to `WorkerConfig` and add a top-level `serviceName()`:

```kotlin
package com.workflow.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import java.time.Duration

@ConfigMapping(prefix = "framework")
interface FrameworkConfig {
    fun worker(): WorkerConfig
    fun leaderElection(): LeaderElectionConfig
    fun shutdown(): ShutdownConfig
    fun sweeper(): SweeperConfig

    @WithDefault("workflow-engine")
    fun serviceName(): String

    interface WorkerConfig {
        @WithDefault("localhost")
        fun id(): String
        @WithDefault("PT1S")
        fun pollInterval(): Duration
        @WithDefault("PT5S")
        fun fallbackPollInterval(): Duration
        @WithDefault("4")
        fun concurrency(): Int
        @WithDefault("1")
        fun batchSize(): Int
        @WithDefault("16")
        fun maxBatchSize(): Int
        @WithDefault("localhost")
        fun podIp(): String
    }

    interface LeaderElectionConfig {
        @WithDefault("default")
        fun namespace(): String
        @WithDefault("workflow-leader")
        fun leaseName(): String
        @WithDefault("PT15S")
        fun leaseDuration(): Duration
        @WithDefault("PT10S")
        fun renewDeadline(): Duration
        @WithDefault("PT2S")
        fun retryPeriod(): Duration
        @WithDefault("PT45S")
        fun healthThreshold(): Duration
    }

    interface ShutdownConfig {
        @WithDefault("PT30S")
        fun globalTimeout(): Duration
        @WithDefault("PT10S")
        fun leaderTeardownTimeout(): Duration
    }

    interface SweeperConfig {
        @WithDefault("PT30S")
        fun interval(): Duration
        @WithDefault("PT2M")
        fun gracePeriod(): Duration
        @WithDefault("PT10M")
        fun staleTaskThreshold(): Duration
    }
}
```

- [ ] **Step 2: Add defaults to application.properties**

Append to the `Framework Config` section of `src/main/resources/application.properties`:

```properties
framework.worker.fallback-poll-interval=PT5S
framework.worker.max-batch-size=16
framework.service-name=workflow-engine
framework.worker.pod-ip=${POD_IP:localhost}
```

- [ ] **Step 3: Run existing config tests to verify no breakage**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="FrameworkConfigTest,ConfigValidatorTest" -pl WorkFlow`

Expected: All existing tests pass. The new properties have defaults so nothing breaks.

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/config/FrameworkConfig.kt src/main/resources/application.properties
git commit -m "feat: add dispatch notifier config properties (fallbackPollInterval, maxBatchSize, serviceName, podIp)"
```

---

### Task 2: Add Ktor Client Dependency and Implement DispatchNotifier with Unit Tests

**Files:**
- Modify: `pom.xml` (add Ktor client deps)
- Create: `src/main/kotlin/worker/DispatchNotifier.kt`
- Create: `src/test/kotlin/worker/DispatchNotifierTest.kt`

- [ ] **Step 0: Add Ktor client dependencies to pom.xml**

Add a Ktor version property and dependencies. In `pom.xml`, add the property:

```xml
<ktor.version>3.1.1</ktor.version>
```

Add these dependencies after the Kotlin Coroutines section:

```xml
        <!-- Ktor Client (for dispatch notification broadcast) -->
        <dependency>
            <groupId>io.ktor</groupId>
            <artifactId>ktor-client-core-jvm</artifactId>
            <version>${ktor.version}</version>
        </dependency>
        <dependency>
            <groupId>io.ktor</groupId>
            <artifactId>ktor-client-cio-jvm</artifactId>
            <version>${ktor.version}</version>
        </dependency>
```

- [ ] **Step 1: Write failing tests for DispatchNotifier**

Create `src/test/kotlin/worker/DispatchNotifierTest.kt`:

```kotlin
package com.workflow.worker

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Duration
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class DispatchNotifierTest {

    private lateinit var peerRegistry: PeerRegistry
    private lateinit var notifier: DispatchNotifier

    @BeforeEach
    fun setup() {
        peerRegistry = mock()
        whenever(peerRegistry.peers()).thenReturn(emptyList())
        notifier = DispatchNotifier(peerRegistry)
    }

    @Test
    fun `signal wakes single awaitWork`() = runTest {
        val result = async {
            notifier.awaitWork("default", Duration.ofSeconds(5))
        }
        // Let the coroutine suspend on awaitWork
        delay(50)
        notifier.signal("default")
        assertTrue(result.await())
    }

    @Test
    fun `signal wakes multiple concurrent waiters`() = runTest {
        val results = (1..4).map {
            async { notifier.awaitWork("default", Duration.ofSeconds(5)) }
        }
        delay(50)
        notifier.signal("default")
        results.forEach { assertTrue(it.await()) }
    }

    @Test
    fun `awaitWork returns false on timeout`() = runTest {
        val result = notifier.awaitWork("default", Duration.ofMillis(100))
        assertFalse(result)
    }

    @Test
    fun `multi-queue isolation - signal on queue A does not wake queue B`() = runTest {
        val resultB = async {
            notifier.awaitWork("queue-b", Duration.ofMillis(200))
        }
        delay(50)
        notifier.signal("queue-a")
        assertFalse(resultB.await())
    }

    @Test
    fun `onRemoteSignal wakes local waiters`() = runTest {
        val result = async {
            notifier.awaitWork("default", Duration.ofSeconds(5))
        }
        delay(50)
        notifier.onRemoteSignal("default")
        assertTrue(result.await())
    }

    @Test
    fun `signal coalescing - rapid signals produce single wakeup`() = runTest {
        // Buffer a signal before anyone is listening
        repeat(100) { notifier.signal("default") }
        // Now await — should return immediately from buffered signal
        val result = notifier.awaitWork("default", Duration.ofMillis(200))
        assertTrue(result)
    }

    @Test
    fun `signal before await is buffered and delivered`() = runTest {
        notifier.signal("default")
        val result = notifier.awaitWork("default", Duration.ofMillis(200))
        assertTrue(result)
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchNotifierTest" -pl WorkFlow`

Expected: Compilation error — `DispatchNotifier` and `PeerRegistry` do not exist yet.

- [ ] **Step 3: Implement DispatchNotifier**

Create `src/main/kotlin/worker/DispatchNotifier.kt`:

```kotlin
package com.workflow.worker

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.post
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class DispatchNotifier(
    private val peerRegistry: PeerRegistry,
) {
    private val log = LoggerFactory.getLogger(DispatchNotifier::class.java)

    private val httpClient = HttpClient(CIO) {
        engine {
            requestTimeout = 2_000
            endpoint { connectTimeout = 2_000 }
        }
    }

    /** Fire-and-forget scope — failures are logged at DEBUG, never propagated. */
    private val broadcastScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    private val flows = ConcurrentHashMap<String, MutableSharedFlow<Unit>>()

    private fun flowFor(queue: String) = flows.getOrPut(queue) {
        MutableSharedFlow(
            replay = 0,
            extraBufferCapacity = 1,
            onBufferOverflow = BufferOverflow.DROP_OLDEST,
        )
    }

    /**
     * Signal that new work is available. Wakes local workers and
     * broadcasts to all peer pods via HTTP (fire-and-forget).
     */
    fun signal(queueName: String) {
        flowFor(queueName).tryEmit(Unit)
        val peers = peerRegistry.peers()
        for (peer in peers) {
            broadcastScope.launch {
                try {
                    httpClient.post("http://$peer:8080/internal/dispatch-notify?queue=$queueName")
                } catch (e: Exception) {
                    log.debug("Peer notify failed for {}: {}", peer, e.message)
                }
            }
        }
    }

    /**
     * Called by the HTTP endpoint when a remote pod signals us.
     * Wakes local workers only — does NOT re-broadcast.
     */
    fun onRemoteSignal(queueName: String) {
        flowFor(queueName).tryEmit(Unit)
    }

    /**
     * Suspend until work is signaled or timeout expires.
     * Returns true if woken by signal, false on timeout.
     */
    suspend fun awaitWork(queueName: String, timeout: Duration): Boolean {
        return withTimeoutOrNull(timeout.toMillis()) {
            flowFor(queueName).first()
        } != null
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchNotifierTest" -pl WorkFlow`

Expected: All 7 tests pass.

- [ ] **Step 5: Commit**

```bash
git add pom.xml src/main/kotlin/worker/DispatchNotifier.kt src/test/kotlin/worker/DispatchNotifierTest.kt
git commit -m "feat: add DispatchNotifier with SharedFlow-based local signal and Ktor HTTP broadcast"
```

---

### Task 3: Implement PeerRegistry with Unit Tests

**Files:**
- Create: `src/main/kotlin/worker/PeerRegistry.kt`
- Create: `src/test/kotlin/worker/PeerRegistryTest.kt`

- [ ] **Step 1: Write failing tests for PeerRegistry**

Create `src/test/kotlin/worker/PeerRegistryTest.kt`:

```kotlin
package com.workflow.worker

import io.fabric8.kubernetes.api.model.EndpointAddress
import io.fabric8.kubernetes.api.model.EndpointSubset
import io.fabric8.kubernetes.api.model.Endpoints
import io.fabric8.kubernetes.api.model.ObjectMeta
import io.fabric8.kubernetes.client.Watcher
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PeerRegistryTest {

    private fun makeEndpoints(vararg ips: String): Endpoints {
        val addresses = ips.map { ip ->
            EndpointAddress().apply { this.ip = ip }
        }
        return Endpoints().apply {
            metadata = ObjectMeta().apply { name = "workflow-engine" }
            subsets = listOf(
                EndpointSubset().apply { this.addresses = addresses },
            )
        }
    }

    @Test
    fun `updateFromEndpoints populates peer list excluding self`() {
        val registry = PeerRegistry(myIp = "10.0.0.1")
        registry.updateFromEndpoints(makeEndpoints("10.0.0.1", "10.0.0.2", "10.0.0.3"))
        assertEquals(listOf("10.0.0.2", "10.0.0.3"), registry.peers())
    }

    @Test
    fun `self IP is excluded from peers`() {
        val registry = PeerRegistry(myIp = "10.0.0.5")
        registry.updateFromEndpoints(makeEndpoints("10.0.0.5"))
        assertTrue(registry.peers().isEmpty())
    }

    @Test
    fun `empty endpoints results in empty peer list`() {
        val registry = PeerRegistry(myIp = "10.0.0.1")
        val emptyEndpoints = Endpoints().apply {
            metadata = ObjectMeta().apply { name = "workflow-engine" }
            subsets = emptyList()
        }
        registry.updateFromEndpoints(emptyEndpoints)
        assertTrue(registry.peers().isEmpty())
    }

    @Test
    fun `null subsets results in empty peer list`() {
        val registry = PeerRegistry(myIp = "10.0.0.1")
        val nullSubsets = Endpoints().apply {
            metadata = ObjectMeta().apply { name = "workflow-engine" }
            subsets = null
        }
        registry.updateFromEndpoints(nullSubsets)
        assertTrue(registry.peers().isEmpty())
    }

    @Test
    fun `update replaces previous peer list`() {
        val registry = PeerRegistry(myIp = "10.0.0.1")
        registry.updateFromEndpoints(makeEndpoints("10.0.0.1", "10.0.0.2"))
        assertEquals(listOf("10.0.0.2"), registry.peers())

        registry.updateFromEndpoints(makeEndpoints("10.0.0.1", "10.0.0.3", "10.0.0.4"))
        assertEquals(listOf("10.0.0.3", "10.0.0.4"), registry.peers())
    }

    @Test
    fun `peers returns empty list before any update`() {
        val registry = PeerRegistry(myIp = "10.0.0.1")
        assertTrue(registry.peers().isEmpty())
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="PeerRegistryTest" -pl WorkFlow`

Expected: Compilation error — `PeerRegistry` class structure does not match yet.

- [ ] **Step 3: Implement PeerRegistry**

Create `src/main/kotlin/worker/PeerRegistry.kt`:

```kotlin
package com.workflow.worker

import com.workflow.config.FrameworkConfig
import com.workflow.leader.KubernetesDetector
import io.fabric8.kubernetes.api.model.Endpoints
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.Watcher
import io.fabric8.kubernetes.client.WatcherException
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import org.slf4j.LoggerFactory

@ApplicationScoped
class PeerRegistry(
    private val client: KubernetesClient,
    private val config: FrameworkConfig,
    private val k8sDetector: KubernetesDetector,
) {
    private val log = LoggerFactory.getLogger(PeerRegistry::class.java)

    @Volatile
    private var _peers: List<String> = emptyList()

    private val myIp: String get() = config.worker().podIp()

    fun peers(): List<String> = _peers

    fun start(@Observes ev: StartupEvent) {
        if (!k8sDetector.isRunningInKubernetes()) {
            log.info("Not running in Kubernetes, peer discovery disabled")
            return
        }
        val namespace = config.leaderElection().namespace()
        val serviceName = config.serviceName()
        log.info("Starting Endpoints watch for {}/{}", namespace, serviceName)

        client.endpoints()
            .inNamespace(namespace)
            .withName(serviceName)
            .watch(object : Watcher<Endpoints> {
                override fun eventReceived(action: Watcher.Action, endpoints: Endpoints) {
                    updateFromEndpoints(endpoints)
                    log.debug("Peer list updated ({}): {}", action, _peers)
                }

                override fun onClose(cause: WatcherException?) {
                    if (cause != null) {
                        log.warn("Endpoints watch closed, Fabric8 will reconnect", cause)
                    }
                }
            })
    }

    internal fun updateFromEndpoints(endpoints: Endpoints) {
        _peers = (endpoints.subsets ?: emptyList())
            .flatMap { subset -> (subset.addresses ?: emptyList()).map { it.ip } }
            .filter { it != myIp }
    }

    /** Test-only constructor that bypasses K8s client and CDI. */
    internal constructor(myIp: String) : this(
        client = io.fabric8.kubernetes.client.KubernetesClientBuilder().build(),
        config = object : FrameworkConfig {
            override fun worker() = object : FrameworkConfig.WorkerConfig {
                override fun id() = "test"
                override fun pollInterval() = java.time.Duration.ofSeconds(1)
                override fun fallbackPollInterval() = java.time.Duration.ofSeconds(5)
                override fun concurrency() = 4
                override fun batchSize() = 1
                override fun maxBatchSize() = 16
                override fun podIp() = myIp
            }
            override fun leaderElection() = throw UnsupportedOperationException()
            override fun shutdown() = throw UnsupportedOperationException()
            override fun sweeper() = throw UnsupportedOperationException()
            override fun serviceName() = "test"
        },
        k8sDetector = KubernetesDetector { false },
    )
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="PeerRegistryTest" -pl WorkFlow`

Expected: All 6 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/worker/PeerRegistry.kt src/test/kotlin/worker/PeerRegistryTest.kt
git commit -m "feat: add PeerRegistry with K8s Endpoints Watch for peer discovery"
```

---

### Task 4: Add Internal HTTP Endpoint

**Files:**
- Create: `src/main/kotlin/worker/DispatchNotifyResource.kt`

- [ ] **Step 1: Create the endpoint**

Create `src/main/kotlin/worker/DispatchNotifyResource.kt`:

```kotlin
package com.workflow.worker

import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.QueryParam

@Path("/internal/dispatch-notify")
class DispatchNotifyResource(
    private val notifier: DispatchNotifier,
) {
    @POST
    fun notify(@QueryParam("queue") queue: String?): String {
        notifier.onRemoteSignal(queue ?: "default")
        return "OK"
    }
}
```

- [ ] **Step 2: Run full test suite to verify no CDI wiring issues**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchNotifierTest,PeerRegistryTest" -pl WorkFlow`

Expected: All existing tests still pass. The endpoint is a simple bean — no integration test needed for the endpoint itself since it just delegates to `notifier.onRemoteSignal()` which is already tested.

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/worker/DispatchNotifyResource.kt
git commit -m "feat: add internal HTTP endpoint for cross-pod dispatch notification"
```

---

### Task 5: Integrate Notifier into BarrierService

**Files:**
- Modify: `src/main/kotlin/engine/BarrierService.kt`

The BarrierService needs to:
1. Accept `DispatchNotifier` as a constructor parameter
2. Return the queue name from `evaluateAndAdvance` so `onTaskCompleted` can signal after the transaction commits
3. Signal after the second transaction in `onTaskCompleted`

- [ ] **Step 1: Modify BarrierService to inject notifier and signal after advance**

Update `src/main/kotlin/engine/BarrierService.kt`:

```kotlin
package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.dsl.WorkflowDefinition
import com.workflow.extension.inTransactionSuspend
import com.workflow.worker.DispatchNotifier
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.temporal.ChronoUnit

@ApplicationScoped
class BarrierService(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val objectMapper: ObjectMapper,
    private val strategyRegistry: PhaseStrategyRegistry,
    private val notifier: DispatchNotifier,
) {
    private val log = LoggerFactory.getLogger(BarrierService::class.java)

    suspend fun onTaskCompleted(
        taskId: String,
        workflowId: String,
        sequenceNumber: Int,
        status: TaskStatus,
        resultJson: String?,
        claimedBy: String? = null,
        claimedAt: Instant? = null,
    ) {
        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            val updated = taskRepo.updateStatusWithHandle(handle, taskId, status, resultJson, claimedBy, claimedAt)
            if (!updated) return@inTransactionSuspend
        }

        var signalQueue: String? = null

        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
            if (nonTerminal > 0) return@inTransactionSuspend

            signalQueue = evaluateAndAdvance(handle, workflowId, sequenceNumber)
        }

        if (signalQueue != null) notifier.signal(signalQueue!!)
    }

    internal suspend fun recoverStuckWorkflow(workflowId: String) {
        var signalQueue: String? = null

        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            val workflow =
                workflowRepo.findByIdWithHandle(handle, workflowId)
                    ?: run {
                        log.warn("Workflow not found during recovery: {}", workflowId)
                        return@inTransactionSuspend
                    }
            if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend

            val seq = workflow.currentSequence
            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, seq)
            if (nonTerminal > 0) return@inTransactionSuspend

            val failedCount = taskRepo.countFailedWithHandle(handle, workflowId, seq)
            val totalCount = taskRepo.countTotalWithHandle(handle, workflowId, seq)
            signalQueue = resolveAndExecute(handle, workflow, seq, failedCount, totalCount)
        }

        if (signalQueue != null) notifier.signal(signalQueue!!)
    }

    /**
     * Returns the queue name of the next phase's tasks if advancement
     * occurred, or null if no advancement happened (CAS lost, no next phase,
     * or workflow completed/aborted).
     */
    private fun evaluateAndAdvance(
        handle: Handle,
        workflowId: String,
        sequenceNumber: Int,
    ): String? {
        val workflow =
            workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: throw IllegalStateException("Workflow not found: $workflowId")
        if (workflow.status != WorkflowStatus.RUNNING) return null
        if (sequenceNumber != workflow.currentSequence) return null

        val failedCount = taskRepo.countFailedWithHandle(handle, workflowId, sequenceNumber)
        val totalCount = taskRepo.countTotalWithHandle(handle, workflowId, sequenceNumber)

        return resolveAndExecute(handle, workflow, sequenceNumber, failedCount, totalCount)
    }

    private fun resolveAndExecute(
        handle: Handle,
        workflow: WorkflowRun,
        sequenceNumber: Int,
        failedCount: Int,
        totalCount: Int,
    ): String? {
        val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
        val sequenceMap = buildSequenceMap(definition)
        val seqInfo =
            sequenceMap[sequenceNumber]
                ?: throw IllegalStateException("Sequence $sequenceNumber not in definition for workflow ${workflow.id}")

        val strategy = strategyRegistry.resolve(seqInfo.phaseType)
        val context = PhaseContext(workflow, definition, seqInfo, sequenceMap, failedCount, totalCount)
        val decision = strategy.resolve(context)

        return executeDecision(handle, workflow, seqInfo, sequenceMap, decision)
    }

    /**
     * Returns the queue name of the inserted next-phase tasks, or null
     * if no tasks were inserted (CAS lost, workflow completed/aborted).
     */
    private fun executeDecision(
        handle: Handle,
        workflow: WorkflowRun,
        seqInfo: SequenceInfo,
        sequenceMap: Map<Int, SequenceInfo>,
        decision: AdvancementDecision,
    ): String? {
        when (decision) {
            is AdvancementDecision.Advance -> {
                val casWon =
                    workflowRepo.casAdvanceWithHandle(
                        handle,
                        workflow.id,
                        seqInfo.sequenceNumber,
                        decision.nextSequence,
                        workflow.version,
                    )
                if (!casWon) {
                    log.debug("CAS lost for workflow {} at sequence {}", workflow.id, seqInfo.sequenceNumber)
                    return null
                }
                val nextSeqInfo = sequenceMap[decision.nextSequence]!!
                val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
                when (nextSeqInfo.phaseType) {
                    PhaseType.PARALLEL -> {
                        taskRepo.insertFanOutFromScatter(
                            handle,
                            workflow.id,
                            seqInfo.sequenceNumber,
                            nextSeqInfo,
                            now,
                        )
                    }

                    PhaseType.LINEAR -> {
                        taskRepo.insertBatchWithHandle(
                            handle,
                            listOf(createTaskForActivity(workflow.id, nextSeqInfo.sequenceNumber, nextSeqInfo.activity, now)),
                        )
                    }
                }
                return nextSeqInfo.activity.queue
            }

            is AdvancementDecision.Complete -> {
                workflowRepo.updateStatusWithHandle(
                    handle,
                    workflow.id,
                    WorkflowStatus.COMPLETED,
                    expectedStatus = WorkflowStatus.RUNNING,
                )
                return null
            }

            is AdvancementDecision.Abort -> {
                log.warn("Workflow {} failed at sequence {}: {}", workflow.id, seqInfo.sequenceNumber, decision.reason)
                val updated =
                    workflowRepo.updateStatusWithHandle(
                        handle,
                        workflow.id,
                        WorkflowStatus.FAILED,
                        expectedStatus = WorkflowStatus.RUNNING,
                    )
                if (updated) {
                    taskRepo.cancelPendingTasksWithHandle(handle, workflow.id)
                }
                return null
            }
        }
    }
}
```

- [ ] **Step 2: Run BarrierService tests to verify no breakage**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="BarrierServiceTest" -pl WorkFlow`

Expected: Compilation errors — existing tests construct BarrierService without the new `notifier` parameter. Fix by adding a mock `DispatchNotifier` to the test setup. In `BarrierServiceTest`, add `private val notifier: DispatchNotifier = mock()` and pass it as the last constructor argument wherever `BarrierService(...)` is instantiated.

- [ ] **Step 3: Fix BarrierServiceTest constructor calls**

Add `mock<DispatchNotifier>()` to all `BarrierService(...)` instantiations in the test file. The exact locations depend on the test file structure — search for `BarrierService(` and append `, notifier` (or the mock) to each call.

- [ ] **Step 4: Run BarrierService tests again**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="BarrierServiceTest" -pl WorkFlow`

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/BarrierService.kt src/test/kotlin/engine/BarrierServiceTest.kt
git commit -m "feat: integrate DispatchNotifier into BarrierService, signal after phase advance"
```

---

### Task 6: Integrate Notifier into WorkflowEngine

**Files:**
- Modify: `src/main/kotlin/engine/WorkflowEngine.kt`

- [ ] **Step 1: Add notifier to WorkflowEngine and signal after startWorkflow**

Update `src/main/kotlin/engine/WorkflowEngine.kt`:

```kotlin
package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dsl.WorkflowDefinition
import com.workflow.extension.inTransactionSuspend
import com.workflow.worker.DispatchNotifier
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Jdbi
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

@ApplicationScoped
class WorkflowEngine(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val objectMapper: ObjectMapper,
    private val notifier: DispatchNotifier,
) {

    private val log = LoggerFactory.getLogger(WorkflowEngine::class.java)

    suspend fun startWorkflow(definition: WorkflowDefinition): String {
        require(definition.activities.isNotEmpty()) { "WorkflowDefinition must have at least one activity" }

        val workflowId = UUID.randomUUID().toString()
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
        val definitionJson = objectMapper.writeValueAsString(definition)
        val firstActivity = definition.activities.first()

        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
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
            workflowRepo.insertWithHandle(handle, run)

            val task = createTaskForActivity(
                workflowId = workflowId,
                sequenceNumber = 1,
                activity = firstActivity,
                now = now,
            )
            taskRepo.insertBatchWithHandle(handle, listOf(task))
        }

        notifier.signal(firstActivity.queue)
        log.info("Started workflow {} with {} activities", workflowId, definition.activities.size)
        return workflowId
    }

    suspend fun cancelWorkflow(workflowId: String): Boolean =
        jdbi.inTransactionSuspend<Boolean, Exception> { handle ->
            val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: return@inTransactionSuspend false
            if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend false

            val updated = workflowRepo.updateStatusWithHandle(
                handle, workflowId, WorkflowStatus.CANCELLED, expectedStatus = WorkflowStatus.RUNNING,
            )
            if (updated) {
                taskRepo.cancelPendingTasksWithHandle(handle, workflowId)
                log.info("Cancelled workflow {}", workflowId)
            }
            updated
        }

    suspend fun replayWorkflow(workflowId: String): Boolean =
        jdbi.inTransactionSuspend<Boolean, Exception> { handle ->
            val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: return@inTransactionSuspend false
            if (workflow.status != WorkflowStatus.FAILED) return@inTransactionSuspend false

            workflowRepo.updateStatusWithHandle(handle, workflowId, WorkflowStatus.RUNNING, WorkflowStatus.FAILED)
            taskRepo.replayDeadLetterBatchWithHandle(handle, workflowId)
            true
        }
}
```

- [ ] **Step 2: Fix WorkflowEngineTest constructor calls**

Add `mock<DispatchNotifier>()` to `WorkflowEngine(...)` instantiations in `WorkflowEngineTest.kt`. Search for `WorkflowEngine(` and add the notifier mock.

- [ ] **Step 3: Run WorkflowEngine tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowEngineTest" -pl WorkFlow`

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/engine/WorkflowEngine.kt src/test/kotlin/engine/WorkflowEngineTest.kt
git commit -m "feat: integrate DispatchNotifier into WorkflowEngine, signal after startWorkflow"
```

---

### Task 7: Modify WorkerLoop to Use DispatchNotifier

**Files:**
- Modify: `src/main/kotlin/worker/WorkerLoop.kt`

Two changes: (1) inject `DispatchNotifier`, replace `delay(pollInterval)` with `notifier.awaitWork()`, and (2) use `maxBatchSize` from config.

- [ ] **Step 1: Update WorkerLoop**

In `src/main/kotlin/worker/WorkerLoop.kt`, make these changes:

1. Add `DispatchNotifier` to constructor parameters (after `objectMapper`).
2. In `start()`, read `maxBatchSize` from config and replace `batchSize` usage.
3. In `pollAndProcess()`, replace `delay(pollInterval.toMillis())` with `notifier.awaitWork("default", fallbackPollInterval)`.

The updated constructor and `pollAndProcess`:

```kotlin
@ApplicationScoped
class WorkerLoop(
    private val config: FrameworkConfig,
    private val taskRepo: TaskRepository,
    private val handlerRegistry: HandlerRegistry,
    private val barrierService: BarrierService,
    private val meterRegistry: MeterRegistry,
    private val inputResolver: InputResolver,
    private val workflowRepo: WorkflowRepository,
    private val objectMapper: ObjectMapper,
    private val notifier: DispatchNotifier,
) : ShutdownParticipant {
```

In `start()`, change the `batchSize` and `pollInterval` reads:

```kotlin
    fun start(scope: CoroutineScope): Job {
        val workerConfig = config.worker()
        val workerId = workerConfig.id()
        val concurrency = workerConfig.concurrency()
        val fallbackPollInterval = workerConfig.fallbackPollInterval()
        val maxBatchSize = workerConfig.maxBatchSize()
        // ... meters setup unchanged ...

        val job =
            scope.launch(ShutdownSignal { !_accepting.get() }) {
                indefinitelyRepeat(Unit)
                    .takeUntilSignal(stopChannel)
                    .unorderedMapAsync(concurrency) { pollAndProcess(workerId, fallbackPollInterval, maxBatchSize) }
                    .collect {}
            }
        activeJob = job

        log.info("Worker loop started: workerId={}, concurrency={}, maxBatchSize={}, fallbackPollInterval={}", workerId, concurrency, maxBatchSize, fallbackPollInterval)
        return job
    }
```

Update `pollAndProcess`:

```kotlin
    private suspend fun pollAndProcess(
        workerId: String,
        fallbackPollInterval: Duration,
        maxBatchSize: Int,
    ) = withContext(MDCContext(mapOf("worker_id" to workerId))) {
        val tasks =
            try {
                taskRepo.claimNext(workerId, maxBatchSize)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.error("Failed to claim tasks", e)
                claimTotal("error").increment()
                notifier.awaitWork("default", fallbackPollInterval)
                return@withContext
            }
        _lastActivityTimestamp = Instant.now()

        if (tasks.isEmpty()) {
            claimTotal("empty").increment()
            notifier.awaitWork("default", fallbackPollInterval)
            return@withContext
        }

        claimTotal("success").increment()
        claimedTasksTotal.increment(tasks.size.toDouble())

        for (task in tasks) {
            processTask(task)
        }
    }
```

- [ ] **Step 2: Run WorkerLoopTest to see what breaks**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkerLoopTest" -pl WorkFlow`

Expected: Compilation errors — constructor now requires `notifier` parameter. Tests that mock `batchSize()` and `pollInterval()` need updating.

- [ ] **Step 3: Fix WorkerLoopTest**

In `src/test/kotlin/worker/WorkerLoopTest.kt`:

1. Add `private lateinit var notifier: DispatchNotifier` to the shared mocks section.
2. In `setup()`, add:
   ```kotlin
   notifier = mock()
   ```
3. Add config stubs for the new properties:
   ```kotlin
   whenever(workerConfig.fallbackPollInterval()).thenReturn(Duration.ofSeconds(5))
   whenever(workerConfig.maxBatchSize()).thenReturn(1)
   ```
4. Update the `WorkerLoop(...)` construction to include `notifier`:
   ```kotlin
   workerLoop = WorkerLoop(config, taskRepo, handlerRegistry, barrierService, meterRegistry, inputResolver, workflowRepo, objectMapper, notifier)
   ```
5. Update `claimNext` mock stubs from `eq(1)` (old batchSize) — the second parameter is now `maxBatchSize` which we set to `1` in tests, so the value stays the same.

- [ ] **Step 4: Run WorkerLoopTest again**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkerLoopTest" -pl WorkFlow`

Expected: All tests pass. Note: tests use `runTest` with virtual time. `notifier.awaitWork()` is mocked, so the suspend behavior is controlled. The `delay()` calls in tests that used `advanceTimeBy` will need adjustment — `awaitWork` on a mock returns immediately (default Mockito behavior returns `false` for Boolean), so the flow will keep looping. This is actually the desired behavior in tests: the mock notifier doesn't block.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/worker/WorkerLoop.kt src/test/kotlin/worker/WorkerLoopTest.kt
git commit -m "feat: replace delay(pollInterval) with notifier.awaitWork() in WorkerLoop"
```

---

### Task 8: Update RBAC Manifest

**Files:**
- Modify: `k8s/rbac.yaml`

- [ ] **Step 1: Add Endpoints permission to the Role**

Update `k8s/rbac.yaml`:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: workflow-engine-leader
  namespace: default  # Match framework.leader-election.namespace
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "create", "update"]
  - apiGroups: [""]
    resources: ["endpoints"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: workflow-engine-leader-binding
  namespace: default
subjects:
  - kind: ServiceAccount
    name: workflow-engine
    namespace: default
roleRef:
  kind: Role
  name: workflow-engine-leader
  apiGroup: rbac.authorization.k8s.io
```

- [ ] **Step 2: Commit**

```bash
git add k8s/rbac.yaml
git commit -m "feat: add Endpoints get/list/watch RBAC for peer discovery"
```

---

### Task 9: Fix Remaining Test Compilation and Run Full Suite

**Files:**
- Modify: Various test files that construct BarrierService or WorkflowEngine directly

- [ ] **Step 1: Find all test files that instantiate BarrierService or WorkflowEngine**

Search for `BarrierService(` and `WorkflowEngine(` across test files. Each needs the `notifier` mock added. Key files likely include:
- `src/test/kotlin/engine/BarrierServiceTest.kt`
- `src/test/kotlin/engine/WorkflowEngineTest.kt`
- `src/test/kotlin/engine/WorkflowIntegrationTest.kt`
- `src/test/kotlin/stress/StressTestBase.kt`
- `src/test/kotlin/engine/SweeperTest.kt`

For each: add `private val notifier: DispatchNotifier = mock()` (or use the existing mock from earlier tasks) and pass it to the constructor.

- [ ] **Step 2: Run the full test suite (excluding benchmarks)**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="!ThroughputBenchmarkTest" -pl WorkFlow`

Expected: All tests pass. Fix any remaining compilation errors from the constructor changes.

- [ ] **Step 3: Commit all remaining test fixes**

```bash
git add -A
git commit -m "fix: update all test files for DispatchNotifier constructor parameter"
```

---

### Task 10: Run Throughput Benchmarks and Verify Improvement

**Files:**
- Read: `src/test/kotlin/stress/ThroughputBenchmarkTest.kt`
- Read: `src/test/kotlin/stress/StressTestBase.kt`

This task verifies the dispatch latency improvement. The benchmarks already measure wall-clock time and tasks/sec. With the notifier integrated, phase transitions should be near-instant instead of waiting for poll intervals.

- [ ] **Step 1: Ensure StressTestBase passes the notifier to BarrierService and WorkflowEngine**

Check `StressTestBase.kt` — it constructs the engine components directly. Add a real `DispatchNotifier` (with an empty `PeerRegistry` since stress tests are single-JVM):

```kotlin
val peerRegistry = PeerRegistry(myIp = "localhost")  // test constructor, no K8s
val notifier = DispatchNotifier(peerRegistry)
```

Pass `notifier` to both `BarrierService(...)` and `WorkflowEngine(...)` and `WorkerLoop(...)` constructors in the stress test base.

- [ ] **Step 2: Run B1 benchmark**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="ThroughputBenchmarkTest#B1*" -pl WorkFlow -Dtag=benchmark`

Expected: Results print to stdout. Compare p50/p95 latency with previous runs — dispatch latency should be significantly lower.

- [ ] **Step 3: Run B2 and B3 benchmarks**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="ThroughputBenchmarkTest#B2*,ThroughputBenchmarkTest#B3*" -pl WorkFlow -Dtag=benchmark`

Expected: B2 (fan-out) and B3 (multi-phase pipeline) show improved throughput due to instant dispatch between phases.

- [ ] **Step 4: Commit benchmark integration**

```bash
git add -A
git commit -m "feat: integrate DispatchNotifier into stress tests and verify throughput improvement"
```

---

## Execution Summary

| Task | Description | Estimated Size |
|------|-------------|---------------|
| 1 | Config properties | Small (1 file + props) |
| 2 | DispatchNotifier + tests | Medium (2 files, 7 tests) |
| 3 | PeerRegistry + tests | Medium (2 files, 6 tests) |
| 4 | HTTP endpoint | Small (1 file) |
| 5 | BarrierService integration | Medium (modify + fix tests) |
| 6 | WorkflowEngine integration | Small (modify + fix tests) |
| 7 | WorkerLoop changes | Medium (modify + fix tests) |
| 8 | RBAC manifest | Small (1 file) |
| 9 | Fix all remaining tests | Medium (search & fix) |
| 10 | Benchmark verification | Medium (integration) |

**Total: 3 new source files, 2 new test files, ~7 modified files, 0 schema changes.**
