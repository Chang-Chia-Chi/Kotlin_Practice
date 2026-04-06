# K8s Launch-and-Watch: Phase 4 — WorkerLoop LaunchK8sJob Branch

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire `KubernetesJobPort` into `WorkerLoop`. Add a `LaunchK8sJob` branch that calls the port then defers the task, with full error coverage matching the existing `Completed` branch pattern.

**Architecture:** TDD — write failing tests first, then add the new constructor parameter and `when` branch. The `WorkerLoop` converts `LaunchK8sJob` into a `taskRepo.defer()` call with `triggerType=k8s-job` — same DB path as before, now triggered by launch rather than by handler pre-arrangement.

**Tech Stack:** Kotlin, Mockito-Kotlin, kotlinx-coroutines-test

**Prerequisite:** Phases 1–3 complete — `HandlerResult.LaunchK8sJob`, `KubernetesJobPort`, `K8sJobLauncherAdapter`, rewritten `K8sJobTriggerDriver` all exist.

---

### Task 1: Write Failing WorkerLoop LaunchK8sJob Tests

**Files:**
- Modify: `src/test/kotlin/worker/usecase/service/execution/WorkerLoopTest.kt`

- [ ] **Step 1: Add `kubernetesJobPort` mock to the test class and update all WorkerLoop constructor calls**

In the `@BeforeEach` `setup()` method, add the mock declaration and pass it to the constructor. Also update the 3 other `WorkerLoop(...)` instantiations in the file (lines ~639, ~693, ~873) to include the new parameter.

Add field to the class:
```kotlin
private lateinit var kubernetesJobPort: com.workflow.worker.usecase.port.outbound.KubernetesJobPort
```

In `setup()`, before creating `workerLoop`, add:
```kotlin
kubernetesJobPort = mock()
```

Update the `workerLoop` constructor call (currently line ~126):
```kotlin
workerLoop = WorkerLoop(workerConfig, shutdownConfig, taskRepo, handlerRegistry, taskSettler, meterRegistry, activityInputResolver, workflowRepo, objectMapper, notifier, kubernetesJobPort)
```

Update the `shortTimeoutLoop` constructor call (currently line ~639):
```kotlin
val shortTimeoutLoop = WorkerLoop(workerConfig, shutdownConfig, taskRepo, handlerRegistry, taskSettler, meterRegistry, activityInputResolver, workflowRepo, objectMapper, notifier, kubernetesJobPort)
```

Update the `freshLoop` constructor call (currently line ~693):
```kotlin
val freshLoop = WorkerLoop(workerConfig, shutdownConfig, taskRepo, handlerRegistry, taskSettler, meterRegistry, activityInputResolver, workflowRepo, objectMapper, notifier, kubernetesJobPort)
```

Update the `batchLoop` constructor call (currently line ~873):
```kotlin
val batchLoop = WorkerLoop(batchWorkerConfig, shutdownConfig, taskRepo, handlerRegistry, taskSettler, meterRegistry, activityInputResolver, workflowRepo, objectMapper, notifier, kubernetesJobPort)
```

- [ ] **Step 2: Add 5 LaunchK8sJob branch tests**

Add these tests in the same nested class that held the `Defer` branch tests (search for the class containing `handler returning Defer` tests — they have been deleted but the nested class likely remains):

```kotlin
@Test
fun `handler returning LaunchK8sJob calls port launch then taskRepo defer`() = runTest {
    val launchTask = makeTask(handlerKey = "launch-handler")
    taskRepo.stub { onBlocking { claimNext(any(), any(), any()) } doReturn listOf(launchTask) doReturn emptyList() }

    val launchHandler = object : TransitionHandler {
        override fun key(): String = "launch-handler"
        override suspend fun execute(input: HandlerInput): HandlerResult =
            HandlerResult.LaunchK8sJob(
                jobName = "dispatch-join-abc",
                namespace = "default",
                image = "img:1",
            )
    }
    whenever(handlerRegistry.resolve("launch-handler")).thenReturn(launchHandler)
    taskRepo.stub { onBlocking { defer(any(), any(), any()) } doReturn true }

    startAndAdvance(this)

    verify(kubernetesJobPort).launch(
        HandlerResult.LaunchK8sJob(jobName = "dispatch-join-abc", namespace = "default", image = "img:1"),
    )
    verify(taskRepo).defer(
        eq(launchTask.id),
        eq("k8s-job"),
        eq("""{"jobName":"dispatch-join-abc","namespace":"default"}"""),
    )
    verify(phaseGate, never()).onTaskCompleted(any(), any(), any(), any(), any(), any(), any(), isNull())
}

@Test
fun `handler returning LaunchK8sJob when port launch throws delegates to handleTaskFailure`() = runTest {
    val launchTask = makeTask(handlerKey = "launch-handler")
    taskRepo.stub { onBlocking { claimNext(any(), any(), any()) } doReturn listOf(launchTask) doReturn emptyList() }

    val launchHandler = object : TransitionHandler {
        override fun key(): String = "launch-handler"
        override suspend fun execute(input: HandlerInput): HandlerResult =
            HandlerResult.LaunchK8sJob(jobName = "j1", namespace = "ns", image = "img:1")
    }
    whenever(handlerRegistry.resolve("launch-handler")).thenReturn(launchHandler)
    whenever(kubernetesJobPort.launch(any())).thenThrow(RuntimeException("K8s API unavailable"))

    startAndAdvance(this)

    verify(kubernetesJobPort).launch(any())
    verify(taskRepo, never()).defer(any(), any(), any())
    // handleTaskFailure calls resetForRetry (retryCount=0 < maxRetries=3)
    verify(taskRepo).resetForRetry(eq(launchTask.id), any())
}

@Test
fun `handler returning LaunchK8sJob when defer returns false falls through to handleTaskFailure`() = runTest {
    val launchTask = makeTask(handlerKey = "launch-handler")
    taskRepo.stub { onBlocking { claimNext(any(), any(), any()) } doReturn listOf(launchTask) doReturn emptyList() }

    val launchHandler = object : TransitionHandler {
        override fun key(): String = "launch-handler"
        override suspend fun execute(input: HandlerInput): HandlerResult =
            HandlerResult.LaunchK8sJob(jobName = "j1", namespace = "ns", image = "img:1")
    }
    whenever(handlerRegistry.resolve("launch-handler")).thenReturn(launchHandler)
    taskRepo.stub { onBlocking { defer(any(), any(), any()) } doReturn false }

    startAndAdvance(this)

    verify(kubernetesJobPort).launch(any())
    verify(taskRepo).defer(eq(launchTask.id), any(), any())
    verify(taskRepo).resetForRetry(eq(launchTask.id), any())
}

@Test
fun `handler returning LaunchK8sJob when defer throws delegates to handleTaskFailure`() = runTest {
    val launchTask = makeTask(handlerKey = "launch-handler")
    taskRepo.stub { onBlocking { claimNext(any(), any(), any()) } doReturn listOf(launchTask) doReturn emptyList() }

    val launchHandler = object : TransitionHandler {
        override fun key(): String = "launch-handler"
        override suspend fun execute(input: HandlerInput): HandlerResult =
            HandlerResult.LaunchK8sJob(jobName = "j1", namespace = "ns", image = "img:1")
    }
    whenever(handlerRegistry.resolve("launch-handler")).thenReturn(launchHandler)
    taskRepo.stub { onBlocking { defer(any(), any(), any()) } doThrow RuntimeException("DB timeout") }

    startAndAdvance(this)

    verify(kubernetesJobPort).launch(any())
    verify(taskRepo).defer(eq(launchTask.id), any(), any())
    verify(taskRepo).resetForRetry(eq(launchTask.id), any())
}

@Test
fun `handler returning LaunchK8sJob when retries exhausted and defer fails reports FAILED`() = runTest {
    val launchTask = makeTask(handlerKey = "launch-handler", retryCount = 3, maxRetries = 3)
    taskRepo.stub { onBlocking { claimNext(any(), any(), any()) } doReturn listOf(launchTask) doReturn emptyList() }

    val launchHandler = object : TransitionHandler {
        override fun key(): String = "launch-handler"
        override suspend fun execute(input: HandlerInput): HandlerResult =
            HandlerResult.LaunchK8sJob(jobName = "j1", namespace = "ns", image = "img:1")
    }
    whenever(handlerRegistry.resolve("launch-handler")).thenReturn(launchHandler)
    taskRepo.stub { onBlocking { defer(any(), any(), any()) } doReturn false }

    startAndAdvance(this)

    verify(taskRepo).defer(eq(launchTask.id), any(), any())
    verify(taskRepo, never()).resetForRetry(any(), any())
    verify(phaseGate).onTaskCompleted(
        eq(launchTask.id), any(), any(), eq(TaskStatus.FAILED), isNull(), any(), any(), any(),
    )
}
```

- [ ] **Step 3: Compile — expect error (WorkerLoop constructor signature mismatch)**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow`
Expected: Compile errors — `WorkerLoop` constructor does not yet accept `kubernetesJobPort`.

---

### Task 2: Update WorkerLoop

**Files:**
- Modify: `src/main/kotlin/worker/usecase/service/execution/WorkerLoop.kt`

- [ ] **Step 1: Add KubernetesJobPort to constructor and imports**

Add import:
```kotlin
import com.workflow.worker.usecase.port.outbound.KubernetesJobPort
import com.workflow.worker.usecase.port.inbound.trigger.TriggerTypes
```

Add constructor parameter (last in the list):
```kotlin
@ApplicationScoped
class WorkerLoop(
    private val workerLoopConfig: WorkerLoopConfig,
    private val shutdownConfig: ShutdownConfig,
    private val taskRepo: TaskRepository,
    private val handlerRegistry: HandlerRegistry,
    private val taskSettler: TaskSettler,
    private val meterRegistry: MeterRegistry,
    private val activityInputResolver: ActivityInputResolver,
    private val workflowRepo: WorkflowRepository,
    private val objectMapper: ObjectMapper,
    private val notifier: WorkerNotifier,
    private val kubernetesJobPort: KubernetesJobPort,
) : ShutdownParticipant {
```

- [ ] **Step 2: Add LaunchK8sJob branch to executeAndReport()**

Replace the `when (result)` block in `executeAndReport()`:

```kotlin
when (result) {
    is HandlerResult.Completed -> {
        try {
            taskSettler.settle(
                taskId = task.id,
                workflowId = task.workflowId,
                sequenceNumber = task.sequenceNumber,
                status = TaskStatus.COMPLETED,
                resultJson = result.result,
                itemsJson = result.items,
                claimedBy = task.claimedBy,
                claimedAt = task.claimedAt,
            )
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error("Barrier failed for COMPLETED task {}, falling through to failure path", task.id, e)
            handleTaskFailure(task, e)
        }
    }

    is HandlerResult.LaunchK8sJob -> {
        try {
            kubernetesJobPort.launch(result)
            val deferred = taskRepo.defer(
                taskId = task.id,
                triggerType = TriggerTypes.K8S_JOB,
                triggerMeta = """{"jobName":"${result.jobName}","namespace":"${result.namespace}"}""",
            )
            if (deferred) {
                log.info("Task {} launched K8s job {} and deferred", task.id, result.jobName)
            } else {
                log.warn("Task {} launch-defer failed (status was not PROCESSING)", task.id)
                handleTaskFailure(task, IllegalStateException("Defer failed: task not in PROCESSING state"))
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error("LaunchK8sJob failed for task {}", task.id, e)
            handleTaskFailure(task, e)
        }
    }
}
```

- [ ] **Step 3: Run WorkerLoopTest — confirm all pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="WorkerLoopTest"`
Expected: All PASS.

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/worker/usecase/service/execution/WorkerLoop.kt
git add src/test/kotlin/worker/usecase/service/execution/WorkerLoopTest.kt
git commit -m "feat: WorkerLoop handles LaunchK8sJob — launch then defer"
```

---

### Task 3: Full Suite and Final Commit

- [ ] **Step 1: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: All PASS.

- [ ] **Step 2: Final commit (if any stray files remain unstaged)**

```bash
git status
git add -p   # review and stage any remaining changes
git commit -m "chore: k8s launch-and-watch phase 4 cleanup"
```
