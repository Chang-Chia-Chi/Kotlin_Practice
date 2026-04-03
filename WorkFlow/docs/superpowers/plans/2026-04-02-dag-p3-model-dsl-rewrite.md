# DAG Refactor — P3: Model + DSL + Repository Overhaul

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the linear-list model with the DAG model. Rewrite `ActivityDefinition`, `WorkflowDefinition`, `SequenceInfo`, `buildSequenceMap`, DSL builders, `WorkflowRun`, `Task`. Update repositories to match V2 schema. Stub `DefaultPhaseGate` so the code compiles. Delete the obsolete strategy layer. All unit tests pass at the end.

**Architecture:** This is the largest plan — it changes many files simultaneously because the model types cascade everywhere. Each task within this plan is still small (one file at a time). Integration tests remain broken until P4–P5; unit tests must all pass after each task.

**Tech Stack:** Kotlin 2.3, JDBI 3, JUnit 5, Jackson

---

### Overview of files changed in this plan

**Create:**
- (none — Edge.kt and FanOutDefinition.kt already created in P1)

**Modify/Rewrite:**
- `src/main/kotlin/workflow/model/ActivityDefinition.kt`
- `src/main/kotlin/workflow/model/WorkflowDefinition.kt`
- `src/main/kotlin/workflow/model/SequenceModel.kt`
- `src/main/kotlin/workflow/model/Task.kt`
- `src/main/kotlin/workflow/model/WorkflowRun.kt`
- `src/main/kotlin/workflow/dsl/WorkflowDslBuilders.kt`
- `src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt` (stubbed)
- `src/main/kotlin/workflow/usecase/service/orchestration/WorkflowEngine.kt`
- `src/main/kotlin/workflow/usecase/port/outbound/persistent/WorkflowRepository.kt`
- `src/main/kotlin/workflow/adapter/persistent/JdbiWorkflowRepository.kt`
- `src/main/kotlin/workflow/adapter/persistent/JdbiTaskRepository.kt`

**Delete:**
- `src/main/kotlin/workflow/model/PhaseContext.kt`
- `src/main/kotlin/workflow/model/AdvancementDecision.kt`
- `src/main/kotlin/workflow/usecase/port/inbound/phase/AdvancementStrategy.kt`
- `src/main/kotlin/workflow/usecase/service/phase/LinearAdvancementStrategy.kt`
- `src/main/kotlin/workflow/usecase/service/phase/ParallelAdvancementStrategy.kt`
- `src/main/kotlin/workflow/usecase/service/phase/AdvancementStrategyRegistry.kt`

**Test files — Rewrite:**
- `src/test/kotlin/workflow/model/SequenceModelTest.kt`
- `src/test/kotlin/workflow/dsl/WorkflowDslBuildersTest.kt`
- `src/test/kotlin/workflow/dsl/WorkflowDslTest.kt`
- `src/test/kotlin/workflow/model/WorkflowModelsTest.kt`

**Test files — Delete:**
- `src/test/kotlin/workflow/usecase/service/phase/LinearAdvancementStrategyTest.kt`
- `src/test/kotlin/workflow/usecase/service/phase/ParallelAdvancementStrategyTest.kt`
- `src/test/kotlin/workflow/usecase/service/phase/AdvancementStrategyRegistryTest.kt`

**Test files — Update helpers only:**
- `src/test/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGateTest.kt`
- `src/test/kotlin/workflow/usecase/service/orchestration/WorkflowEngineTest.kt`
- `src/test/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdogTest.kt`
- `src/test/kotlin/workflow/adapter/persistent/RepositoryTest.kt`
- `src/test/kotlin/stress/StressTestBase.kt`

---

### Task 1: Rewrite `ActivityDefinition` and `WorkflowDefinition`

**Files:**
- Modify: `src/main/kotlin/workflow/model/ActivityDefinition.kt`
- Modify: `src/main/kotlin/workflow/model/WorkflowDefinition.kt`

- [ ] **Step 1: Replace `ActivityDefinition.kt`**

```kotlin
package com.workflow.workflow.model

import java.time.Duration

data class ActivityDefinition(
    val name: String,
    val transition: String,
    val retries: Int = 0,
    val failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    val deadline: Duration = Duration.ofMinutes(30),
    val fanOut: FanOutDefinition? = null,
    val backoffBase: Duration = Duration.ofSeconds(1),
    val backoffCap: Duration = Duration.ofSeconds(300),
    val queue: String = "default",
    val inputs: Map<String, String> = emptyMap(),
    val successors: List<Edge> = emptyList(),
) {
    val isTerminal: Boolean get() = successors.isEmpty() && fanOut == null
}
```

- [ ] **Step 2: Replace `WorkflowDefinition.kt`**

```kotlin
package com.workflow.workflow.model

import java.time.Duration

data class WorkflowDefinition(
    val activities: Map<String, ActivityDefinition>,
    val start: String,
    val deadline: Duration = Duration.ofHours(1),
) {
    init {
        require(activities.isNotEmpty()) { "Workflow must have at least one activity" }
        require(deadline > Duration.ZERO) { "Workflow deadline must be positive" }
        require(start in activities) { "Start activity '$start' not found in activities" }

        for ((name, activity) in activities) {
            for (edge in activity.successors) {
                require(edge.target in activities) {
                    "Activity '$name' has edge to unknown activity '${edge.target}'"
                }
            }
        }

        for ((name, activity) in activities) {
            require(!(activity.failurePolicy == FailurePolicy.BEST_EFFORT &&
                    activity.successors.any { it.label != DEFAULT_BRANCH })) {
                "Activity '$name': BEST_EFFORT policy is incompatible with conditional (on()) successors"
            }
        }

        for ((name, activity) in activities) {
            require(!(activity.fanOut != null &&
                    activity.successors.any { it.label != DEFAULT_BRANCH })) {
                "Activity '$name': fanOut cannot be combined with conditional successors"
            }
        }

        require(activities.values.any { it.isTerminal }) {
            "Workflow must have at least one terminal activity (no successors and no fanOut)"
        }

        // Cycle detection + unreachable check
        val reachable = topologicalSort(this)
        val unreachable = activities.keys - reachable.toSet()
        require(unreachable.isEmpty()) { "Unreachable activities: $unreachable" }
    }
}

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
        result.add(0, name)
    }

    visit(definition.start)
    return result
}
```

- [ ] **Step 3: Verify compile**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl WorkFlow`

Expected: FAIL — many files reference old `activities: List` or old `ActivityDefinition` fields. This is expected; we fix them in subsequent steps.

---

### Task 2: Rewrite `SequenceModel.kt`

**Files:**
- Modify: `src/main/kotlin/workflow/model/SequenceModel.kt`

- [ ] **Step 1: Replace `SequenceModel.kt`**

**Critical:** The PARALLEL phase `SequenceInfo.activity` must use a *synthetic* `ActivityDefinition` built from `FanOutDefinition` settings — specifically `fanOut.transition` becomes the `handlerKey` for parallel worker tasks. Without this, parallel tasks would incorrectly use the scatter handler.

```kotlin
package com.workflow.workflow.model

enum class PhaseType { LINEAR, SCATTER, PARALLEL }

data class SequenceInfo(
    val sequenceNumber: Int,
    val activityName: String,
    val activity: ActivityDefinition,
    val phaseType: PhaseType,
    val predecessorSequences: List<Int>,
)

fun buildSequenceMap(definition: WorkflowDefinition): Map<Int, SequenceInfo> {
    val topoOrder = topologicalSort(definition)

    // Build predecessor map: activityName → list of activity names that have edges to it
    val predecessorNames: Map<String, MutableList<String>> =
        definition.activities.keys.associateWith { mutableListOf() }
    for ((actName, activity) in definition.activities) {
        for (edge in activity.successors) {
            predecessorNames[edge.target]!!.add(actName)
        }
    }

    // Assign sequence numbers in topological order
    var seqCounter = 1
    val linearSeq = mutableMapOf<String, Int>()
    val scatterSeq = mutableMapOf<String, Int>()
    val parallelSeq = mutableMapOf<String, Int>()

    for (actName in topoOrder) {
        val activity = definition.activities[actName]!!
        if (activity.fanOut != null) {
            scatterSeq[actName] = seqCounter++
            parallelSeq[actName] = seqCounter++
        } else {
            linearSeq[actName] = seqCounter++
        }
    }

    // The "output seq" of a predecessor: what successors must wait for
    fun outputSeq(name: String): Int = parallelSeq[name] ?: linearSeq[name]!!

    val map = mutableMapOf<Int, SequenceInfo>()

    for (actName in topoOrder) {
        val activity = definition.activities[actName]!!
        val predSeqs = predecessorNames[actName]!!.map { outputSeq(it) }

        if (activity.fanOut != null) {
            val sSeq = scatterSeq[actName]!!
            val pSeq = parallelSeq[actName]!!

            map[sSeq] = SequenceInfo(
                sequenceNumber = sSeq,
                activityName = actName,
                activity = activity,
                phaseType = PhaseType.SCATTER,
                predecessorSequences = predSeqs,
            )

            // Synthetic activity for parallel tasks — uses FanOutDefinition settings.
            // The transition (handlerKey) for each parallel worker comes from fanOut.transition.
            // The scatter activity's own failurePolicy applies at join evaluation time.
            val fanOut = activity.fanOut!!
            val parallelActivity = ActivityDefinition(
                name = "$actName.__parallel__",
                transition = fanOut.transition,
                retries = fanOut.retries,
                failurePolicy = activity.failurePolicy, // scatter activity's policy governs join failure
                deadline = fanOut.deadline,
                backoffBase = fanOut.backoffBase,
                backoffCap = fanOut.backoffCap,
                queue = fanOut.queue,
            )
            map[pSeq] = SequenceInfo(
                sequenceNumber = pSeq,
                activityName = "$actName.__parallel__",
                activity = parallelActivity,
                phaseType = PhaseType.PARALLEL,
                predecessorSequences = listOf(sSeq),
            )
        } else {
            val seq = linearSeq[actName]!!
            map[seq] = SequenceInfo(
                sequenceNumber = seq,
                activityName = actName,
                activity = activity,
                phaseType = PhaseType.LINEAR,
                predecessorSequences = predSeqs,
            )
        }
    }

    return map
}
```

---

### Task 3: Update `WorkflowRun` and `Task`

**Files:**
- Modify: `src/main/kotlin/workflow/model/WorkflowRun.kt`
- Modify: `src/main/kotlin/workflow/model/Task.kt`

- [ ] **Step 1: Replace `WorkflowRun.kt`**

```kotlin
package com.workflow.workflow.model

import java.time.Instant

data class WorkflowRun(
    val id: String,
    val definitionJson: String,
    val version: Int,
    val status: WorkflowStatus,
    val createdAt: Instant,
    val updatedAt: Instant,
    val deadlineAt: Instant,
)
```

- [ ] **Step 2: Replace `Task.kt`** (add `activityName`, update `createTaskForActivity`)

```kotlin
package com.workflow.workflow.model

import java.time.Instant
import java.util.UUID

data class Task(
    val id: String,
    val workflowId: String,
    val activityName: String,
    val sequenceNumber: Int,
    val status: TaskStatus,
    val handlerKey: String,
    val item: String? = null,
    val resultJson: String?,
    val claimedBy: String?,
    val claimedAt: Instant?,
    val completedAt: Instant?,
    val retryCount: Int,
    val maxRetries: Int,
    val deadlineAt: Instant?,
    val notBefore: Instant? = null,
    val backoffBase: Int = 1,
    val backoffCap: Int = 300,
    val enqueuedAt: Instant = Instant.EPOCH,
    val queueName: String = "default",
)

internal fun createTaskForActivity(
    workflowId: String,
    activityName: String,
    sequenceNumber: Int,
    activity: ActivityDefinition,
    now: Instant,
    item: String? = null,
): Task = Task(
    id = UUID.randomUUID().toString(),
    workflowId = workflowId,
    activityName = activityName,
    sequenceNumber = sequenceNumber,
    status = TaskStatus.PENDING,
    handlerKey = activity.transition,
    item = item,
    resultJson = null,
    claimedBy = null,
    claimedAt = null,
    completedAt = null,
    retryCount = 0,
    maxRetries = activity.retries,
    deadlineAt = now.plus(activity.deadline),
    backoffBase = activity.backoffBase.seconds.toInt(),
    backoffCap = activity.backoffCap.seconds.toInt(),
    queueName = activity.queue,
)

internal fun createSkippedTaskForActivity(
    workflowId: String,
    activityName: String,
    sequenceNumber: Int,
    activity: ActivityDefinition,
    now: Instant,
): Task = Task(
    id = UUID.randomUUID().toString(),
    workflowId = workflowId,
    activityName = activityName,
    sequenceNumber = sequenceNumber,
    status = TaskStatus.SKIPPED,
    handlerKey = activity.transition,
    item = null,
    resultJson = null,
    claimedBy = null,
    claimedAt = null,
    completedAt = now,
    retryCount = 0,
    maxRetries = 0,
    deadlineAt = null,
    queueName = activity.queue,
)
```

---

### Task 4: Delete obsolete strategy layer

- [ ] **Step 1: Delete `PhaseContext.kt`**

Delete: `src/main/kotlin/workflow/model/PhaseContext.kt`

- [ ] **Step 2: Delete `AdvancementDecision.kt`**

Delete: `src/main/kotlin/workflow/model/AdvancementDecision.kt`

- [ ] **Step 3: Delete strategy interface and implementations**

Delete: `src/main/kotlin/workflow/usecase/port/inbound/phase/AdvancementStrategy.kt`
Delete: `src/main/kotlin/workflow/usecase/service/phase/LinearAdvancementStrategy.kt`
Delete: `src/main/kotlin/workflow/usecase/service/phase/ParallelAdvancementStrategy.kt`
Delete: `src/main/kotlin/workflow/usecase/service/phase/AdvancementStrategyRegistry.kt`

- [ ] **Step 4: Delete strategy unit tests**

Delete: `src/test/kotlin/workflow/usecase/service/phase/LinearAdvancementStrategyTest.kt`
Delete: `src/test/kotlin/workflow/usecase/service/phase/ParallelAdvancementStrategyTest.kt`
Delete: `src/test/kotlin/workflow/usecase/service/phase/AdvancementStrategyRegistryTest.kt`

---

### Task 5: Stub `DefaultPhaseGate` so it compiles

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt`

- [ ] **Step 1: Replace `DefaultPhaseGate.kt` with a stub**

```kotlin
package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.worker.usecase.port.outbound.notification.WorkerNotifier
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.usecase.port.inbound.orchestration.PhaseGate
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Jdbi
import java.time.Instant

@ApplicationScoped
class DefaultPhaseGate(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val objectMapper: ObjectMapper,
    private val notifier: WorkerNotifier,
) : PhaseGate {

    override suspend fun onTaskCompleted(
        taskId: String,
        workflowId: String,
        sequenceNumber: Int,
        status: TaskStatus,
        resultJson: String?,
        claimedBy: String?,
        claimedAt: Instant?,
    ) {
        throw UnsupportedOperationException("DefaultPhaseGate rewritten in Plan 4 (dag-p4-phase-gate)")
    }

    override suspend fun recoverStuckWorkflow(workflowId: String) {
        throw UnsupportedOperationException("DefaultPhaseGate rewritten in Plan 4 (dag-p4-phase-gate)")
    }
}
```

---

### Task 6: Rewrite DSL builders

**Files:**
- Modify: `src/main/kotlin/workflow/dsl/WorkflowDslBuilders.kt`

- [ ] **Step 1: Replace `WorkflowDslBuilders.kt`**

```kotlin
package com.workflow.workflow.dsl

import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.DEFAULT_BRANCH
import com.workflow.workflow.model.Edge
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.FanOutDefinition
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.WorkflowDefinition
import java.time.Duration

@DslMarker
annotation class WorkflowDsl

@WorkflowDsl
class InputsBuilder {
    private val entries = mutableMapOf<String, String>()

    infix fun String.from(ref: String) { entries[this] = ref }

    fun build(): Map<String, String> = entries.toMap()
}

@WorkflowDsl
class BranchBuilder {
    private val targets = mutableListOf<String>()

    fun next(t: String) { targets += t }

    fun buildEdges(label: String): List<Edge> = targets.map { Edge(it, label) }
}

@WorkflowDsl
class FanOutBuilder {
    private var transition: String? = null
    private var retries: Int = 0
    private var failurePolicy: FailurePolicy = FailurePolicy.ABORT
    private var deadline: Duration = Duration.ofMinutes(30)
    private var joinPolicy: JoinPolicy = JoinPolicy.All
    private var backoffBase: Duration = Duration.ofSeconds(1)
    private var backoffCap: Duration = Duration.ofSeconds(300)
    private var queue: String = "default"

    fun transition(t: String) { transition = t }
    fun retries(n: Int) { retries = n }
    fun failurePolicy(p: FailurePolicy) { failurePolicy = p }
    fun deadline(d: Duration) { deadline = d }
    fun joinPolicy(p: JoinPolicy) { joinPolicy = p }
    fun backoffBase(d: Duration) { backoffBase = d }
    fun backoffCap(d: Duration) { backoffCap = d }
    fun queue(q: String) { queue = q }

    fun build(): FanOutDefinition {
        requireNotNull(transition) { "fanOut transition is required" }
        return FanOutDefinition(
            transition = transition!!,
            retries = retries,
            failurePolicy = failurePolicy,
            deadline = deadline,
            joinPolicy = joinPolicy,
            backoffBase = backoffBase,
            backoffCap = backoffCap,
            queue = queue,
        )
    }
}

@WorkflowDsl
class ActivityBuilder(private val name: String) {
    private var transition: String? = null
    private var retries: Int = 0
    private var failurePolicy: FailurePolicy = FailurePolicy.ABORT
    private var deadline: Duration = Duration.ofMinutes(30)
    private var backoffBase: Duration = Duration.ofSeconds(1)
    private var backoffCap: Duration = Duration.ofSeconds(300)
    private var queue: String = "default"
    private var inputsDef: Map<String, String> = emptyMap()
    private var fanOutDef: FanOutDefinition? = null
    private val successorEdges = mutableListOf<Edge>()
    private var hasConditional = false
    private var hasUnconditional = false

    fun transition(t: String) { transition = t }
    fun retries(n: Int) { retries = n }
    fun failurePolicy(p: FailurePolicy) { failurePolicy = p }
    fun deadline(d: Duration) { deadline = d }
    fun backoffBase(d: Duration) { backoffBase = d }
    fun backoffCap(d: Duration) { backoffCap = d }
    fun queue(q: String) { queue = q }

    fun inputs(block: InputsBuilder.() -> Unit) {
        inputsDef = InputsBuilder().apply(block).build()
    }

    fun next(target: String) {
        require(!hasConditional) {
            "Activity '$name': cannot mix next() and on() — use one or the other"
        }
        hasUnconditional = true
        successorEdges += Edge(target, DEFAULT_BRANCH)
    }

    fun on(label: String, block: BranchBuilder.() -> Unit) {
        require(!hasUnconditional) {
            "Activity '$name': cannot mix next() and on() — use one or the other"
        }
        hasConditional = true
        successorEdges += BranchBuilder().apply(block).buildEdges(label)
    }

    fun fanOut(block: FanOutBuilder.() -> Unit) {
        fanOutDef = FanOutBuilder().apply(block).build()
    }

    fun build(): ActivityDefinition {
        requireNotNull(transition) { "Activity '$name' transition is required" }
        return ActivityDefinition(
            name = name,
            transition = transition!!,
            retries = retries,
            failurePolicy = failurePolicy,
            deadline = deadline,
            fanOut = fanOutDef,
            backoffBase = backoffBase,
            backoffCap = backoffCap,
            queue = queue,
            inputs = inputsDef,
            successors = successorEdges.toList(),
        )
    }
}

@WorkflowDsl
class WorkflowBuilder {
    private val activities = mutableMapOf<String, ActivityDefinition>()
    private var startName: String? = null
    private var deadline: Duration = Duration.ofHours(1)

    fun start(name: String) { startName = name }

    fun activity(name: String, block: ActivityBuilder.() -> Unit) {
        if (startName == null) startName = name  // first activity is default start
        activities[name] = ActivityBuilder(name).apply(block).build()
    }

    fun deadline(d: Duration) { deadline = d }

    fun build(): WorkflowDefinition {
        require(activities.isNotEmpty()) { "Workflow must have at least one activity" }
        return WorkflowDefinition(
            activities = activities.toMap(),
            start = requireNotNull(startName) { "Workflow start activity is required" },
            deadline = deadline,
        )
    }
}

fun workflow(block: WorkflowBuilder.() -> Unit): WorkflowDefinition =
    WorkflowBuilder().apply(block).build()
```

---

### Task 7: Update `WorkflowRepository` interface and `JdbiWorkflowRepository`

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/port/outbound/persistent/WorkflowRepository.kt`
- Modify: `src/main/kotlin/workflow/adapter/persistent/JdbiWorkflowRepository.kt`

- [ ] **Step 1: Replace `WorkflowRepository.kt`**

```kotlin
package com.workflow.workflow.usecase.port.outbound.persistent

import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import org.jdbi.v3.core.Handle
import java.time.Duration

interface WorkflowRepository {
    suspend fun insert(run: WorkflowRun)
    suspend fun findById(id: String): WorkflowRun?
    suspend fun casVersion(id: String, expectedVersion: Int): Boolean
    suspend fun updateStatus(id: String, newStatus: WorkflowStatus, expectedStatus: WorkflowStatus): Boolean
    suspend fun findStuck(gracePeriod: Duration): List<WorkflowRun>
    suspend fun findTimedOut(): List<WorkflowRun>

    fun insertWithHandle(handle: Handle, run: WorkflowRun)
    fun findByIdWithHandle(handle: Handle, id: String): WorkflowRun?
    fun casVersionWithHandle(handle: Handle, id: String, expectedVersion: Int): Boolean
    fun updateStatusWithHandle(handle: Handle, id: String, newStatus: WorkflowStatus, expectedStatus: WorkflowStatus): Boolean
    fun mergeIdempotentWithHandle(handle: Handle, run: WorkflowRun, idempotencyKey: String): Pair<String, Boolean>
}
```

- [ ] **Step 2: Replace `JdbiWorkflowRepository.kt`**

```kotlin
package com.workflow.workflow.adapter.persistent

import com.workflow.infrastructure.persistence.caseInsensitive
import com.workflow.infrastructure.persistence.inTransactionSuspend
import com.workflow.infrastructure.persistence.readClob
import com.workflow.infrastructure.persistence.readTimestamp
import com.workflow.infrastructure.persistence.withHandleSuspend
import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

@ApplicationScoped
class JdbiWorkflowRepository(private val jdbi: Jdbi) : WorkflowRepository {

    override suspend fun insert(run: WorkflowRun) {
        jdbi.withHandleSuspend<Unit, Exception> { h: Handle -> insertWithHandle(h, run) }
    }

    override suspend fun findById(id: String): WorkflowRun? =
        jdbi.withHandleSuspend<WorkflowRun?, Exception> { h: Handle -> findByIdWithHandle(h, id) }

    override suspend fun casVersion(id: String, expectedVersion: Int): Boolean =
        jdbi.inTransactionSuspend<Boolean, Exception> { h: Handle ->
            casVersionWithHandle(h, id, expectedVersion)
        }

    override suspend fun updateStatus(id: String, newStatus: WorkflowStatus, expectedStatus: WorkflowStatus): Boolean =
        jdbi.inTransactionSuspend<Boolean, Exception> { h: Handle ->
            updateStatusWithHandle(h, id, newStatus, expectedStatus)
        }

    override suspend fun findStuck(gracePeriod: Duration): List<WorkflowRun> =
        jdbi.withHandleSuspend<List<WorkflowRun>, Exception> { h: Handle ->
            // Placeholder: full DAG-aware stuck detection implemented in Plan 5
            emptyList()
        }

    override suspend fun findTimedOut(): List<WorkflowRun> =
        jdbi.withHandleSuspend<List<WorkflowRun>, Exception> { h: Handle ->
            h.createQuery("SELECT * FROM workflow WHERE status = 'RUNNING' AND deadline_at < :now")
                .bind("now", LocalDateTime.now(ZoneOffset.UTC))
                .mapToMap()
                .list()
                .map(::mapWorkflowRow)
        }

    override fun insertWithHandle(handle: Handle, run: WorkflowRun) {
        handle.createUpdate(
            """
            INSERT INTO workflow (id, definition, version, status, created_at, updated_at, deadline_at)
            VALUES (:id, :definition, :version, :status, :createdAt, :updatedAt, :deadlineAt)
            """,
        )
            .bind("id", run.id)
            .bind("definition", run.definitionJson)
            .bind("version", run.version)
            .bind("status", run.status.name)
            .bind("createdAt", LocalDateTime.ofInstant(run.createdAt, ZoneOffset.UTC))
            .bind("updatedAt", LocalDateTime.ofInstant(run.updatedAt, ZoneOffset.UTC))
            .bind("deadlineAt", LocalDateTime.ofInstant(run.deadlineAt, ZoneOffset.UTC))
            .execute()
    }

    override fun findByIdWithHandle(handle: Handle, id: String): WorkflowRun? =
        handle.createQuery("SELECT * FROM workflow WHERE id = :id")
            .bind("id", id)
            .mapToMap()
            .findOne()
            .map(::mapWorkflowRow)
            .orElse(null)

    override fun casVersionWithHandle(handle: Handle, id: String, expectedVersion: Int): Boolean {
        val count = handle.createUpdate(
            """
            UPDATE workflow
            SET version = version + 1, updated_at = :now
            WHERE id = :id AND version = :expectedVersion AND status = 'RUNNING'
            """,
        )
            .bind("id", id)
            .bind("expectedVersion", expectedVersion)
            .bind("now", LocalDateTime.now(ZoneOffset.UTC))
            .execute()
        return count == 1
    }

    override fun updateStatusWithHandle(
        handle: Handle,
        id: String,
        newStatus: WorkflowStatus,
        expectedStatus: WorkflowStatus,
    ): Boolean {
        WorkflowStatus.requireTransition(expectedStatus, newStatus)
        val count = handle.createUpdate(
            "UPDATE workflow SET status = :status, updated_at = :now WHERE id = :id AND status = :expectedStatus",
        )
            .bind("id", id)
            .bind("status", newStatus.name)
            .bind("expectedStatus", expectedStatus.name)
            .bind("now", LocalDateTime.now(ZoneOffset.UTC))
            .execute()
        return count == 1
    }

    override fun mergeIdempotentWithHandle(handle: Handle, run: WorkflowRun, idempotencyKey: String): Pair<String, Boolean> {
        val count = handle.createUpdate(
            """
            MERGE INTO workflow w
            USING (SELECT :idemKey AS idem_key FROM dual) src
            ON (w.idempotency_key = src.idem_key)
            WHEN NOT MATCHED THEN INSERT
                (id, idempotency_key, definition, version, status, created_at, updated_at, deadline_at)
            VALUES (:id, :idemKey, :definition, :version, :status, :createdAt, :updatedAt, :deadlineAt)
            """,
        )
            .bind("idemKey", idempotencyKey)
            .bind("id", run.id)
            .bind("definition", run.definitionJson)
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

    private fun mapWorkflowRow(row: Map<String, Any?>): WorkflowRun {
        val ci = caseInsensitive(row)
        return WorkflowRun(
            id = ci["ID"] as String,
            definitionJson = readClob(ci["DEFINITION"]),
            version = (ci["VERSION"] as Number).toInt(),
            status = WorkflowStatus.valueOf(ci["STATUS"] as String),
            createdAt = readTimestamp(ci["CREATED_AT"]),
            updatedAt = readTimestamp(ci["UPDATED_AT"]),
            deadlineAt = readTimestamp(ci["DEADLINE_AT"]),
        )
    }
}
```

---

### Task 8: Update `JdbiTaskRepository` to persist `activityName`

**Files:**
- Modify: `src/main/kotlin/workflow/adapter/persistent/JdbiTaskRepository.kt`

- [ ] **Step 1: Update `insertBatchWithHandle` to include `activity_name`**

In `JdbiTaskRepository.insertBatchWithHandle`, change the INSERT SQL and add `activityName` binding:

```kotlin
override fun insertBatchWithHandle(handle: Handle, tasks: List<Task>) {
    if (tasks.isEmpty()) return
    val batch = handle.prepareBatch(
        """
        INSERT INTO task (id, workflow_id, activity_name, sequence_number, status, handler_key,
                          item, result, claimed_by, claimed_at, completed_at,
                          retry_count, max_retries, deadline_at, not_before, backoff_base, backoff_cap, queue_name)
        VALUES (:id, :workflowId, :activityName, :sequenceNumber, :status, :handlerKey,
                :item, :result, :claimedBy, :claimedAt, :completedAt,
                :retryCount, :maxRetries, :deadlineAt, :notBefore, :backoffBase, :backoffCap, :queueName)
        """,
    )
    for (task in tasks) {
        batch
            .bind("id", task.id)
            .bind("workflowId", task.workflowId)
            .bind("activityName", task.activityName)
            .bind("sequenceNumber", task.sequenceNumber)
            .bind("status", task.status.name)
            .bind("handlerKey", task.handlerKey)
        bindNullableClob(batch, "item", task.item)
        bindNullableClob(batch, "result", task.resultJson)
        batch.bind("claimedBy", task.claimedBy)
        bindNullableTimestamp(batch, "claimedAt", task.claimedAt)
        bindNullableTimestamp(batch, "completedAt", task.completedAt)
        batch
            .bind("retryCount", task.retryCount)
            .bind("maxRetries", task.maxRetries)
        bindNullableTimestamp(batch, "deadlineAt", task.deadlineAt)
        bindNullableTimestamp(batch, "notBefore", task.notBefore)
        batch
            .bind("backoffBase", task.backoffBase)
            .bind("backoffCap", task.backoffCap)
            .bind("queueName", task.queueName)
        batch.add()
    }
    batch.execute()
}
```

- [ ] **Step 2: Update `mapTaskRow` to read `activity_name`**

In `mapTaskRow`, add the `activityName` field:

```kotlin
private fun mapTaskRow(row: Map<String, Any?>): Task {
    val ci = caseInsensitive(row)
    return Task(
        id = ci["ID"] as String,
        workflowId = ci["WORKFLOW_ID"] as String,
        activityName = (ci["ACTIVITY_NAME"] as? String) ?: "",
        sequenceNumber = (ci["SEQUENCE_NUMBER"] as Number).toInt(),
        status = TaskStatus.valueOf(ci["STATUS"] as String),
        handlerKey = ci["HANDLER_KEY"] as String,
        item = ci["ITEM"]?.let { readClob(it) },
        resultJson = ci["RESULT"]?.let { readClob(it) },
        claimedBy = ci["CLAIMED_BY"] as String?,
        claimedAt = readNullableTimestamp(ci["CLAIMED_AT"]),
        completedAt = readNullableTimestamp(ci["COMPLETED_AT"]),
        retryCount = (ci["RETRY_COUNT"] as Number).toInt(),
        maxRetries = (ci["MAX_RETRIES"] as Number).toInt(),
        deadlineAt = readNullableTimestamp(ci["DEADLINE_AT"]),
        notBefore = readNullableTimestamp(ci["NOT_BEFORE"]),
        backoffBase = (ci["BACKOFF_BASE"] as Number).toInt(),
        backoffCap = (ci["BACKOFF_CAP"] as Number).toInt(),
        enqueuedAt = readTimestamp(ci["ENQUEUED_AT"]),
        queueName = (ci["QUEUE_NAME"] as? String) ?: "default",
    )
}
```

---

### Task 9: Update `WorkflowEngine` for new types

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/service/orchestration/WorkflowEngine.kt`

- [ ] **Step 1: Update `startWorkflow` in `WorkflowEngine.kt`**

Replace the `startWorkflow` method:

```kotlin
override suspend fun startWorkflow(
    definition: WorkflowDefinition,
    idempotencyKey: String?,
): StartResult {
    require(definition.activities.isNotEmpty()) { "WorkflowDefinition must have at least one activity" }

    val workflowId = UUID.randomUUID().toString()
    val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
    val definitionJson = objectMapper.writeValueAsString(definition)

    val sequenceMap = buildSequenceMap(definition)
    val startSeqInfo = sequenceMap[1]!! // start activity always gets seq 1 from topo sort

    val run = WorkflowRun(
        id = workflowId,
        definitionJson = definitionJson,
        version = 0,
        status = WorkflowStatus.RUNNING,
        createdAt = now,
        updatedAt = now,
        deadlineAt = now.plus(definition.deadline),
    )

    if (idempotencyKey == null) {
        val queueName = jdbi.inTransactionSuspend<String, Exception> { handle ->
            workflowRepo.insertWithHandle(handle, run)
            val task = createTaskForActivity(workflowId, startSeqInfo.activityName, 1, startSeqInfo.activity, now)
            taskRepo.insertBatchWithHandle(handle, listOf(task))
            startSeqInfo.activity.queue
        }
        notifier.signal(queueName)
        log.info("Started workflow {} with {} activities", workflowId, definition.activities.size)
        return StartResult.Created(workflowId)
    }

    val (mergeId, created, queueName) = jdbi.inTransactionSuspend<Triple<String, Boolean, String?>, Exception> { handle ->
        val (mId, isNew) = workflowRepo.mergeIdempotentWithHandle(handle, run, idempotencyKey)
        if (isNew) {
            val task = createTaskForActivity(mId, startSeqInfo.activityName, 1, startSeqInfo.activity, now)
            taskRepo.insertBatchWithHandle(handle, listOf(task))
            Triple(mId, true, startSeqInfo.activity.queue)
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

Also add import for `buildSequenceMap`:
```kotlin
import com.workflow.workflow.model.buildSequenceMap
import com.workflow.workflow.model.createTaskForActivity
```

---

### Task 10: Rewrite unit tests

**Files:**
- Modify: `src/test/kotlin/workflow/model/SequenceModelTest.kt`
- Modify: `src/test/kotlin/workflow/dsl/WorkflowDslBuildersTest.kt`
- Modify: `src/test/kotlin/workflow/dsl/WorkflowDslTest.kt`
- Modify: `src/test/kotlin/workflow/model/WorkflowModelsTest.kt`

- [ ] **Step 1: Rewrite `SequenceModelTest.kt`** (spec items 1–11)

```kotlin
package com.workflow.workflow.model

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SequenceModelTest {

    // ── Spec item 1: Linear chain ─────────────────────────────────────────

    @Test
    fun `linear chain produces correct sequence numbers and predecessors`() {
        val def = workflow {
            activity("a") { transition("a.h"); next("b") }
            activity("b") { transition("b.h") }
        }
        val map = buildSequenceMap(def)

        assertEquals(2, map.size)
        val a = map[1]!!
        assertEquals("a", a.activityName)
        assertEquals(PhaseType.LINEAR, a.phaseType)
        assertEquals(emptyList(), a.predecessorSequences)

        val b = map[2]!!
        assertEquals("b", b.activityName)
        assertEquals(PhaseType.LINEAR, b.phaseType)
        assertEquals(listOf(1), b.predecessorSequences)
    }

    // ── Spec item 2: Fork ─────────────────────────────────────────────────

    @Test
    fun `fork gives B and C different seq numbers, D predecessors are both`() {
        val def = workflow {
            activity("a") { transition("a.h"); next("b"); next("c") }
            activity("b") { transition("b.h"); next("d") }
            activity("c") { transition("c.h"); next("d") }
            activity("d") { transition("d.h") }
        }
        val map = buildSequenceMap(def)

        assertEquals(4, map.size)
        val seqA = map.values.first { it.activityName == "a" }.sequenceNumber
        val seqB = map.values.first { it.activityName == "b" }.sequenceNumber
        val seqC = map.values.first { it.activityName == "c" }.sequenceNumber
        val seqD = map.values.first { it.activityName == "d" }.sequenceNumber

        assertTrue(seqB != seqC, "B and C must have different sequence numbers")
        assertEquals(setOf(seqB, seqC), map[seqD]!!.predecessorSequences.toSet())
    }

    // ── Spec item 3: Conditional — same shape as fork, edge labels recorded

    @Test
    fun `conditional edges have correct labels on successors`() {
        val def = workflow {
            activity("validate") {
                transition("v.h")
                on("OK") { next("charge") }
                on("INVALID") { next("reject") }
            }
            activity("charge") { transition("c.h") }
            activity("reject") { transition("r.h") }
        }
        val map = buildSequenceMap(def)
        assertEquals(3, map.size)

        val validate = def.activities["validate"]!!
        val okEdge = validate.successors.first { it.target == "charge" }
        val invalidEdge = validate.successors.first { it.target == "reject" }
        assertEquals("OK", okEdge.label)
        assertEquals("INVALID", invalidEdge.label)
    }

    // ── Spec item 4: Fan-out → SCATTER at N, PARALLEL at N+1 ─────────────

    @Test
    fun `fan-out produces SCATTER at N and PARALLEL at N+1`() {
        val def = workflow {
            activity("scatter") {
                transition("s.h")
                fanOut { transition("p.h"); joinPolicy(JoinPolicy.All) }
                next("join")
            }
            activity("join") { transition("j.h") }
        }
        val map = buildSequenceMap(def)

        assertEquals(3, map.size)
        val scatter = map.values.first { it.activityName == "scatter" }
        val parallel = map.values.first { it.activityName == "scatter.__parallel__" }
        val join = map.values.first { it.activityName == "join" }

        assertEquals(PhaseType.SCATTER, scatter.phaseType)
        assertEquals(PhaseType.PARALLEL, parallel.phaseType)
        assertEquals(scatter.sequenceNumber + 1, parallel.sequenceNumber)
        assertEquals(listOf(parallel.sequenceNumber), join.predecessorSequences)
    }

    // ── Spec item 5: Fan-out inside DAG ───────────────────────────────────

    @Test
    fun `fan-out inside DAG has correct predecessor chain`() {
        val def = workflow {
            activity("start") { transition("s.h"); next("scatter") }
            activity("scatter") {
                transition("sc.h")
                fanOut { transition("p.h") }
                next("end")
            }
            activity("end") { transition("e.h") }
        }
        val map = buildSequenceMap(def)

        assertEquals(4, map.size) // start, scatter(SCATTER), scatter(PARALLEL), end
        val startSeq = map.values.first { it.activityName == "start" }.sequenceNumber
        val scatterSeq = map.values.first { it.activityName == "scatter" }.sequenceNumber
        val parallelSeq = map.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber
        val endSeq = map.values.first { it.activityName == "end" }.sequenceNumber

        assertEquals(listOf(startSeq), map[scatterSeq]!!.predecessorSequences)
        assertEquals(listOf(scatterSeq), map[parallelSeq]!!.predecessorSequences)
        assertEquals(listOf(parallelSeq), map[endSeq]!!.predecessorSequences)
    }

    // ── Spec item 6: Cycle detection ──────────────────────────────────────

    @Test
    fun `cycle in activity graph is rejected at build time`() {
        assertFailsWith<IllegalArgumentException> {
            WorkflowDefinition(
                activities = mapOf(
                    "a" to ActivityDefinition("a", "a.h", successors = listOf(Edge("b"))),
                    "b" to ActivityDefinition("b", "b.h", successors = listOf(Edge("a"))),
                ),
                start = "a",
            )
        }
    }

    // ── Spec item 7: Unreachable activity rejected ────────────────────────

    @Test
    fun `unreachable activity is rejected at build time`() {
        assertFailsWith<IllegalArgumentException> {
            WorkflowDefinition(
                activities = mapOf(
                    "a" to ActivityDefinition("a", "a.h"),
                    "orphan" to ActivityDefinition("orphan", "orphan.h"),
                ),
                start = "a",
            )
        }
    }

    // ── Spec item 8: Unknown edge target rejected ─────────────────────────

    @Test
    fun `unknown edge target is rejected at build time`() {
        assertFailsWith<IllegalArgumentException> {
            WorkflowDefinition(
                activities = mapOf(
                    "a" to ActivityDefinition("a", "a.h", successors = listOf(Edge("nonexistent"))),
                ),
                start = "a",
            )
        }
    }

    // ── Spec item 9: BEST_EFFORT + on() rejected ──────────────────────────

    @Test
    fun `BEST_EFFORT with conditional successors is rejected at build time`() {
        assertFailsWith<IllegalArgumentException> {
            WorkflowDefinition(
                activities = mapOf(
                    "a" to ActivityDefinition(
                        "a", "a.h",
                        failurePolicy = FailurePolicy.BEST_EFFORT,
                        successors = listOf(Edge("b", "OK")),
                    ),
                    "b" to ActivityDefinition("b", "b.h"),
                ),
                start = "a",
            )
        }
    }

    // ── Spec item 10: fanOut + on() rejected ──────────────────────────────

    @Test
    fun `fanOut with conditional successors is rejected at build time`() {
        assertFailsWith<IllegalArgumentException> {
            WorkflowDefinition(
                activities = mapOf(
                    "a" to ActivityDefinition(
                        "a", "a.h",
                        fanOut = FanOutDefinition("p.h"),
                        successors = listOf(Edge("b", "OK")),
                    ),
                    "b" to ActivityDefinition("b", "b.h"),
                ),
                start = "a",
            )
        }
    }

    // ── Spec item 11: no start → reject ───────────────────────────────────

    @Test
    fun `missing start activity is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            WorkflowDefinition(
                activities = mapOf("a" to ActivityDefinition("a", "a.h")),
                start = "nonexistent",
            )
        }
    }
}
```

Add imports to `SequenceModelTest.kt`:
```kotlin
import com.workflow.workflow.dsl.workflow
import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.Edge
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.FanOutDefinition
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.buildSequenceMap
```

- [ ] **Step 2: Rewrite `WorkflowDslBuildersTest.kt`**

Replace entire file:

```kotlin
package com.workflow.workflow.dsl

import com.workflow.workflow.model.DEFAULT_BRANCH
import com.workflow.workflow.model.Edge
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.FanOutDefinition
import com.workflow.workflow.model.JoinPolicy
import java.time.Duration
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class WorkflowDslBuildersTest {

    // ── Spec item 31: Linear workflow ────────────────────────────────────

    @Test
    fun `linear workflow builds correctly`() {
        val def = workflow {
            activity("step-1") {
                transition("process.step1")
                retries(2)
                failurePolicy(FailurePolicy.ABORT)
                deadline(Duration.ofMinutes(10))
                next("step-2")
            }
            activity("step-2") {
                transition("process.step2")
                failurePolicy(FailurePolicy.BEST_EFFORT)
            }
        }

        assertEquals("step-1", def.start)
        assertEquals(2, def.activities.size)

        val first = def.activities["step-1"]!!
        assertEquals("process.step1", first.transition)
        assertEquals(2, first.retries)
        assertEquals(FailurePolicy.ABORT, first.failurePolicy)
        assertEquals(Duration.ofMinutes(10), first.deadline)
        assertNull(first.fanOut)
        assertEquals(listOf(Edge("step-2", DEFAULT_BRANCH)), first.successors)

        val second = def.activities["step-2"]!!
        assertEquals("process.step2", second.transition)
        assertTrue(second.successors.isEmpty())
    }

    // ── Spec item 32: Conditional workflow ───────────────────────────────

    @Test
    fun `conditional workflow builds with correct edge labels`() {
        val def = workflow {
            activity("validate") {
                transition("v.h")
                on("OK") { next("charge") }
                on("INVALID") { next("reject") }
            }
            activity("charge") { transition("c.h") }
            activity("reject") { transition("r.h") }
        }

        val validate = def.activities["validate"]!!
        assertEquals(2, validate.successors.size)
        val okEdge = validate.successors.first { it.label == "OK" }
        val invalidEdge = validate.successors.first { it.label == "INVALID" }
        assertEquals("charge", okEdge.target)
        assertEquals("reject", invalidEdge.target)
    }

    // ── Spec item 33: Unconditional fork ─────────────────────────────────

    @Test
    fun `fork builds with multiple DEFAULT_BRANCH edges`() {
        val def = workflow {
            activity("prepare") {
                transition("p.h")
                next("send-email")
                next("update-crm")
            }
            activity("send-email") { transition("e.h") }
            activity("update-crm") { transition("c.h") }
        }

        val prepare = def.activities["prepare"]!!
        assertEquals(2, prepare.successors.size)
        assertTrue(prepare.successors.all { it.label == DEFAULT_BRANCH })
        assertEquals(setOf("send-email", "update-crm"), prepare.successors.map { it.target }.toSet())
    }

    // ── Spec item 34: Fan-out with FanOutDefinition ───────────────────────

    @Test
    fun `fan-out builds with FanOutDefinition embedded and next() as successor`() {
        val def = workflow {
            activity("scatter") {
                transition("DispatchScatterHandler")
                fanOut {
                    transition("DispatchSimulationHandler")
                    retries(2)
                    joinPolicy(JoinPolicy.All)
                }
                next("join")
            }
            activity("join") { transition("DispatchJoinHandler") }
        }

        val scatter = def.activities["scatter"]!!
        assertNotNull(scatter.fanOut)
        assertEquals("DispatchSimulationHandler", scatter.fanOut!!.transition)
        assertEquals(2, scatter.fanOut!!.retries)
        assertEquals(JoinPolicy.All, scatter.fanOut!!.joinPolicy)
        assertEquals(listOf(Edge("join", DEFAULT_BRANCH)), scatter.successors)
    }

    // ── Spec item 35: Migrated dispatchWorkflow builds ────────────────────

    @Test
    fun `migrated dispatchWorkflow builds and scatter batchToken resolves from scatter`() {
        val def = workflow {
            start("scatter")
            activity("scatter") {
                transition("DispatchScatterHandler")
                fanOut {
                    transition("DispatchSimulationHandler")
                    retries(2)
                    joinPolicy(JoinPolicy.All)
                }
                next("join")
            }
            activity("join") {
                transition("DispatchJoinHandler")
                deadline(Duration.ofMinutes(10))
                inputs { "batchToken" from "scatter.batchToken" }
            }
        }

        assertEquals("scatter", def.start)
        assertEquals("scatter.batchToken", def.activities["join"]!!.inputs["batchToken"])
    }

    // ── Spec item 36: Mixed on() + next() is rejected ────────────────────

    @Test
    fun `mixing on() and next() on same activity is rejected at build time`() {
        assertFailsWith<IllegalArgumentException> {
            workflow {
                activity("a") {
                    transition("a.h")
                    next("b")
                    on("OK") { next("c") }
                }
                activity("b") { transition("b.h") }
                activity("c") { transition("c.h") }
            }
        }
    }

    // ── Additional DSL tests ──────────────────────────────────────────────

    @Test
    fun `missing transition throws`() {
        assertFailsWith<IllegalArgumentException> {
            workflow {
                activity("step") { retries(1) }
            }
        }
    }

    @Test
    fun `empty workflow throws`() {
        assertFailsWith<IllegalArgumentException> {
            workflow { }
        }
    }

    @Test
    fun `workflow deadline defaults to 1 hour`() {
        val def = workflow {
            activity("step") { transition("h") }
        }
        assertEquals(Duration.ofHours(1), def.deadline)
    }

    @Test
    fun `workflow deadline customizable`() {
        val def = workflow {
            deadline(Duration.ofMinutes(30))
            activity("step") { transition("h") }
        }
        assertEquals(Duration.ofMinutes(30), def.deadline)
    }

    @Test
    fun `inputs DSL works on new builder`() {
        val def = workflow {
            activity("step") {
                transition("h")
                inputs {
                    "x" from "prev.field"
                    "y" from "prev"
                }
            }
        }
        val inputs = def.activities["step"]!!.inputs
        assertEquals("prev.field", inputs["x"])
        assertEquals("prev", inputs["y"])
    }

    @Test
    fun `BranchBuilder supports multiple next() calls for fork on label`() {
        val def = workflow {
            activity("charge") {
                transition("c.h")
                on("SUCCESS") { next("notify"); next("audit") }
                on("FAILED") { next("reject") }
            }
            activity("notify") { transition("n.h") }
            activity("audit")  { transition("a.h") }
            activity("reject") { transition("r.h") }
        }
        val charge = def.activities["charge"]!!
        val successEdges = charge.successors.filter { it.label == "SUCCESS" }
        assertEquals(2, successEdges.size)
        assertEquals(setOf("notify", "audit"), successEdges.map { it.target }.toSet())
    }
}
```

- [ ] **Step 3: Rewrite `WorkflowDslTest.kt`** (Jackson serialization round-trip)

Replace entire file:

```kotlin
package com.workflow.workflow.dsl

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.DEFAULT_BRANCH
import com.workflow.workflow.model.Edge
import com.workflow.workflow.model.FanOutDefinition
import com.workflow.workflow.model.WorkflowDefinition
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals

class WorkflowDslTest {

    private val mapper = jacksonObjectMapper().registerModule(JavaTimeModule())

    private inline fun <reified T> roundTrip(value: T): T {
        val json = mapper.writeValueAsString(value)
        return mapper.readValue(json)
    }

    @Test
    fun `linear workflow serialization round-trip`() {
        val def = workflow {
            activity("step-1") {
                transition("process.step1")
                retries(2)
                next("step-2")
            }
            activity("step-2") { transition("process.step2") }
        }
        assertEquals(def, roundTrip(def))
    }

    @Test
    fun `conditional workflow round-trip`() {
        val def = workflow {
            activity("validate") {
                transition("v.h")
                on("OK") { next("charge") }
                on("INVALID") { next("reject") }
            }
            activity("charge") { transition("c.h") }
            activity("reject") { transition("r.h") }
        }
        assertEquals(def, roundTrip(def))
    }

    @Test
    fun `fan-out workflow round-trip preserves FanOutDefinition`() {
        val def = workflow {
            activity("scatter") {
                transition("sc.h")
                fanOut { transition("par.h"); retries(2) }
                next("join")
            }
            activity("join") { transition("j.h") }
        }
        val restored = roundTrip(def)
        assertEquals(def, restored)
        assertEquals("par.h", restored.activities["scatter"]!!.fanOut!!.transition)
    }
}
```

- [ ] **Step 4: Update `WorkflowModelsTest.kt`** — Remove `WorkflowRun.currentSequence` usages

In `WorkflowModelsTest.kt`:

Replace the `workflowRun()` helper:
```kotlin
private fun workflowRun(
    id: String = "wf-1",
    definitionJson: String = """{"activities":{}}""",
    version: Int = 0,
    status: WorkflowStatus = WorkflowStatus.RUNNING,
    createdAt: Instant = now,
    updatedAt: Instant = now,
    deadlineAt: Instant = later,
) = WorkflowRun(id, definitionJson, version, status, createdAt, updatedAt, deadlineAt)
```

Replace the `WorkflowRun construction preserves all fields` test:
```kotlin
@Test
fun `WorkflowRun construction preserves all fields`() {
    val run = workflowRun()
    assertEquals("wf-1", run.id)
    assertEquals("""{"activities":{}}""", run.definitionJson)
    assertEquals(0, run.version)
    assertEquals(WorkflowStatus.RUNNING, run.status)
    assertEquals(now, run.createdAt)
    assertEquals(now, run.updatedAt)
    assertEquals(later, run.deadlineAt)
}
```

Update `task()` helper to include `activityName`:
```kotlin
private fun task(
    id: String = "task-1",
    workflowId: String = "wf-1",
    activityName: String = "step1",
    sequenceNumber: Int = 1,
    status: TaskStatus = TaskStatus.PENDING,
    handlerKey: String = "process.step1",
    item: String? = null,
    resultJson: String? = null,
    claimedBy: String? = null,
    claimedAt: Instant? = null,
    completedAt: Instant? = null,
    retryCount: Int = 0,
    maxRetries: Int = 3,
    deadlineAt: Instant? = later,
) = Task(
    id, workflowId, activityName, sequenceNumber, status, handlerKey,
    item, resultJson, claimedBy, claimedAt, completedAt,
    retryCount, maxRetries, deadlineAt,
)
```

Update `Task construction preserves all fields`:
```kotlin
@Test
fun `Task construction preserves all fields`() {
    val t = task(item = """{"key":"value"}""")
    assertEquals("task-1", t.id)
    assertEquals("wf-1", t.workflowId)
    assertEquals("step1", t.activityName)
    assertEquals(1, t.sequenceNumber)
    assertEquals(TaskStatus.PENDING, t.status)
    assertEquals("process.step1", t.handlerKey)
    assertEquals("""{"key":"value"}""", t.item)
    // ... (rest unchanged)
}
```

---

### Task 11: Fix remaining compile errors in integration test helpers

These tests are integration tests (Oracle). We only fix their compile errors here; behavioral correctness is tested in P7.

- [ ] **Step 1: Update `DefaultPhaseGateTest.kt` helper**

In `makeWorkflow()` helper, remove `currentSequence` parameter:

```kotlin
private fun makeWorkflow(
    id: String = randomId(),
    definition: WorkflowDefinition,
    version: Int = 0,
    status: WorkflowStatus = WorkflowStatus.RUNNING,
    createdAt: Instant = now(),
    updatedAt: Instant = now(),
    deadlineAt: Instant = now().plus(java.time.Duration.ofMinutes(30)),
): WorkflowRun = WorkflowRun(
    id = id,
    definitionJson = objectMapper.writeValueAsString(definition),
    version = version,
    status = status,
    createdAt = createdAt,
    updatedAt = updatedAt,
    deadlineAt = deadlineAt,
)
```

Remove the `AdvancementStrategyRegistry` import and constructor arg from `DefaultPhaseGateTest.setup()`:
```kotlin
barrier = DefaultPhaseGate(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
```

Also remove `AdvancementStrategyRegistry` usage in `WorkflowEngineTest.setup()`:
```kotlin
phaseGate = DefaultPhaseGate(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
```

- [ ] **Step 2: Update `StressTestBase.kt`** — Remove `AdvancementStrategyRegistry` import and usage

In `StressTestBase.kt`, find all references to `AdvancementStrategyRegistry` and remove them. Wherever `DefaultPhaseGate` is constructed with `AdvancementStrategyRegistry`, remove that arg:
```kotlin
// Before:
DefaultPhaseGate(jdbi, workflowRepo, taskRepo, objectMapper, AdvancementStrategyRegistry(), notifier)
// After:
DefaultPhaseGate(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
```

- [ ] **Step 3: Update `WorkflowEngineTest.kt`**

Remove `run.currentSequence` assertion (line ~85):
```kotlin
// Remove: assertEquals(1, run.currentSequence)
```

Remove `AdvancementStrategyRegistry` import and usage.

Update workflow DSL calls that use old API (e.g., `fanOut("simulate")`) to new API. Check for any activity definitions using old `fanOut: String?` syntax and update to use `fanOut { transition("...") }`.

- [ ] **Step 4: Update `WorkflowWatchdogTest.kt`**

Remove `workflow.currentSequence` references from log assertions if any.

Update `WorkflowDefinition` construction from `activities = listOf(...)` to `activities = mapOf(...)`.

- [ ] **Step 5: Update `RepositoryTest.kt`**

Remove any `currentSequence` from `WorkflowRun` constructions. Update `WorkflowDefinition` usages from List to Map form.

- [ ] **Step 6: Update `ActivityInputResolverTest.kt`**

Update `WorkflowDefinition` and `WorkflowRun` constructions for new API.

---

### Task 12: Compile check and unit test run

- [ ] **Step 1: Compile the project**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile test-compile -pl WorkFlow`

Expected: `BUILD SUCCESS` — all files compile cleanly.

- [ ] **Step 2: Run all unit tests (no Oracle needed)**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowModelsTest,SequenceModelTest,WorkflowDslBuildersTest,WorkflowDslTest,ActivityInputResolverTest" -pl WorkFlow`

Expected: `BUILD SUCCESS`

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/workflow/
git add src/test/kotlin/workflow/
git add src/test/kotlin/stress/StressTestBase.kt
git commit -m "refactor: replace linear model with DAG types, rewrite DSL and buildSequenceMap"
```
