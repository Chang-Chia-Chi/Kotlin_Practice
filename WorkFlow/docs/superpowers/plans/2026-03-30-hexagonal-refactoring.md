# Hexagonal Architecture Refactoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the project into domain-first hexagonal architecture with per-domain vertical slices and shared infrastructure.

**Architecture:** Three core domains (workflow, dispatch, worker) each get full hexagonal layering (model/usecase/port/service/adapter). Support domains (leader, shutdown, queryexporter) live under infrastructure/ with lighter structure. Shared utilities (JDBI, S3, HTTP clients) live in infrastructure/.

**Tech Stack:** Kotlin 2.3, Quarkus 3.x, JDBI 3, Maven

**Important Notes:**
- Source root is `src/main/kotlin/` with package prefix `com.workflow.` (e.g., directory `engine/` → package `com.workflow.engine`). This pattern continues: directory `workflow/model/` → package `com.workflow.workflow.model`.
- Maven command: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn`
- Test command: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test`
- This is a **structural refactoring only** — no behavioral changes. All existing tests must pass.
- For each file move: (1) create new file at target path, (2) update package declaration, (3) keep all code identical, (4) delete old file.
- After each task: update imports across the **entire** codebase (main + test), then compile.

---

### Task 1: Infrastructure — Shared Utilities

Move cross-cutting utilities into `infrastructure/`. These have no domain dependencies so they're safe to move first.

**Files:**
- Move: `config/FrameworkConfig.kt` → `infrastructure/config/FrameworkConfig.kt` (temporary — split in Task 2)
- Move: `config/ConfigValidator.kt` → `infrastructure/config/ConfigValidator.kt`
- Move: `extension/JdbiExtension.kt` → `infrastructure/persistence/JdbiExtension.kt`
- Move: `extension/FlowExtension.kt` → `infrastructure/coroutine/FlowExtension.kt`
- Move: `engine/RowMapperUtils.kt` → `infrastructure/persistence/RowMapperUtils.kt`
- Move: `dispatch/adapter/S3ClientProducer.kt` → `infrastructure/storage/S3ClientProducer.kt`
- Move: `worker/HttpClientProducer.kt` → `infrastructure/http/HttpClientProducer.kt`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p src/main/kotlin/infrastructure/config
mkdir -p src/main/kotlin/infrastructure/persistence
mkdir -p src/main/kotlin/infrastructure/coroutine
mkdir -p src/main/kotlin/infrastructure/storage
mkdir -p src/main/kotlin/infrastructure/http
```

- [ ] **Step 2: Move config files**

Move `config/FrameworkConfig.kt` → `infrastructure/config/FrameworkConfig.kt`:
- Change package from `com.workflow.config` to `com.workflow.infrastructure.config`
- Keep all content identical

Move `config/ConfigValidator.kt` → `infrastructure/config/ConfigValidator.kt`:
- Change package from `com.workflow.config` to `com.workflow.infrastructure.config`
- Update import of `FrameworkConfig` to `com.workflow.infrastructure.config.FrameworkConfig`

Delete old files: `config/FrameworkConfig.kt`, `config/ConfigValidator.kt`, then remove empty `config/` directory.

- [ ] **Step 3: Move extension files**

Move `extension/JdbiExtension.kt` → `infrastructure/persistence/JdbiExtension.kt`:
- Change package from `com.workflow.extension` to `com.workflow.infrastructure.persistence`

Move `extension/FlowExtension.kt` → `infrastructure/coroutine/FlowExtension.kt`:
- Change package from `com.workflow.extension` to `com.workflow.infrastructure.coroutine`

Delete old files and remove empty `extension/` directory.

- [ ] **Step 4: Move RowMapperUtils**

Move `engine/RowMapperUtils.kt` → `infrastructure/persistence/RowMapperUtils.kt`:
- Change package from `com.workflow.engine` to `com.workflow.infrastructure.persistence`

Delete old file (do NOT delete `engine/` directory — it still has other files).

- [ ] **Step 5: Move producer files**

Move `dispatch/adapter/S3ClientProducer.kt` → `infrastructure/storage/S3ClientProducer.kt`:
- Change package from `com.workflow.dispatch.adapter` to `com.workflow.infrastructure.storage`

Move `worker/HttpClientProducer.kt` → `infrastructure/http/HttpClientProducer.kt`:
- Change package from `com.workflow.worker` to `com.workflow.infrastructure.http`

Delete old files.

- [ ] **Step 6: Fix all imports across codebase**

Search and replace these import changes across ALL `.kt` files in `src/main/kotlin/` and `src/test/kotlin/`:

| Old import | New import |
|---|---|
| `com.workflow.config.FrameworkConfig` | `com.workflow.infrastructure.config.FrameworkConfig` |
| `com.workflow.config.ConfigValidator` | `com.workflow.infrastructure.config.ConfigValidator` |
| `com.workflow.extension.inTransactionSuspend` | `com.workflow.infrastructure.persistence.inTransactionSuspend` |
| `com.workflow.extension.withHandleSuspend` | `com.workflow.infrastructure.persistence.withHandleSuspend` |
| `com.workflow.extension.` (all FlowExtension functions) | `com.workflow.infrastructure.coroutine.` |
| `com.workflow.engine.readClob` | `com.workflow.infrastructure.persistence.readClob` |
| `com.workflow.engine.readTimestamp` | `com.workflow.infrastructure.persistence.readTimestamp` |
| `com.workflow.engine.readNullableTimestamp` | `com.workflow.infrastructure.persistence.readNullableTimestamp` |
| `com.workflow.engine.caseInsensitive` | `com.workflow.infrastructure.persistence.caseInsensitive` |
| `com.workflow.dispatch.adapter.S3ClientProducer` | `com.workflow.infrastructure.storage.S3ClientProducer` |
| `com.workflow.worker.HttpClientProducer` | `com.workflow.infrastructure.http.HttpClientProducer` |

- [ ] **Step 7: Compile check**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -q
```

Fix any remaining import issues until compilation succeeds.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: move shared utilities to infrastructure/ package"
```

---

### Task 2: Split FrameworkConfig Into Per-Domain Configs

Split the monolithic `FrameworkConfig` into domain-owned config interfaces. This is the most delicate task because every domain imports it.

**Files:**
- Modify: `infrastructure/config/FrameworkConfig.kt` → keep as root `@ConfigMapping`, delegate to sub-configs
- Create: `workflow/config/SweeperConfig.kt`
- Create: `worker/config/WorkerLoopConfig.kt`
- Create: `infrastructure/leader/LeaderElectionConfig.kt`
- Create: `infrastructure/shutdown/ShutdownConfig.kt`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p src/main/kotlin/workflow/config
mkdir -p src/main/kotlin/worker/config
```

- [ ] **Step 2: Create per-domain config interfaces**

Create `workflow/config/SweeperConfig.kt`:

```kotlin
package com.workflow.workflow.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import java.time.Duration

@ConfigMapping(prefix = "framework.sweeper")
interface SweeperConfig {
    @WithDefault("PT30S")
    fun interval(): Duration
    @WithDefault("PT2M")
    fun gracePeriod(): Duration
    @WithDefault("PT10M")
    fun staleTaskThreshold(): Duration
}
```

Create `worker/config/WorkerLoopConfig.kt`:

```kotlin
package com.workflow.worker.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import java.time.Duration

@ConfigMapping(prefix = "framework.worker")
interface WorkerLoopConfig {
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
```

Create `infrastructure/leader/LeaderElectionConfig.kt`:

```kotlin
package com.workflow.infrastructure.leader

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import java.time.Duration

@ConfigMapping(prefix = "framework.leader-election")
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
```

Create `infrastructure/shutdown/ShutdownConfig.kt`:

```kotlin
package com.workflow.infrastructure.shutdown

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import java.time.Duration

@ConfigMapping(prefix = "framework.shutdown")
interface ShutdownConfig {
    @WithDefault("PT30S")
    fun globalTimeout(): Duration
    @WithDefault("PT10S")
    fun leaderTeardownTimeout(): Duration
}
```

- [ ] **Step 3: Update FrameworkConfig to delegate**

Replace `infrastructure/config/FrameworkConfig.kt` content — keep only the root interface with `serviceName()` and remove the nested sub-interfaces. The nested interfaces are no longer needed since each domain now has its own `@ConfigMapping`.

```kotlin
package com.workflow.infrastructure.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault

@ConfigMapping(prefix = "framework")
interface FrameworkConfig {
    @WithDefault("workflow-engine")
    fun serviceName(): String
}
```

- [ ] **Step 4: Update all consumers to use domain-specific configs**

Find every file that imports `FrameworkConfig` and uses `.worker()`, `.leaderElection()`, `.shutdown()`, or `.sweeper()`. Replace with the corresponding domain-specific config injection:

- `config.worker().pollInterval()` → inject `WorkerLoopConfig`, use `workerLoopConfig.pollInterval()`
- `config.sweeper().interval()` → inject `SweeperConfig`, use `sweeperConfig.interval()`
- `config.leaderElection().leaseName()` → inject `LeaderElectionConfig`, use `leaderElectionConfig.leaseName()`
- `config.shutdown().globalTimeout()` → inject `ShutdownConfig`, use `shutdownConfig.globalTimeout()`
- `config.serviceName()` → keep using `FrameworkConfig` for this only

Update constructor parameters for each consuming class.

- [ ] **Step 5: Update ConfigValidator**

`infrastructure/config/ConfigValidator.kt` currently validates across all sub-configs. Update it to inject the individual domain configs:

```kotlin
package com.workflow.infrastructure.config

import com.workflow.infrastructure.leader.LeaderElectionConfig
import com.workflow.worker.config.WorkerLoopConfig
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import org.slf4j.LoggerFactory

@ApplicationScoped
class ConfigValidator(
    private val workerConfig: WorkerLoopConfig,
    private val leaderConfig: LeaderElectionConfig,
) {
    private val log = LoggerFactory.getLogger(ConfigValidator::class.java)

    fun validate(@Observes ev: StartupEvent) {
        require(workerConfig.concurrency() >= 1) {
            "framework.worker.concurrency must be >= 1, got ${workerConfig.concurrency()}"
        }
        require(workerConfig.batchSize() in 1..workerConfig.maxBatchSize()) {
            "framework.worker.batch-size must be in 1..${workerConfig.maxBatchSize()}, got ${workerConfig.batchSize()}"
        }
        require(leaderConfig.renewDeadline() < leaderConfig.leaseDuration()) {
            "leader-election.renew-deadline must be < lease-duration"
        }
        log.info("Configuration validated successfully")
    }
}
```

- [ ] **Step 6: Update test files that reference FrameworkConfig sub-interfaces**

Search for `FrameworkConfig.WorkerConfig`, `FrameworkConfig.LeaderElectionConfig`, `FrameworkConfig.ShutdownConfig`, `FrameworkConfig.SweeperConfig` in test files and replace with the new config types.

- [ ] **Step 7: Compile and run tests**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -q
```

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: split FrameworkConfig into per-domain config interfaces"
```

---

### Task 3: Infrastructure — Support Domains (Leader, Shutdown, Query Exporter)

Move leader, shutdown, and queryexporter into `infrastructure/`. Mostly file moves with package renames. Split `KubernetesDetector.kt` and `ExporterConfig.kt`.

**Files:**
- Move: `leader/LeaderElection.kt` → `infrastructure/leader/LeaderElection.kt`
- Move: `leader/LeaderManager.kt` → `infrastructure/leader/LeaderManager.kt`
- Move: `leader/LeaderHealthCheck.kt` → `infrastructure/leader/LeaderHealthCheck.kt`
- Split+Move: `leader/KubernetesDetector.kt` → `infrastructure/leader/KubernetesDetector.kt` (interface) + `infrastructure/leader/EnvKubernetesDetector.kt` (impl)
- Move: `leader/NotLeader.kt` → `infrastructure/leader/NotLeader.kt`
- Move: `shutdown/ShutdownCoordinator.kt` → `infrastructure/shutdown/ShutdownCoordinator.kt`
- Move: `shutdown/ShutdownParticipant.kt` → `infrastructure/shutdown/ShutdownParticipant.kt`
- Move: `shutdown/ShutdownSignal.kt` → `infrastructure/shutdown/ShutdownSignal.kt`
- Move: `shutdown/ShutdownState.kt` → `infrastructure/shutdown/ShutdownState.kt`
- Move: `queryexporter/` → `infrastructure/queryexporter/` (all files, preserving sub-structure)
- Split: `queryexporter/config/ExporterConfig.kt` → individual files

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p src/main/kotlin/infrastructure/leader
mkdir -p src/main/kotlin/infrastructure/shutdown
mkdir -p src/main/kotlin/infrastructure/queryexporter/config
mkdir -p src/main/kotlin/infrastructure/queryexporter/core
mkdir -p src/main/kotlin/infrastructure/queryexporter/spi
mkdir -p src/main/kotlin/infrastructure/queryexporter/adapter
mkdir -p src/main/kotlin/infrastructure/queryexporter/bootstrap
```

Note: `infrastructure/leader/` and `infrastructure/shutdown/` may already have config files from Task 2.

- [ ] **Step 2: Move leader files**

For each file in `leader/`, copy to `infrastructure/leader/` and change package from `com.workflow.leader` to `com.workflow.infrastructure.leader`. Delete originals.

Split `KubernetesDetector.kt`:
- `infrastructure/leader/KubernetesDetector.kt` — keep only the `fun interface KubernetesDetector`
- `infrastructure/leader/EnvKubernetesDetector.kt` — move `class EnvKubernetesDetector` to its own file

- [ ] **Step 3: Move shutdown files**

For each file in `shutdown/`, copy to `infrastructure/shutdown/` and change package from `com.workflow.shutdown` to `com.workflow.infrastructure.shutdown`. Delete originals.

- [ ] **Step 4: Move queryexporter files**

For each file in `queryexporter/`, copy to `infrastructure/queryexporter/` preserving sub-directory structure. Change all packages from `com.workflow.queryexporter.*` to `com.workflow.infrastructure.queryexporter.*`. Delete originals.

Split `ExporterConfig.kt` into individual files in `infrastructure/queryexporter/config/`:
- `ExporterConfig.kt` — `data class ExporterConfig` + companion `load()` method
- `QueryConfig.kt` — `data class QueryConfig`
- `ScheduleConfig.kt` — `data class ScheduleConfig`
- `MetricConfig.kt` — `data class MetricConfig`
- `MetricType.kt` — `enum class MetricType`
- `ExporterConfigValidator.kt` — `object ExporterConfigValidator` + `class ExporterConfigException`

Move `QueryExporterBean.kt` and `QueryExporterBootstrap.kt` to `infrastructure/queryexporter/bootstrap/`.
Move `QuarkusDataSourceProvider.kt` and `LeaderManagerGuardAdapter.kt` to `infrastructure/queryexporter/adapter/`.

- [ ] **Step 5: Fix all imports across codebase**

| Old import prefix | New import prefix |
|---|---|
| `com.workflow.leader.` | `com.workflow.infrastructure.leader.` |
| `com.workflow.shutdown.` | `com.workflow.infrastructure.shutdown.` |
| `com.workflow.queryexporter.` | `com.workflow.infrastructure.queryexporter.` |

- [ ] **Step 6: Delete old directories**

Remove empty `leader/`, `shutdown/`, `queryexporter/` directories.

- [ ] **Step 7: Compile and run tests**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -q
```

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: move leader, shutdown, queryexporter to infrastructure/"
```

---

### Task 4: Workflow Domain — Model Layer

Split multi-class files into one-class-per-file and move to `workflow/model/`.

**Files:**
- Split: `engine/WorkflowModels.kt` → 5 files in `workflow/model/`
- Split: `dsl/WorkflowDsl.kt` → 4 files in `workflow/model/`
- Split: `engine/PhaseStrategy.kt` → model types to `workflow/model/` (interface moves in Task 5)
- Move: `engine/SequenceModel.kt` → `workflow/model/SequenceModel.kt`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p src/main/kotlin/workflow/model
```

- [ ] **Step 2: Split WorkflowModels.kt**

All files use package `com.workflow.workflow.model`.

Create `workflow/model/WorkflowStatus.kt`:
```kotlin
package com.workflow.workflow.model

enum class WorkflowStatus {
    RUNNING, COMPLETED, FAILED, TIMED_OUT, CANCELLED;

    val isTerminal: Boolean get() = this != RUNNING

    companion object {
        private val allowed = setOf(
            RUNNING to COMPLETED,
            RUNNING to FAILED,
            RUNNING to TIMED_OUT,
            RUNNING to CANCELLED,
            FAILED to RUNNING,
            TIMED_OUT to RUNNING,
            CANCELLED to RUNNING,
        )

        fun requireTransition(from: WorkflowStatus, to: WorkflowStatus) {
            require((from to to) in allowed) {
                "Illegal workflow transition: $from → $to"
            }
        }
    }
}
```

Create `workflow/model/TaskStatus.kt`:
```kotlin
package com.workflow.workflow.model

enum class TaskStatus {
    PENDING, PROCESSING, WAITING_FOR_SIGNAL, COMPLETED, FAILED, TIMED_OUT, DEAD_LETTER, CANCELLED;

    val isTerminal: Boolean get() = this in terminalStatuses

    companion object {
        private val terminalStatuses = setOf(COMPLETED, FAILED, TIMED_OUT, DEAD_LETTER, CANCELLED)
        private val allowed = setOf(
            PENDING to PROCESSING,
            PENDING to CANCELLED,
            PROCESSING to COMPLETED,
            PROCESSING to FAILED,
            PROCESSING to TIMED_OUT,
            PROCESSING to PENDING,
            PROCESSING to DEAD_LETTER,
            PROCESSING to WAITING_FOR_SIGNAL,
            WAITING_FOR_SIGNAL to COMPLETED,
            WAITING_FOR_SIGNAL to FAILED,
            WAITING_FOR_SIGNAL to TIMED_OUT,
            WAITING_FOR_SIGNAL to CANCELLED,
            FAILED to PENDING,
            FAILED to DEAD_LETTER,
        )

        fun requireTransition(from: TaskStatus, to: TaskStatus) {
            require((from to to) in allowed) {
                "Illegal task transition: $from → $to"
            }
        }
    }
}
```

Create `workflow/model/WorkflowRun.kt`:
```kotlin
package com.workflow.workflow.model

import java.time.Instant

data class WorkflowRun(
    val id: String,
    val definitionJson: String,
    val currentSequence: Int,
    val version: Int,
    val status: WorkflowStatus,
    val createdAt: Instant,
    val updatedAt: Instant,
    val deadlineAt: Instant,
)
```

Create `workflow/model/Task.kt`:
```kotlin
package com.workflow.workflow.model

import java.time.Instant
import java.util.UUID

data class Task(
    val id: String,
    val workflowId: String,
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
    sequenceNumber: Int,
    activity: ActivityDefinition,
    now: Instant,
    item: String? = null,
): Task {
    return Task(
        id = UUID.randomUUID().toString(),
        workflowId = workflowId,
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
}
```

Create `workflow/model/StartResult.kt`:
```kotlin
package com.workflow.workflow.model

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

- [ ] **Step 3: Split WorkflowDsl.kt (dsl/)**

Create `workflow/model/FailurePolicy.kt`:
```kotlin
package com.workflow.workflow.model

enum class FailurePolicy { ABORT, BEST_EFFORT }
```

Create `workflow/model/JoinPolicy.kt`:
```kotlin
package com.workflow.workflow.model

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(value = JoinPolicy.All::class, name = "ALL"),
    JsonSubTypes.Type(value = JoinPolicy.Threshold::class, name = "THRESHOLD"),
    JsonSubTypes.Type(value = JoinPolicy.Percentage::class, name = "PERCENTAGE"),
)
sealed interface JoinPolicy {
    data object All : JoinPolicy

    data class Threshold(val n: Int) : JoinPolicy {
        init {
            require(n > 0) { "Threshold n must be > 0, got $n" }
        }
    }

    data class Percentage(val pct: Int) : JoinPolicy {
        init {
            require(pct in 1..100) { "Percentage pct must be in 1..100, got $pct" }
        }
    }
}
```

Create `workflow/model/ActivityDefinition.kt`:
```kotlin
package com.workflow.workflow.model

import java.time.Duration

data class ActivityDefinition(
    val name: String,
    val transition: String,
    val retries: Int = 0,
    val failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    val deadline: Duration = Duration.ofMinutes(30),
    val fanOut: String? = null,
    val joinPolicy: JoinPolicy = JoinPolicy.All,
    val backoffBase: Duration = Duration.ofSeconds(1),
    val backoffCap: Duration = Duration.ofSeconds(300),
    val queue: String = "default",
    val inputs: Map<String, String> = emptyMap(),
)
```

Create `workflow/model/WorkflowDefinition.kt`:
```kotlin
package com.workflow.workflow.model

import java.time.Duration

data class WorkflowDefinition(
    val activities: List<ActivityDefinition>,
    val deadline: Duration = Duration.ofHours(1),
) {
    init {
        require(activities.isNotEmpty()) { "Workflow must have at least one activity" }
        require(deadline > Duration.ZERO) { "Workflow deadline must be positive" }
        val names = activities.map { it.name }
        require(names.size == names.toSet().size) {
            "Activity names must be unique, found duplicates: ${names.groupBy { it }.filter { it.value.size > 1 }.keys}"
        }
        for (activity in activities) {
            val target = activity.fanOut ?: continue
            require(activities.any { it.name == target }) {
                "Activity '${activity.name}' fanOut references unknown activity '$target'"
            }
        }
        for ((i, activity) in activities.withIndex()) {
            val target = activity.fanOut ?: continue
            require(i + 1 < activities.size && activities[i + 1].name == target) {
                "fanOut target '$target' must be the next activity after '${activity.name}'"
            }
        }
        for (activity in activities) {
            val target = activity.fanOut ?: continue
            val targetActivity = activities.first { it.name == target }
            require(targetActivity.fanOut == null) {
                "fanOut target '$target' cannot itself be a fanOut source"
            }
        }
    }
}
```

- [ ] **Step 4: Split PhaseStrategy.kt model types**

Create `workflow/model/PhaseContext.kt`:
```kotlin
package com.workflow.workflow.model

data class PhaseContext(
    val workflow: WorkflowRun,
    val definition: WorkflowDefinition,
    val currentSeqInfo: SequenceInfo,
    val sequenceMap: Map<Int, SequenceInfo>,
    val failedCount: Int,
    val totalCount: Int,
)

fun PhaseContext.failOrAdvance(): AdvancementDecision? {
    if (failedCount == 0) return null
    return when (currentSeqInfo.activity.failurePolicy) {
        FailurePolicy.ABORT -> AdvancementDecision.Abort(
            "$failedCount task(s) failed at sequence ${currentSeqInfo.sequenceNumber}",
        )
        FailurePolicy.BEST_EFFORT -> advanceOrComplete()
    }
}

fun PhaseContext.advanceOrComplete(): AdvancementDecision {
    val nextSeq = currentSeqInfo.nextSequence ?: return AdvancementDecision.Complete
    return AdvancementDecision.Advance(nextSeq)
}
```

Create `workflow/model/AdvancementDecision.kt`:
```kotlin
package com.workflow.workflow.model

sealed interface AdvancementDecision {
    data class Advance(val nextSequence: Int) : AdvancementDecision
    data object Complete : AdvancementDecision
    data class Abort(val reason: String) : AdvancementDecision
}
```

- [ ] **Step 5: Move SequenceModel.kt**

Move `engine/SequenceModel.kt` → `workflow/model/SequenceModel.kt`:
- Change package to `com.workflow.workflow.model`
- Update import of `WorkflowDefinition` from `com.workflow.dsl.WorkflowDefinition` to same-package (remove import)
- Update import of `ActivityDefinition` similarly

- [ ] **Step 6: Fix imports across codebase**

| Old import | New import |
|---|---|
| `com.workflow.engine.WorkflowStatus` | `com.workflow.workflow.model.WorkflowStatus` |
| `com.workflow.engine.TaskStatus` | `com.workflow.workflow.model.TaskStatus` |
| `com.workflow.engine.WorkflowRun` | `com.workflow.workflow.model.WorkflowRun` |
| `com.workflow.engine.Task` | `com.workflow.workflow.model.Task` |
| `com.workflow.engine.StartResult` | `com.workflow.workflow.model.StartResult` |
| `com.workflow.engine.workflowId` | `com.workflow.workflow.model.workflowId` |
| `com.workflow.engine.createTaskForActivity` | `com.workflow.workflow.model.createTaskForActivity` |
| `com.workflow.engine.PhaseContext` | `com.workflow.workflow.model.PhaseContext` |
| `com.workflow.engine.AdvancementDecision` | `com.workflow.workflow.model.AdvancementDecision` |
| `com.workflow.engine.failOrAdvance` | `com.workflow.workflow.model.failOrAdvance` |
| `com.workflow.engine.advanceOrComplete` | `com.workflow.workflow.model.advanceOrComplete` |
| `com.workflow.engine.PhaseType` | `com.workflow.workflow.model.PhaseType` |
| `com.workflow.engine.SequenceInfo` | `com.workflow.workflow.model.SequenceInfo` |
| `com.workflow.engine.buildSequenceMap` | `com.workflow.workflow.model.buildSequenceMap` |
| `com.workflow.dsl.WorkflowDefinition` | `com.workflow.workflow.model.WorkflowDefinition` |
| `com.workflow.dsl.ActivityDefinition` | `com.workflow.workflow.model.ActivityDefinition` |
| `com.workflow.dsl.FailurePolicy` | `com.workflow.workflow.model.FailurePolicy` |
| `com.workflow.dsl.JoinPolicy` | `com.workflow.workflow.model.JoinPolicy` |

Delete old files: `engine/WorkflowModels.kt`, `dsl/WorkflowDsl.kt`, `engine/PhaseStrategy.kt` (keep only the interface — handled in Task 5), `engine/SequenceModel.kt`.

- [ ] **Step 7: Compile check**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -q
```

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: split workflow model types into individual files under workflow/model/"
```

---

### Task 5: Workflow Domain — Ports, Services, Adapters, DSL

Extract interfaces for inbound/outbound ports. Move service implementations and create JDBI adapter implementations.

**Files:**
- Create: `workflow/usecase/port/inbound/orchestration/WorkflowOperations.kt` (new interface)
- Create: `workflow/usecase/port/inbound/orchestration/BarrierOperations.kt` (new interface)
- Create: `workflow/usecase/port/inbound/phase/PhaseStrategy.kt` (extracted from engine/PhaseStrategy.kt)
- Create: `workflow/usecase/port/outbound/persistent/WorkflowRepository.kt` (new interface)
- Create: `workflow/usecase/port/outbound/persistent/TaskRepository.kt` (new interface)
- Move: `engine/WorkflowEngine.kt` → `workflow/usecase/service/orchestration/WorkflowEngine.kt`
- Move: `engine/BarrierService.kt` → `workflow/usecase/service/orchestration/BarrierService.kt`
- Move: `engine/Sweeper.kt` → `workflow/usecase/service/orchestration/Sweeper.kt`
- Move: `engine/InputResolver.kt` → `workflow/usecase/service/orchestration/InputResolver.kt`
- Move: `engine/LinearPhaseStrategy.kt` → `workflow/usecase/service/phase/LinearPhaseStrategy.kt`
- Move: `engine/ParallelPhaseStrategy.kt` → `workflow/usecase/service/phase/ParallelPhaseStrategy.kt`
- Move: `engine/PhaseStrategyRegistry.kt` → `workflow/usecase/service/phase/PhaseStrategyRegistry.kt`
- Move: `engine/WorkflowRepository.kt` → `workflow/adapter/persistent/JdbiWorkflowRepository.kt`
- Move: `engine/TaskRepository.kt` → `workflow/adapter/persistent/JdbiTaskRepository.kt`
- Move: `dsl/WorkflowDslBuilders.kt` → `workflow/dsl/WorkflowDslBuilders.kt`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p src/main/kotlin/workflow/usecase/port/inbound/orchestration
mkdir -p src/main/kotlin/workflow/usecase/port/inbound/phase
mkdir -p src/main/kotlin/workflow/usecase/port/outbound/persistent
mkdir -p src/main/kotlin/workflow/usecase/service/orchestration
mkdir -p src/main/kotlin/workflow/usecase/service/phase
mkdir -p src/main/kotlin/workflow/adapter/persistent
mkdir -p src/main/kotlin/workflow/dsl
```

- [ ] **Step 2: Create inbound port interfaces**

Create `workflow/usecase/port/inbound/orchestration/WorkflowOperations.kt`:
```kotlin
package com.workflow.workflow.usecase.port.inbound.orchestration

import com.workflow.workflow.model.StartResult
import com.workflow.workflow.model.WorkflowDefinition

interface WorkflowOperations {
    suspend fun startWorkflow(
        definition: WorkflowDefinition,
        idempotencyKey: String? = null,
    ): StartResult

    suspend fun cancelWorkflow(workflowId: String): Boolean
    suspend fun replayWorkflow(workflowId: String): Boolean
}
```

Create `workflow/usecase/port/inbound/orchestration/BarrierOperations.kt`:
```kotlin
package com.workflow.workflow.usecase.port.inbound.orchestration

import com.workflow.workflow.model.TaskStatus
import java.time.Instant

interface BarrierOperations {
    suspend fun onTaskCompleted(
        taskId: String,
        workflowId: String,
        sequenceNumber: Int,
        status: TaskStatus,
        resultJson: String?,
        claimedBy: String? = null,
        claimedAt: Instant? = null,
    )

    suspend fun recoverStuckWorkflow(workflowId: String)
}
```

Create `workflow/usecase/port/inbound/phase/PhaseStrategy.kt`:
```kotlin
package com.workflow.workflow.usecase.port.inbound.phase

import com.workflow.workflow.model.AdvancementDecision
import com.workflow.workflow.model.PhaseContext

interface PhaseStrategy {
    fun resolve(context: PhaseContext): AdvancementDecision
}
```

- [ ] **Step 3: Create outbound port interfaces**

Extract interfaces from current concrete repositories. The interfaces contain only the public method signatures.

Create `workflow/usecase/port/outbound/persistent/WorkflowRepository.kt`:
```kotlin
package com.workflow.workflow.usecase.port.outbound.persistent

import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import org.jdbi.v3.core.Handle
import java.time.Duration

interface WorkflowRepository {
    suspend fun insert(run: WorkflowRun)
    suspend fun findById(id: String): WorkflowRun?
    suspend fun casAdvance(id: String, expectedSequence: Int, nextSequence: Int, expectedVersion: Int): Boolean
    suspend fun updateStatus(id: String, newStatus: WorkflowStatus, expectedStatus: WorkflowStatus): Boolean
    suspend fun findStuck(gracePeriod: Duration): List<WorkflowRun>
    suspend fun findTimedOut(): List<WorkflowRun>

    fun insertWithHandle(handle: Handle, run: WorkflowRun)
    fun findByIdWithHandle(handle: Handle, id: String): WorkflowRun?
    fun casAdvanceWithHandle(handle: Handle, id: String, expectedSequence: Int, nextSequence: Int, expectedVersion: Int): Boolean
    fun updateStatusWithHandle(handle: Handle, id: String, newStatus: WorkflowStatus, expectedStatus: WorkflowStatus): Boolean
    fun mergeIdempotentWithHandle(handle: Handle, run: WorkflowRun, idempotencyKey: String): Pair<String, Boolean>
}
```

Create `workflow/usecase/port/outbound/persistent/TaskRepository.kt`:
```kotlin
package com.workflow.workflow.usecase.port.outbound.persistent

import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import org.jdbi.v3.core.Handle
import java.time.Instant

interface TaskRepository {
    suspend fun insertBatch(tasks: List<Task>)
    suspend fun claimNext(workerId: String, limit: Int, queueName: String = "default"): List<Task>
    suspend fun updateStatus(id: String, newStatus: TaskStatus, resultJson: String? = null): Boolean
    suspend fun countNonTerminal(workflowId: String, sequenceNumber: Int): Int
    suspend fun countFailed(workflowId: String, sequenceNumber: Int): Int
    suspend fun countTotal(workflowId: String, sequenceNumber: Int): Int
    suspend fun findByWorkflowAndSequence(workflowId: String, sequenceNumber: Int): List<Task>
    suspend fun resetForRetry(id: String, newRetryCount: Int)
    suspend fun replayDeadLetterTask(taskId: String): Boolean
    suspend fun replayDeadLetterBatch(workflowId: String): Int
    suspend fun findExpired(now: Instant): List<Task>
    suspend fun resetStaleTasks(staleThreshold: Instant): Int
    suspend fun deadLetterExhaustedTasks(staleThreshold: Instant): Int

    fun updateStatusWithHandle(handle: Handle, id: String, newStatus: TaskStatus, resultJson: String? = null, claimedBy: String? = null, claimedAt: Instant? = null): Boolean
    fun countNonTerminalWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): Int
    fun countFailedWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): Int
    fun countTotalWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): Int
    fun findByWorkflowAndSequenceWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): List<Task>
    fun cancelPendingTasksWithHandle(handle: Handle, workflowId: String): Int
    fun insertBatchWithHandle(handle: Handle, tasks: List<Task>)
    fun replayDeadLetterBatchWithHandle(handle: Handle, workflowId: String): Int
    fun findDistinctQueuesByWorkflowId(handle: Handle, workflowId: String, statuses: List<String>): List<String>
}
```

- [ ] **Step 4: Move and rename repository implementations**

Move `engine/WorkflowRepository.kt` → `workflow/adapter/persistent/JdbiWorkflowRepository.kt`:
- Change package to `com.workflow.workflow.adapter.persistent`
- Rename class from `WorkflowRepository` to `JdbiWorkflowRepository`
- Add `: WorkflowRepository` interface implementation
- Add import for `com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository`
- Update all model imports to `com.workflow.workflow.model.*`
- Update JDBI extension imports to `com.workflow.infrastructure.persistence.*`

Move `engine/TaskRepository.kt` → `workflow/adapter/persistent/JdbiTaskRepository.kt`:
- Same pattern: rename class, implement interface, update imports

- [ ] **Step 5: Move service files**

Move `engine/WorkflowEngine.kt` → `workflow/usecase/service/orchestration/WorkflowEngine.kt`:
- Change package to `com.workflow.workflow.usecase.service.orchestration`
- Add `: WorkflowOperations` interface implementation
- Update all imports

Move `engine/BarrierService.kt` → `workflow/usecase/service/orchestration/BarrierService.kt`:
- Change package to `com.workflow.workflow.usecase.service.orchestration`
- Add `: BarrierOperations` interface implementation
- Change constructor parameter types from concrete `WorkflowRepository`/`TaskRepository` to the port interfaces
- Update all imports

Move `engine/Sweeper.kt` → `workflow/usecase/service/orchestration/Sweeper.kt`:
- Change package, update imports
- Change constructor parameter types to port interfaces

Move `engine/InputResolver.kt` → `workflow/usecase/service/orchestration/InputResolver.kt`:
- Change package, update imports
- Change constructor parameter types to port interfaces

- [ ] **Step 6: Move phase strategy files**

Move `engine/LinearPhaseStrategy.kt` → `workflow/usecase/service/phase/LinearPhaseStrategy.kt`:
- Change package to `com.workflow.workflow.usecase.service.phase`
- Update PhaseStrategy import to `com.workflow.workflow.usecase.port.inbound.phase.PhaseStrategy`

Move `engine/ParallelPhaseStrategy.kt` → `workflow/usecase/service/phase/ParallelPhaseStrategy.kt`:
- Same pattern

Move `engine/PhaseStrategyRegistry.kt` → `workflow/usecase/service/phase/PhaseStrategyRegistry.kt`:
- Same pattern

Delete old `engine/PhaseStrategy.kt` (model types already moved in Task 4; interface now in port).

- [ ] **Step 7: Move DSL builders**

Move `dsl/WorkflowDslBuilders.kt` → `workflow/dsl/WorkflowDslBuilders.kt`:
- Change package to `com.workflow.workflow.dsl`
- Update imports of `WorkflowDefinition`, `ActivityDefinition` etc. to `com.workflow.workflow.model.*`

Delete old `dsl/` directory.

- [ ] **Step 8: Fix all imports and delete old engine/ directory**

Update all imports across codebase:

| Old import | New import |
|---|---|
| `com.workflow.engine.WorkflowEngine` | `com.workflow.workflow.usecase.service.orchestration.WorkflowEngine` |
| `com.workflow.engine.BarrierService` | `com.workflow.workflow.usecase.service.orchestration.BarrierService` |
| `com.workflow.engine.Sweeper` | `com.workflow.workflow.usecase.service.orchestration.Sweeper` |
| `com.workflow.engine.InputResolver` | `com.workflow.workflow.usecase.service.orchestration.InputResolver` |
| `com.workflow.engine.WorkflowRepository` | `com.workflow.workflow.adapter.persistent.JdbiWorkflowRepository` (or port interface depending on usage) |
| `com.workflow.engine.TaskRepository` | `com.workflow.workflow.adapter.persistent.JdbiTaskRepository` (or port interface) |
| `com.workflow.engine.PhaseStrategy` | `com.workflow.workflow.usecase.port.inbound.phase.PhaseStrategy` |
| `com.workflow.engine.LinearPhaseStrategy` | `com.workflow.workflow.usecase.service.phase.LinearPhaseStrategy` |
| `com.workflow.engine.ParallelPhaseStrategy` | `com.workflow.workflow.usecase.service.phase.ParallelPhaseStrategy` |
| `com.workflow.engine.PhaseStrategyRegistry` | `com.workflow.workflow.usecase.service.phase.PhaseStrategyRegistry` |
| `com.workflow.dsl.WorkflowDslBuilders` (or builder functions) | `com.workflow.workflow.dsl.*` |

**Important:** Service classes should depend on port interfaces, not adapter implementations. Update constructor injection types:
- `WorkflowEngine(... workflowRepo: WorkflowRepository ...)` → uses the **port interface** `com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository`
- Same for `TaskRepository` references in services

Delete old `engine/` and `dsl/` directories (verify empty first).

- [ ] **Step 9: Compile and run tests**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -q
```

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "refactor: restructure workflow domain with hexagonal ports and adapters"
```

---

### Task 6: Dispatch Domain — Model Layer + Ports

Split DispatchModels.kt, split DispatchPorts.kt, extract algorithm interfaces.

**Files:**
- Split: `dispatch/model/DispatchModels.kt` → individual files in `dispatch/model/`
- Split: `dispatch/port/DispatchPorts.kt` → individual files in `dispatch/usecase/port/outbound/`
- Split+Move: algorithm interfaces → `dispatch/usecase/port/inbound/algorithm/`
- Move: `dispatch/simulation/SimulationContext.kt` → `dispatch/model/SimulationContext.kt`
- Move: `dispatch/simulation/CandidateIndex.kt` → `dispatch/model/CandidateIndex.kt`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p src/main/kotlin/dispatch/usecase/port/inbound/algorithm
mkdir -p src/main/kotlin/dispatch/usecase/port/outbound/persistence
mkdir -p src/main/kotlin/dispatch/usecase/port/outbound/storage
```

- [ ] **Step 2: Split DispatchModels.kt**

Create individual files in `dispatch/model/` — each with package `com.workflow.dispatch.model`:
- `DispatchMode.kt` — `enum class DispatchMode`
- `DispatchConfig.kt` — `data class DispatchConfig`
- `SiteTarget.kt` — `data class SiteTarget`
- `BomMapping.kt` — `data class BomMapping`
- `TargetBomAllocation.kt` — `data class TargetBomAllocation`
- `CandidateProduct.kt` — `data class CandidateProduct`
- `DispatchDecision.kt` — `data class DispatchDecision`
- `SimulationResult.kt` — `data class SimulationResult`
- `SiteBomKey.kt` — `data class SiteBomKey`
- `Baseline.kt` — `data class Baseline`
- `TerminationDecision.kt` — `enum class TerminationDecision { STOP, SKIP_SITE }` (from `TerminationStrategy.kt`)
- `TargetSelection.kt` — `sealed interface TargetSelection` + variants (from `DispatchAlgorithm.kt`)

Move `dispatch/simulation/SimulationContext.kt` → `dispatch/model/SimulationContext.kt` (change package to `com.workflow.dispatch.model`).
Move `dispatch/simulation/CandidateIndex.kt` → `dispatch/model/CandidateIndex.kt` (change package to `com.workflow.dispatch.model`).

Delete old `dispatch/model/DispatchModels.kt`. Delete old `dispatch/simulation/SimulationContext.kt` and `dispatch/simulation/CandidateIndex.kt`.

- [ ] **Step 3: Split DispatchPorts.kt into outbound ports**

Create individual files in `dispatch/usecase/port/outbound/persistence/`:
- `DispatchConfigRepository.kt` — `interface DispatchConfigRepository`
- `CandidateQueryPort.kt` — `interface CandidateQueryPort`
- `BaselineProvider.kt` — `interface BaselineProvider`
- `SimulationResultStore.kt` — `interface SimulationResultStore`

Create individual files in `dispatch/usecase/port/outbound/storage/`:
- `StoragePort.kt` — `interface StoragePort`
- `CsvFormatter.kt` — `interface CsvFormatter`
- `ParquetFormatter.kt` — `interface ParquetFormatter`

All with package matching their directory path (e.g., `com.workflow.dispatch.usecase.port.outbound.persistence`).

Delete old `dispatch/port/DispatchPorts.kt`.

- [ ] **Step 4: Extract algorithm interfaces into inbound ports**

From `dispatch/algorithm/DispatchAlgorithm.kt`, extract interface to `dispatch/usecase/port/inbound/algorithm/DispatchAlgorithm.kt`:
```kotlin
package com.workflow.dispatch.usecase.port.inbound.algorithm

import com.workflow.dispatch.model.*
import java.math.BigDecimal

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
```

Similarly extract to `dispatch/usecase/port/inbound/algorithm/`:
- `DispatchAlgorithmFactory.kt` — interface only
- `CandidateMatcher.kt` — interface only
- `GapComputer.kt` — interface only
- `TerminationStrategy.kt` — interface only

Note: The dispatch handlers (`DispatchScatterHandler`, `DispatchJoinHandler`, `DispatchSimulationHandler`) implement `TransitionHandler` from the worker domain. No separate `DispatchOperations` interface is needed — they already have a contract via `TransitionHandler`. The `dispatch/usecase/port/inbound/handler/` directory is not created.

- [ ] **Step 5: Fix imports across codebase**

| Old import | New import |
|---|---|
| `com.workflow.dispatch.port.DispatchConfigRepository` | `com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository` |
| `com.workflow.dispatch.port.CandidateQueryPort` | `com.workflow.dispatch.usecase.port.outbound.persistence.CandidateQueryPort` |
| `com.workflow.dispatch.port.BaselineProvider` | `com.workflow.dispatch.usecase.port.outbound.persistence.BaselineProvider` |
| `com.workflow.dispatch.port.SimulationResultStore` | `com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore` |
| `com.workflow.dispatch.port.StoragePort` | `com.workflow.dispatch.usecase.port.outbound.storage.StoragePort` |
| `com.workflow.dispatch.port.CsvFormatter` | `com.workflow.dispatch.usecase.port.outbound.storage.CsvFormatter` |
| `com.workflow.dispatch.port.ParquetFormatter` | `com.workflow.dispatch.usecase.port.outbound.storage.ParquetFormatter` |
| `com.workflow.dispatch.algorithm.DispatchAlgorithm` (interface) | `com.workflow.dispatch.usecase.port.inbound.algorithm.DispatchAlgorithm` |
| `com.workflow.dispatch.algorithm.CandidateMatcher` (interface) | `com.workflow.dispatch.usecase.port.inbound.algorithm.CandidateMatcher` |
| `com.workflow.dispatch.algorithm.GapComputer` (interface) | `com.workflow.dispatch.usecase.port.inbound.algorithm.GapComputer` |
| `com.workflow.dispatch.algorithm.TerminationStrategy` (interface) | `com.workflow.dispatch.usecase.port.inbound.algorithm.TerminationStrategy` |
| `com.workflow.dispatch.algorithm.TerminationDecision` | `com.workflow.dispatch.model.TerminationDecision` |
| `com.workflow.dispatch.algorithm.TargetSelection` | `com.workflow.dispatch.model.TargetSelection` |
| `com.workflow.dispatch.simulation.SimulationContext` | `com.workflow.dispatch.model.SimulationContext` |
| `com.workflow.dispatch.simulation.CandidateIndex` | `com.workflow.dispatch.model.CandidateIndex` |

- [ ] **Step 6: Compile check**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -q
```

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor: split dispatch models and ports into hexagonal structure"
```

---

### Task 7: Dispatch Domain — Services, Adapters, DSL

Move algorithm implementations, handlers, simulation engine, storage adapters, and DSL.

**Files:**
- Split+Move: algorithm implementations → `dispatch/usecase/service/algorithm/`
- Move: handlers → `dispatch/usecase/service/handler/`
- Move: SimulationEngine → `dispatch/usecase/service/simulation/`
- Move: storage adapters → `dispatch/adapter/storage/`
- Move: DSL files → `dispatch/dsl/`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p src/main/kotlin/dispatch/usecase/service/algorithm
mkdir -p src/main/kotlin/dispatch/usecase/service/handler
mkdir -p src/main/kotlin/dispatch/usecase/service/simulation
mkdir -p src/main/kotlin/dispatch/adapter/storage
mkdir -p src/main/kotlin/dispatch/dsl
```

- [ ] **Step 2: Move and split algorithm implementations**

From `dispatch/algorithm/DispatchAlgorithm.kt`, move `DefaultDispatchAlgorithm` class to:
`dispatch/usecase/service/algorithm/DefaultDispatchAlgorithm.kt`
- Package: `com.workflow.dispatch.usecase.service.algorithm`
- Import the interface from `com.workflow.dispatch.usecase.port.inbound.algorithm.DispatchAlgorithm`

From `dispatch/algorithm/DispatchAlgorithmFactory.kt`, move `DefaultDispatchAlgorithmFactory` to:
`dispatch/usecase/service/algorithm/DefaultDispatchAlgorithmFactory.kt`

From `dispatch/algorithm/CandidateMatcher.kt`, move implementations to:
- `dispatch/usecase/service/algorithm/DefaultCandidateMatcher.kt`
- `dispatch/usecase/service/algorithm/QtyCandidateMatcher.kt`

From `dispatch/algorithm/GapComputer.kt`, move implementations to:
- `dispatch/usecase/service/algorithm/QtyGapComputer.kt`
- `dispatch/usecase/service/algorithm/RatioGapComputer.kt`

From `dispatch/algorithm/TerminationStrategy.kt`, move `FailFastTermination` to:
`dispatch/usecase/service/algorithm/FailFastTermination.kt`

Move `dispatch/algorithm/SelectionKernel.kt` → `dispatch/usecase/service/algorithm/SelectionKernel.kt`

Move `dispatch/algorithm/DispatchAlgorithmDsl.kt` → `dispatch/dsl/DispatchAlgorithmDsl.kt`

Delete old `dispatch/algorithm/` directory.

- [ ] **Step 3: Move handler files**

Move each file from `dispatch/handler/` to `dispatch/usecase/service/handler/`:
- `DispatchScatterHandler.kt`
- `DispatchJoinHandler.kt`
- `DispatchSimulationHandler.kt`
- `DispatchScheduler.kt`

Change package from `com.workflow.dispatch.handler` to `com.workflow.dispatch.usecase.service.handler`.

Move `dispatch/handler/DispatchWorkflow.kt` → `dispatch/dsl/DispatchWorkflow.kt`
- Change package to `com.workflow.dispatch.dsl`

Delete old `dispatch/handler/` directory.

- [ ] **Step 4: Move simulation engine**

Move `dispatch/simulation/SimulationEngine.kt` → `dispatch/usecase/service/simulation/SimulationEngine.kt`
- Change package to `com.workflow.dispatch.usecase.service.simulation`
- Update imports for SimulationContext and CandidateIndex (now in `dispatch/model/`)

Delete old `dispatch/simulation/` directory (should be empty after Task 6 moved the model files).

- [ ] **Step 5: Move storage adapters**

Move `dispatch/adapter/S3StorageAdapter.kt` → `dispatch/adapter/storage/S3StorageAdapter.kt`
- Change package to `com.workflow.dispatch.adapter.storage`

Move `dispatch/port/DefaultCsvFormatter.kt` → `dispatch/adapter/storage/DefaultCsvFormatter.kt`
- Change package to `com.workflow.dispatch.adapter.storage`

Move `dispatch/port/NoOpParquetFormatter.kt` → `dispatch/adapter/storage/NoOpParquetFormatter.kt`
- Change package to `com.workflow.dispatch.adapter.storage`

Delete old `dispatch/adapter/` (if only S3ClientProducer was in it, already moved) and `dispatch/port/` directories.

- [ ] **Step 6: Fix all imports across codebase**

| Old import | New import |
|---|---|
| `com.workflow.dispatch.algorithm.DefaultDispatchAlgorithm` | `com.workflow.dispatch.usecase.service.algorithm.DefaultDispatchAlgorithm` |
| `com.workflow.dispatch.algorithm.DefaultDispatchAlgorithmFactory` | `com.workflow.dispatch.usecase.service.algorithm.DefaultDispatchAlgorithmFactory` |
| `com.workflow.dispatch.algorithm.DefaultCandidateMatcher` | `com.workflow.dispatch.usecase.service.algorithm.DefaultCandidateMatcher` |
| `com.workflow.dispatch.algorithm.QtyCandidateMatcher` | `com.workflow.dispatch.usecase.service.algorithm.QtyCandidateMatcher` |
| `com.workflow.dispatch.algorithm.QtyGapComputer` | `com.workflow.dispatch.usecase.service.algorithm.QtyGapComputer` |
| `com.workflow.dispatch.algorithm.RatioGapComputer` | `com.workflow.dispatch.usecase.service.algorithm.RatioGapComputer` |
| `com.workflow.dispatch.algorithm.FailFastTermination` | `com.workflow.dispatch.usecase.service.algorithm.FailFastTermination` |
| `com.workflow.dispatch.algorithm.SelectionEntry` | `com.workflow.dispatch.usecase.service.algorithm.SelectionEntry` |
| `com.workflow.dispatch.algorithm.selectByGap` | `com.workflow.dispatch.usecase.service.algorithm.selectByGap` |
| `com.workflow.dispatch.algorithm.DispatchAlgorithmDsl` (or DSL functions) | `com.workflow.dispatch.dsl.*` |
| `com.workflow.dispatch.algorithm.dispatchAlgorithm` | `com.workflow.dispatch.dsl.dispatchAlgorithm` |
| `com.workflow.dispatch.handler.*` | `com.workflow.dispatch.usecase.service.handler.*` |
| `com.workflow.dispatch.handler.DispatchWorkflow` (or functions) | `com.workflow.dispatch.dsl.*` |
| `com.workflow.dispatch.simulation.SimulationEngine` | `com.workflow.dispatch.usecase.service.simulation.SimulationEngine` |
| `com.workflow.dispatch.adapter.S3StorageAdapter` | `com.workflow.dispatch.adapter.storage.S3StorageAdapter` |
| `com.workflow.dispatch.port.DefaultCsvFormatter` | `com.workflow.dispatch.adapter.storage.DefaultCsvFormatter` |
| `com.workflow.dispatch.port.NoOpParquetFormatter` | `com.workflow.dispatch.adapter.storage.NoOpParquetFormatter` |

- [ ] **Step 7: Compile and run tests**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -q
```

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: restructure dispatch domain services, adapters, and DSL"
```

---

### Task 8: Worker Domain

Move worker files into hexagonal structure. Extract `PeerDiscovery` interface. Split `DispatchNotifier` (interface already exists — just move).

**Files:**
- Move: `worker/TransitionHandler.kt` → `worker/usecase/port/inbound/execution/TransitionHandler.kt`
- Move: `worker/DispatchNotifier.kt` interface part → `worker/usecase/port/outbound/notification/DispatchNotifier.kt`
- Move: `worker/DispatchNotifier.kt` impl part → `worker/adapter/http/DispatchNotifierImpl.kt`
- Create: `worker/usecase/port/outbound/peer/PeerDiscovery.kt` (new interface)
- Move: `worker/PeerRegistry.kt` → `worker/adapter/http/PeerRegistry.kt` (implements PeerDiscovery)
- Move: `worker/WorkerLoop.kt` → `worker/usecase/service/execution/WorkerLoop.kt`
- Move: `worker/HandlerRegistry.kt` → `worker/usecase/service/execution/HandlerRegistry.kt`
- Move: `worker/MeteredTransitionHandler.kt` → `worker/usecase/service/execution/MeteredTransitionHandler.kt`
- Move: `worker/DispatchNotifyResource.kt` → `worker/adapter/web/DispatchNotifyResource.kt`
- Move: `worker/WorkerLoopHealthCheck.kt` → `worker/health/WorkerLoopHealthCheck.kt`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p src/main/kotlin/worker/usecase/port/inbound/execution
mkdir -p src/main/kotlin/worker/usecase/port/outbound/notification
mkdir -p src/main/kotlin/worker/usecase/port/outbound/peer
mkdir -p src/main/kotlin/worker/usecase/service/execution
mkdir -p src/main/kotlin/worker/adapter/web
mkdir -p src/main/kotlin/worker/adapter/http
mkdir -p src/main/kotlin/worker/health
```

- [ ] **Step 2: Move port interfaces**

Move the `DispatchNotifier` interface (lines 31-55 of current file) to:
`worker/usecase/port/outbound/notification/DispatchNotifier.kt`
- Package: `com.workflow.worker.usecase.port.outbound.notification`
- Interface only — no implementation

Move `worker/TransitionHandler.kt` → `worker/usecase/port/inbound/execution/TransitionHandler.kt`
- Change package to `com.workflow.worker.usecase.port.inbound.execution`

Create `worker/usecase/port/outbound/peer/PeerDiscovery.kt`:
```kotlin
package com.workflow.worker.usecase.port.outbound.peer

interface PeerDiscovery {
    fun peers(): List<String>
}
```

- [ ] **Step 3: Move service files**

Move `worker/WorkerLoop.kt` → `worker/usecase/service/execution/WorkerLoop.kt`:
- Change package to `com.workflow.worker.usecase.service.execution`
- Update imports for model types (`Task`, `TaskStatus`), port interfaces, config
- Change constructor to depend on port interfaces where appropriate

Move `worker/HandlerRegistry.kt` → `worker/usecase/service/execution/HandlerRegistry.kt`:
- Change package to `com.workflow.worker.usecase.service.execution`

Move `worker/MeteredTransitionHandler.kt` → `worker/usecase/service/execution/MeteredTransitionHandler.kt`:
- Change package to `com.workflow.worker.usecase.service.execution`

- [ ] **Step 4: Move adapter files**

Move `DispatchNotifierImpl` class to `worker/adapter/http/DispatchNotifierImpl.kt`:
- Package: `com.workflow.worker.adapter.http`
- Import interface from `com.workflow.worker.usecase.port.outbound.notification.DispatchNotifier`
- Change constructor to depend on `PeerDiscovery` interface instead of concrete `PeerRegistry`

Move `worker/PeerRegistry.kt` → `worker/adapter/http/PeerRegistry.kt`:
- Package: `com.workflow.worker.adapter.http`
- Add `: PeerDiscovery` interface implementation
- Import `com.workflow.worker.usecase.port.outbound.peer.PeerDiscovery`

Move `worker/DispatchNotifyResource.kt` → `worker/adapter/web/DispatchNotifyResource.kt`:
- Package: `com.workflow.worker.adapter.web`

- [ ] **Step 5: Move health check and config**

Move `worker/WorkerLoopHealthCheck.kt` → `worker/health/WorkerLoopHealthCheck.kt`:
- Package: `com.workflow.worker.health`

WorkerLoopConfig was already created in Task 2 at `worker/config/WorkerLoopConfig.kt`.

- [ ] **Step 6: Fix all imports across codebase**

| Old import | New import |
|---|---|
| `com.workflow.worker.TransitionHandler` | `com.workflow.worker.usecase.port.inbound.execution.TransitionHandler` |
| `com.workflow.worker.DispatchNotifier` | `com.workflow.worker.usecase.port.outbound.notification.DispatchNotifier` |
| `com.workflow.worker.DispatchNotifierImpl` | `com.workflow.worker.adapter.http.DispatchNotifierImpl` |
| `com.workflow.worker.PeerRegistry` | `com.workflow.worker.adapter.http.PeerRegistry` |
| `com.workflow.worker.WorkerLoop` | `com.workflow.worker.usecase.service.execution.WorkerLoop` |
| `com.workflow.worker.HandlerRegistry` | `com.workflow.worker.usecase.service.execution.HandlerRegistry` |
| `com.workflow.worker.MeteredTransitionHandler` | `com.workflow.worker.usecase.service.execution.MeteredTransitionHandler` |
| `com.workflow.worker.DispatchNotifyResource` | `com.workflow.worker.adapter.web.DispatchNotifyResource` |
| `com.workflow.worker.WorkerLoopHealthCheck` | `com.workflow.worker.health.WorkerLoopHealthCheck` |

Delete old worker files (all should now be in sub-packages).

- [ ] **Step 7: Compile and run tests**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -q
```

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: restructure worker domain with hexagonal ports and adapters"
```

---

### Task 9: Migrate Test Files

Move all test files to mirror the new main source structure. Update package declarations and imports.

**Files:**
- Move all files in `src/test/kotlin/` to match new package structure

- [ ] **Step 1: Create test directory structure**

```bash
# Workflow domain tests
mkdir -p src/test/kotlin/workflow/model
mkdir -p src/test/kotlin/workflow/usecase/service/orchestration
mkdir -p src/test/kotlin/workflow/usecase/service/phase
mkdir -p src/test/kotlin/workflow/adapter/persistent
mkdir -p src/test/kotlin/workflow/dsl

# Dispatch domain tests
mkdir -p src/test/kotlin/dispatch/model
mkdir -p src/test/kotlin/dispatch/usecase/service/algorithm
mkdir -p src/test/kotlin/dispatch/usecase/service/handler
mkdir -p src/test/kotlin/dispatch/usecase/service/simulation
mkdir -p src/test/kotlin/dispatch/adapter/storage
mkdir -p src/test/kotlin/dispatch/dsl

# Worker domain tests
mkdir -p src/test/kotlin/worker/usecase/service/execution
mkdir -p src/test/kotlin/worker/adapter/web
mkdir -p src/test/kotlin/worker/adapter/http
mkdir -p src/test/kotlin/worker/health

# Infrastructure tests
mkdir -p src/test/kotlin/infrastructure/leader
mkdir -p src/test/kotlin/infrastructure/shutdown
mkdir -p src/test/kotlin/infrastructure/config
mkdir -p src/test/kotlin/infrastructure/persistence
mkdir -p src/test/kotlin/infrastructure/queryexporter
```

- [ ] **Step 2: Move workflow tests**

| Old test location | New test location |
|---|---|
| `engine/WorkflowModelsTest.kt` | `workflow/model/WorkflowModelsTest.kt` |
| `engine/SequenceModelTest.kt` | `workflow/model/SequenceModelTest.kt` |
| `engine/WorkflowEngineTest.kt` | `workflow/usecase/service/orchestration/WorkflowEngineTest.kt` |
| `engine/BarrierServiceTest.kt` | `workflow/usecase/service/orchestration/BarrierServiceTest.kt` |
| `engine/SweeperTest.kt` | `workflow/usecase/service/orchestration/SweeperTest.kt` |
| `engine/InputResolverTest.kt` | `workflow/usecase/service/orchestration/InputResolverTest.kt` |
| `engine/LinearPhaseStrategyTest.kt` | `workflow/usecase/service/phase/LinearPhaseStrategyTest.kt` |
| `engine/ParallelPhaseStrategyTest.kt` | `workflow/usecase/service/phase/ParallelPhaseStrategyTest.kt` |
| `engine/PhaseStrategyRegistryTest.kt` | `workflow/usecase/service/phase/PhaseStrategyRegistryTest.kt` |
| `engine/RepositoryTest.kt` | `workflow/adapter/persistent/RepositoryTest.kt` |
| `engine/SchemaTest.kt` | `workflow/adapter/persistent/SchemaTest.kt` |
| `engine/IdempotencyKeyTest.kt` | `workflow/adapter/persistent/IdempotencyKeyTest.kt` |
| `engine/WorkflowIntegrationTest.kt` | `workflow/adapter/persistent/WorkflowIntegrationTest.kt` |
| `engine/OracleTestContainer.kt` | `workflow/adapter/persistent/OracleTestContainer.kt` |
| `dsl/WorkflowDslTest.kt` | `workflow/dsl/WorkflowDslTest.kt` |
| `dsl/WorkflowDslBuildersTest.kt` | `workflow/dsl/WorkflowDslBuildersTest.kt` |

For each file: change package declaration and update all imports.

- [ ] **Step 3: Move dispatch tests**

| Old test location | New test location |
|---|---|
| `dispatch/model/DispatchModelsTest.kt` | `dispatch/model/DispatchModelsTest.kt` |
| `dispatch/algorithm/DispatchAlgorithmTest.kt` | `dispatch/usecase/service/algorithm/DispatchAlgorithmTest.kt` |
| `dispatch/algorithm/DispatchAlgorithmDslTest.kt` | `dispatch/dsl/DispatchAlgorithmDslTest.kt` |
| `dispatch/algorithm/CandidateMatcherTest.kt` | `dispatch/usecase/service/algorithm/CandidateMatcherTest.kt` |
| `dispatch/algorithm/GapComputerTest.kt` | `dispatch/usecase/service/algorithm/GapComputerTest.kt` |
| `dispatch/algorithm/SelectionKernelTest.kt` | `dispatch/usecase/service/algorithm/SelectionKernelTest.kt` |
| `dispatch/handler/DispatchHandlersTest.kt` | `dispatch/usecase/service/handler/DispatchHandlersTest.kt` |
| `dispatch/port/DefaultCsvFormatterTest.kt` | `dispatch/adapter/storage/DefaultCsvFormatterTest.kt` |
| `dispatch/adapter/S3StorageAdapterTest.kt` | `dispatch/adapter/storage/S3StorageAdapterTest.kt` |
| `dispatch/simulation/SimulationEngineTest.kt` | `dispatch/usecase/service/simulation/SimulationEngineTest.kt` |
| `dispatch/simulation/CandidateIndexTest.kt` | `dispatch/model/CandidateIndexTest.kt` |

- [ ] **Step 4: Move worker tests**

| Old test location | New test location |
|---|---|
| `worker/WorkerLoopTest.kt` | `worker/usecase/service/execution/WorkerLoopTest.kt` |
| `worker/HandlerRegistryTest.kt` | `worker/usecase/service/execution/HandlerRegistryTest.kt` |
| `worker/MeteredTransitionHandlerTest.kt` | `worker/usecase/service/execution/MeteredTransitionHandlerTest.kt` |
| `worker/DispatchNotifierTest.kt` | `worker/adapter/http/DispatchNotifierTest.kt` |
| `worker/DispatchNotifyResourceTest.kt` | `worker/adapter/web/DispatchNotifyResourceTest.kt` |
| `worker/PeerRegistryTest.kt` | `worker/adapter/http/PeerRegistryTest.kt` |
| `worker/WorkerLoopHealthCheckTest.kt` | `worker/health/WorkerLoopHealthCheckTest.kt` |
| `worker/FakeDispatchNotifier.kt` | `worker/adapter/http/FakeDispatchNotifier.kt` |

- [ ] **Step 5: Move infrastructure tests**

| Old test location | New test location |
|---|---|
| `leader/LeaderManagerTest.kt` | `infrastructure/leader/LeaderManagerTest.kt` |
| `leader/LeaderHealthCheckTest.kt` | `infrastructure/leader/LeaderHealthCheckTest.kt` |
| `leader/KubernetesDetectorTest.kt` | `infrastructure/leader/KubernetesDetectorTest.kt` |
| `leader/NotLeaderTest.kt` | `infrastructure/leader/NotLeaderTest.kt` |
| `shutdown/ShutdownCoordinatorTest.kt` | `infrastructure/shutdown/ShutdownCoordinatorTest.kt` |
| `shutdown/ShutdownSignalTest.kt` | `infrastructure/shutdown/ShutdownSignalTest.kt` |
| `config/ConfigValidatorTest.kt` | `infrastructure/config/ConfigValidatorTest.kt` |
| `config/FrameworkConfigTest.kt` | `infrastructure/config/FrameworkConfigTest.kt` |
| `config/ConfigOnlyTestProfile.kt` | `infrastructure/config/ConfigOnlyTestProfile.kt` |
| `extension/FlowExtensionTest.kt` | `infrastructure/coroutine/FlowExtensionTest.kt` |
| `extension/JdbiExtensionTest.kt` | `infrastructure/persistence/JdbiExtensionTest.kt` |
| `queryexporter/*Test.kt` | `infrastructure/queryexporter/*Test.kt` |

- [ ] **Step 6: Update benchmark and stress test imports**

Files in `benchmark/` and `stress/` stay in their current directories but need import updates for all renamed packages. Go through each file and update imports.

- [ ] **Step 7: Delete old test directories**

Remove all empty test directories after moves: `engine/`, `dsl/`, `leader/`, `shutdown/`, `config/`, `extension/`, `worker/` (old files), `dispatch/algorithm/`, `dispatch/handler/`, `dispatch/port/`, `dispatch/simulation/`, `dispatch/adapter/`, `queryexporter/`.

- [ ] **Step 8: Compile and run full test suite**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test
```

Expect all tests to pass with no behavioral changes.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "refactor: migrate all test files to match hexagonal package structure"
```

---

### Task 10: Final Cleanup and Verification

Remove any remaining empty directories, verify no old packages remain, run full test suite with coverage.

- [ ] **Step 1: Verify no old source files remain**

```bash
# Should return nothing — all old directories should be empty/deleted
ls src/main/kotlin/engine/ 2>/dev/null
ls src/main/kotlin/dsl/ 2>/dev/null
ls src/main/kotlin/config/ 2>/dev/null
ls src/main/kotlin/extension/ 2>/dev/null
ls src/main/kotlin/leader/ 2>/dev/null
ls src/main/kotlin/shutdown/ 2>/dev/null
ls src/main/kotlin/queryexporter/ 2>/dev/null
```

If any remain, delete them.

- [ ] **Step 2: Verify package structure**

```bash
# List all top-level packages — should only be: infrastructure, workflow, dispatch, worker
ls src/main/kotlin/
```

Expected output:
```
dispatch
infrastructure
worker
workflow
```

- [ ] **Step 3: Run full test suite**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test
```

All tests must pass.

- [ ] **Step 4: Run coverage check**

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn verify -Djacoco
python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70
```

Coverage should be unchanged from before the refactoring.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: final cleanup — remove old directories and verify hexagonal structure"
```
