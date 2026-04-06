# K8s Launch-and-Watch Design

**Date:** 2026-04-07
**Status:** Approved

## Problem

`K8sJobTriggerDriver` is a watcher only — it monitors existing K8s Jobs but cannot create them. `DispatchJoinHandler` was returning `deferK8sJob(...)` as a placeholder, but the framework had no ability to actually launch the Job it was deferring to. Two gaps:

1. `DispatchJoinHandler` should complete directly (scope is small now); the `deferK8sJob` return was a test stand-in.
2. When the dispatch scope grows, the framework needs a "launch-and-watch" path: create the K8s Job, then monitor it to completion.

## Decisions

- `HandlerResult.Defer` is deleted — no callers remain after fixing the handler. Future trigger types get their own `HandlerResult` subtypes.
- `K8sJobTriggerDriver` is rewritten to use `SharedIndexInformer` (Approach 2) — delegates reconnection and caching to Fabric8, operator-grade reliability.
- Launcher and watcher share a `workflow-managed=true` label constant. The informer filters by this label so it only observes framework-managed jobs.
- No separate `K8sJobSpec` data class — `HandlerResult.LaunchK8sJob` is passed directly to the outbound port.

## Scope

### Part 1 — Immediate fix

| File | Change |
|------|--------|
| `DispatchJoinHandler` | Return `HandlerResult.Completed(result = null)` instead of `deferK8sJob(...)` |
| `DispatchE2EHappyPathTest` | Drop Step 4 (await DEFERRED) + Step 5 (push mock K8s Job); remove `K8sMockServerResource` + `K8sJobTriggerDriver` injection; assert join task `COMPLETED` directly |
| `DispatchHandlersTest` | Assert `HandlerResult.Completed`, not `deferK8sJob` |

### Part 2 — Launch-and-watch capability

| File | Change |
|------|--------|
| `HandlerResult` | Delete `Defer`; add `LaunchK8sJob` subtype |
| `TriggerTypes.kt` | Delete `deferK8sJob`; add `launchK8sJob` helper |
| `K8sJobTypes.kt` *(new)* | `K8sJobMeta` (moved from driver) + `K8sLabels` constant object |
| `KubernetesJobPort` *(new)* | Outbound port: `suspend fun launch(spec: HandlerResult.LaunchK8sJob)` |
| `K8sJobLauncherAdapter` *(new)* | Fabric8 `JobBuilder`; sets label, `backoffLimit=0`, `restartPolicy=Never` |
| `K8sJobTriggerDriver` | Rewrite to `SharedIndexInformer`; delete `TrackedJob` data class; `trackedJobs: ConcurrentHashMap<"ns/name", taskId>`; `@Volatile informer`; label filter; `readConfigMapOutputWithRetry` (3 attempts, 500 ms delay) |
| `WorkerLoop` | Replace `Defer` branch with `LaunchK8sJob`: call `kubernetesJobPort.launch(result)`, then `taskRepo.defer(triggerType=k8s-job, triggerMeta={"jobName":...,"namespace":...})` |

## Types

### `HandlerResult`

```kotlin
sealed interface HandlerResult {
    data class Completed(val result: String?, val items: String? = null) : HandlerResult
    data class LaunchK8sJob(
        val jobName: String,
        val namespace: String,
        val image: String,
        val args: List<String> = emptyList(),
        val env: Map<String, String> = emptyMap(),
    ) : HandlerResult
}
```

### `K8sJobTypes.kt` — `worker/adapter/trigger/`

```kotlin
data class K8sJobMeta(val jobName: String, val namespace: String)

object K8sLabels {
    const val WORKFLOW_MANAGED = "workflow-managed"
    const val WORKFLOW_MANAGED_VALUE = "true"
}
```

### `KubernetesJobPort` — `worker/usecase/port/outbound/`

```kotlin
interface KubernetesJobPort {
    suspend fun launch(spec: HandlerResult.LaunchK8sJob)
}
```

## Components

### `K8sJobLauncherAdapter`

- Package: `worker/adapter/trigger/` (co-located with driver and `K8sJobTypes.kt`)
- Injects: `KubernetesClient`
- Builds `Job` via Fabric8 `JobBuilder` with: name, namespace, image, args, env vars, `backoffLimit=0`, `restartPolicy=Never`, label `workflow-managed=true`
- Wraps Fabric8 call in `withContext(Dispatchers.IO)`

### `K8sJobTriggerDriver` (rewritten)

- Single `SharedIndexInformer<Job>` started once, `@Volatile`
- `inform(handler)` — no separate `.start()` call (redundant)
- Informer filters: `inAnyNamespace().withLabel(K8sLabels.WORKFLOW_MANAGED, K8sLabels.WORKFLOW_MANAGED_VALUE)`
- `trackedJobs: ConcurrentHashMap<String, String>` keyed `"namespace/jobName"` → `taskId`
- `trackedTaskIds: MutableSet<String>` for O(1) membership check (replaces `containsValue`)
- `settledTaskIds` and `eventQueue` unchanged in purpose
- `readConfigMapOutputWithRetry`: 3 attempts, 500 ms `delay` between; `internal suspend fun`
- `trackedCount()`: returns `trackedJobs.size`

### `WorkerLoop` — `LaunchK8sJob` branch

```
LaunchK8sJob received
  → kubernetesJobPort.launch(result)          // creates K8s Job
  → taskRepo.defer(taskId, "k8s-job", meta)  // task → DEFERRED
  → K8sJobTriggerDriver informs on completion
  → TriggerLoop settles task
```

## Testing

| Test | Change |
|------|--------|
| `HandlerResultTest` | Remove `Defer`; add `LaunchK8sJob` |
| `TriggerTypesTest` | Remove `deferK8sJob`; add `launchK8sJob` |
| `DispatchHandlersTest` | Join handler asserts `Completed` |
| `WorkerLoopTest` | Replace `Defer` tests with `LaunchK8sJob`: mock `KubernetesJobPort`, verify `launch()` then `taskRepo.defer()` with correct args |
| `K8sJobTriggerDriverTest` | Rewrite using `KubernetesMockServer` CRUD mode (informer needs live Watch stream) |
| `DispatchE2EHappyPathTest` | Drop K8s steps; join task asserts `COMPLETED` |
| `K8sJobLauncherAdapterTest` *(new)* | `KubernetesMockServer` CRUD mode; assert Job created with correct spec, label, `backoffLimit=0`, `restartPolicy=Never` |

## Invariants

- Watcher and launcher never cross-depend — both import `K8sLabels` from `K8sJobTypes.kt`
- `K8sJobTriggerDriver` unchanged public interface (`TriggerDriver`) — no contract break
- `taskRepo.defer()` DB call unchanged — task still transitions to `DEFERRED` in storage
