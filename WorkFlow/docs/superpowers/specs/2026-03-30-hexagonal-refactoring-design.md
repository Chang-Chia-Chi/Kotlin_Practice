# Hexagonal Architecture Refactoring Design

## Summary

Refactor the project from a partially-layered structure into domain-first hexagonal (ports & adapters) architecture. Each business domain owns its full vertical slice (model, usecase/port, usecase/service, adapter). Shared infrastructure lives in a top-level `infrastructure/` package. Support domains (leader, shutdown, query-exporter) stay light under `infrastructure/`.

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Top-level organization | Domain-first (B) | Package-by-feature scales better, keeps bounded contexts self-contained |
| Hexagonal depth | Proportional (B) | Full hexagonal for core domains; light for support domains |
| Shared adapters | `infrastructure/` package | Shared clients (S3, JDBI, HTTP) in one place; domain adapters use them |
| Config ownership | Split per domain | Each domain owns its `@ConfigMapping`; no monolithic config |
| File granularity | One class/interface per file | Interfaces split from implementations; data classes in separate files |
| `FrameworkConfig` rename | `WorkflowConfig` (eliminated) | Split into `WorkerConfig`, `SweeperConfig`, `LeaderElectionConfig`, `ShutdownConfig` per domain |
| Algorithm placement | Under `dispatch/usecase/` | Algorithm interfaces are inbound ports; implementations are services |

## Target Layout

### Top-level

```
src/main/kotlin/
  infrastructure/       # Shared clients, producers, framework glue, support domains
  workflow/             # Core domain: workflow orchestration (full hexagonal)
  dispatch/             # Core domain: candidate allocation (full hexagonal)
  worker/               # Core domain: task execution loop (full hexagonal)
```

### Workflow Domain

```
workflow/
  model/
    WorkflowStatus.kt              # enum WorkflowStatus
    TaskStatus.kt                   # enum TaskStatus
    WorkflowRun.kt                  # data class WorkflowRun
    Task.kt                         # data class Task + createTaskForActivity()
    StartResult.kt                  # sealed interface StartResult
    SequenceModel.kt                # SequenceInfo, SequenceMap types
    WorkflowDefinition.kt           # data class WorkflowDefinition
    ActivityDefinition.kt           # data class ActivityDefinition
    FailurePolicy.kt                # enum FailurePolicy
    JoinPolicy.kt                   # sealed interface JoinPolicy + variants
    PhaseContext.kt                  # data class PhaseContext + extension fns
    AdvancementDecision.kt          # sealed interface AdvancementDecision + variants
  usecase/
    port/
      inbound/
        orchestration/
          WorkflowOperations.kt     # Interface: start, cancel, replay
          BarrierOperations.kt      # Interface: onTaskComplete, advance
        phase/
          PhaseStrategy.kt          # Interface: phase advancement contract
      outbound/
        persistent/
          WorkflowRepository.kt     # Interface: workflow CRUD
          TaskRepository.kt         # Interface: task CRUD
    service/
      orchestration/
        WorkflowEngine.kt          # Implements WorkflowOperations
        BarrierService.kt          # Implements BarrierOperations
        Sweeper.kt                 # Scheduled stale-task reclaim
        InputResolver.kt           # Resolves inputs from prior outputs
      phase/
        LinearPhaseStrategy.kt     # Implements PhaseStrategy
        ParallelPhaseStrategy.kt   # Implements PhaseStrategy
        PhaseStrategyRegistry.kt   # Strategy lookup
  adapter/
    persistent/
      JdbiWorkflowRepository.kt    # Implements WorkflowRepository
      JdbiTaskRepository.kt        # Implements TaskRepository
  config/
    SweeperConfig.kt                # @ConfigMapping(prefix = "workflow.sweeper")
  dsl/
    WorkflowDslBuilders.kt         # Builder functions
```

**Migration from current structure:**
- `engine/WorkflowModels.kt` -> split into `model/WorkflowStatus.kt`, `model/TaskStatus.kt`, `model/WorkflowRun.kt`, `model/Task.kt`, `model/StartResult.kt`
- `engine/SequenceModel.kt` -> `model/SequenceModel.kt`
- `engine/PhaseStrategy.kt` -> split: interface to `usecase/port/inbound/phase/PhaseStrategy.kt`, `PhaseContext` to `model/PhaseContext.kt`, `AdvancementDecision` to `model/AdvancementDecision.kt`
- `engine/WorkflowEngine.kt` -> extract interface to `usecase/port/inbound/orchestration/WorkflowOperations.kt`, impl to `usecase/service/orchestration/WorkflowEngine.kt`
- `engine/BarrierService.kt` -> extract interface to `usecase/port/inbound/orchestration/BarrierOperations.kt`, impl to `usecase/service/orchestration/BarrierService.kt`
- `engine/Sweeper.kt` -> `usecase/service/orchestration/Sweeper.kt`
- `engine/InputResolver.kt` -> `usecase/service/orchestration/InputResolver.kt`
- `engine/LinearPhaseStrategy.kt` -> `usecase/service/phase/LinearPhaseStrategy.kt`
- `engine/ParallelPhaseStrategy.kt` -> `usecase/service/phase/ParallelPhaseStrategy.kt`
- `engine/PhaseStrategyRegistry.kt` -> `usecase/service/phase/PhaseStrategyRegistry.kt`
- `engine/WorkflowRepository.kt` -> extract interface to `usecase/port/outbound/persistent/WorkflowRepository.kt`, impl to `adapter/persistent/JdbiWorkflowRepository.kt`
- `engine/TaskRepository.kt` -> extract interface to `usecase/port/outbound/persistent/TaskRepository.kt`, impl to `adapter/persistent/JdbiTaskRepository.kt`
- `engine/RowMapperUtils.kt` -> `infrastructure/persistence/RowMapperUtils.kt`
- `dsl/WorkflowDsl.kt` -> split into `model/WorkflowDefinition.kt`, `model/ActivityDefinition.kt`, `model/FailurePolicy.kt`, `model/JoinPolicy.kt`
- `dsl/WorkflowDslBuilders.kt` -> `dsl/WorkflowDslBuilders.kt`
- `config/FrameworkConfig.kt` -> split: `WorkerConfig` to `worker/config/WorkerLoopConfig.kt`, `SweeperConfig` to `workflow/config/SweeperConfig.kt`, `LeaderElectionConfig` to `infrastructure/leader/LeaderElectionConfig.kt`, `ShutdownConfig` to `infrastructure/shutdown/ShutdownConfig.kt`

### Dispatch Domain

```
dispatch/
  model/
    DispatchConfig.kt               # data class DispatchConfig
    CandidateProduct.kt             # data class CandidateProduct
    DispatchDecision.kt             # data class DispatchDecision
    Baseline.kt                     # data class Baseline
    DispatchMode.kt                 # enum DispatchMode
    TerminationDecision.kt          # enum TerminationDecision
    SimulationContext.kt            # data class SimulationContext
    CandidateIndex.kt              # CandidateIndex lookup structure
  usecase/
    port/
      inbound/
        algorithm/
          DispatchAlgorithm.kt      # Interface: core algorithm contract
          DispatchAlgorithmFactory.kt # Interface: algorithm factory
          CandidateMatcher.kt       # Interface: candidate matching
          GapComputer.kt            # Interface: gap computation
          TerminationStrategy.kt    # Interface: termination logic
        handler/
          DispatchOperations.kt     # Interface: scatter, join, simulate
      outbound/
        persistence/
          DispatchConfigRepository.kt # Interface: config CRUD
          CandidateQueryPort.kt     # Interface: candidate queries
          BaselineProvider.kt       # Interface: site/BOM allocations
          SimulationResultStore.kt  # Interface: persist decisions
        storage/
          StoragePort.kt            # Interface: file storage (S3, etc.)
          CsvFormatter.kt           # Interface: CSV formatting
          ParquetFormatter.kt       # Interface: Parquet formatting
    service/
      algorithm/
        DefaultDispatchAlgorithm.kt     # Implements DispatchAlgorithm
        DefaultDispatchAlgorithmFactory.kt # Implements DispatchAlgorithmFactory
        DefaultCandidateMatcher.kt      # Implements CandidateMatcher
        QtyCandidateMatcher.kt          # Implements CandidateMatcher
        QtyGapComputer.kt               # Implements GapComputer
        RatioGapComputer.kt             # Implements GapComputer
        FailFastTermination.kt          # Implements TerminationStrategy
        SelectionKernel.kt              # Pure computation: selectByGap()
      handler/
        DispatchScatterHandler.kt
        DispatchJoinHandler.kt
        DispatchSimulationHandler.kt
        DispatchScheduler.kt
      simulation/
        SimulationEngine.kt            # Core simulation execution
  adapter/
    storage/
      S3StorageAdapter.kt              # Implements StoragePort
      DefaultCsvFormatter.kt           # Implements CsvFormatter
      NoOpParquetFormatter.kt          # Implements ParquetFormatter
  dsl/
    DispatchWorkflow.kt                # Predefined workflow definition
    DispatchAlgorithmDsl.kt            # Algorithm DSL builder
```

**Migration from current structure:**
- `dispatch/model/DispatchModels.kt` -> split into individual files under `model/`
- `dispatch/port/DispatchPorts.kt` -> split into individual interface files under `usecase/port/outbound/`
- `dispatch/algorithm/CandidateMatcher.kt` -> split: interface to `usecase/port/inbound/algorithm/CandidateMatcher.kt`, `DefaultCandidateMatcher` to `usecase/service/algorithm/DefaultCandidateMatcher.kt`, `QtyCandidateMatcher` to `usecase/service/algorithm/QtyCandidateMatcher.kt`
- `dispatch/algorithm/GapComputer.kt` -> split: interface to `usecase/port/inbound/algorithm/GapComputer.kt`, impls to `usecase/service/algorithm/`
- `dispatch/algorithm/TerminationStrategy.kt` -> split: interface to `usecase/port/inbound/algorithm/TerminationStrategy.kt`, `TerminationDecision` to `model/TerminationDecision.kt`, `FailFastTermination` to `usecase/service/algorithm/FailFastTermination.kt`
- `dispatch/algorithm/DispatchAlgorithm.kt` -> split: interface to `usecase/port/inbound/algorithm/DispatchAlgorithm.kt`, `DefaultDispatchAlgorithm` to `usecase/service/algorithm/DefaultDispatchAlgorithm.kt`
- `dispatch/algorithm/DispatchAlgorithmFactory.kt` -> split: interface to `usecase/port/inbound/algorithm/DispatchAlgorithmFactory.kt`, impl to `usecase/service/algorithm/DefaultDispatchAlgorithmFactory.kt`
- `dispatch/algorithm/SelectionKernel.kt` -> `usecase/service/algorithm/SelectionKernel.kt`
- `dispatch/algorithm/DispatchAlgorithmDsl.kt` -> `dsl/DispatchAlgorithmDsl.kt`
- `dispatch/handler/DispatchScatterHandler.kt` -> `usecase/service/handler/DispatchScatterHandler.kt`
- `dispatch/handler/DispatchJoinHandler.kt` -> `usecase/service/handler/DispatchJoinHandler.kt`
- `dispatch/handler/DispatchSimulationHandler.kt` -> `usecase/service/handler/DispatchSimulationHandler.kt`
- `dispatch/handler/DispatchScheduler.kt` -> `usecase/service/handler/DispatchScheduler.kt`
- `dispatch/handler/DispatchWorkflow.kt` -> `dsl/DispatchWorkflow.kt`
- `dispatch/simulation/SimulationEngine.kt` -> `usecase/service/simulation/SimulationEngine.kt`
- `dispatch/simulation/SimulationContext.kt` -> `model/SimulationContext.kt`
- `dispatch/simulation/CandidateIndex.kt` -> `model/CandidateIndex.kt`
- `dispatch/adapter/S3StorageAdapter.kt` -> `adapter/storage/S3StorageAdapter.kt`
- `dispatch/adapter/S3ClientProducer.kt` -> `infrastructure/storage/S3ClientProducer.kt`
- `dispatch/port/DefaultCsvFormatter.kt` -> `adapter/storage/DefaultCsvFormatter.kt`
- `dispatch/port/NoOpParquetFormatter.kt` -> `adapter/storage/NoOpParquetFormatter.kt`

### Worker Domain

```
worker/
  usecase/
    port/
      inbound/
        execution/
          TransitionHandler.kt      # Interface: handler contract
      outbound/
        notification/
          DispatchNotifier.kt       # Interface: cross-pod notification
        peer/
          PeerDiscovery.kt          # Interface: peer pod discovery
    service/
      execution/
        WorkerLoop.kt
        HandlerRegistry.kt
        MeteredTransitionHandler.kt # Decorator
  adapter/
    web/
      DispatchNotifyResource.kt     # REST controller
    http/
      DispatchNotifierImpl.kt       # Implements DispatchNotifier
      PeerRegistry.kt              # Implements PeerDiscovery
  config/
    WorkerLoopConfig.kt
  health/
    WorkerLoopHealthCheck.kt
```

**Migration from current structure:**
- `worker/TransitionHandler.kt` -> `usecase/port/inbound/execution/TransitionHandler.kt`
- `worker/DispatchNotifier.kt` -> split: interface to `usecase/port/outbound/notification/DispatchNotifier.kt`, `DispatchNotifierImpl` to `adapter/http/DispatchNotifierImpl.kt`
- `worker/PeerRegistry.kt` -> extract interface `PeerDiscovery` to `usecase/port/outbound/peer/PeerDiscovery.kt`, impl to `adapter/http/PeerRegistry.kt`
- `worker/WorkerLoop.kt` -> `usecase/service/execution/WorkerLoop.kt`
- `worker/HandlerRegistry.kt` -> `usecase/service/execution/HandlerRegistry.kt`
- `worker/MeteredTransitionHandler.kt` -> `usecase/service/execution/MeteredTransitionHandler.kt`
- `worker/DispatchNotifyResource.kt` -> `adapter/web/DispatchNotifyResource.kt`
- `worker/HttpClientProducer.kt` -> `infrastructure/http/HttpClientProducer.kt`
- `worker/WorkerLoopHealthCheck.kt` -> `health/WorkerLoopHealthCheck.kt`

### Infrastructure

```
infrastructure/
  config/
    ConfigValidator.kt
  persistence/
    JdbiExtension.kt
    RowMapperUtils.kt
  storage/
    S3ClientProducer.kt
  http/
    HttpClientProducer.kt
  coroutine/
    FlowExtension.kt
  leader/
    LeaderElection.kt               # interface
    LeaderManager.kt                 # implements LeaderElection + ShutdownParticipant
    LeaderHealthCheck.kt
    KubernetesDetector.kt            # interface
    EnvKubernetesDetector.kt         # implements KubernetesDetector (split from KubernetesDetector.kt)
    NotLeader.kt
    LeaderElectionConfig.kt          # @ConfigMapping(prefix = "workflow.leader-election")
  shutdown/
    ShutdownParticipant.kt           # interface
    ShutdownCoordinator.kt
    ShutdownSignal.kt
    ShutdownState.kt
    ShutdownConfig.kt                # @ConfigMapping(prefix = "workflow.shutdown")
  queryexporter/
    config/
      ExporterConfig.kt
      QueryConfig.kt
      ScheduleConfig.kt
      MetricConfig.kt
      MetricType.kt
      ExporterConfigValidator.kt
    core/
      QueryScheduler.kt
      QueryExecutor.kt
      MetricWriter.kt
    spi/
      DataSourceProvider.kt
      LeaderGuard.kt
    adapter/
      QuarkusDataSourceProvider.kt
      LeaderManagerGuardAdapter.kt
    bootstrap/
      QueryExporterBean.kt
      QueryExporterBootstrap.kt
```

## Dependency Rules

1. **`model/`** depends on nothing (pure data)
2. **`usecase/port/`** depends only on `model/`
3. **`usecase/service/`** depends on `model/` and `port/` (both inbound and outbound)
4. **`adapter/`** depends on `port/outbound/` and `infrastructure/`
5. **`infrastructure/`** depends on nothing domain-specific
6. **Cross-domain:** domains may depend on each other's inbound ports, never on services or adapters directly

```
adapter/ ──> usecase/port/outbound/ ──> model/
                                          ^
usecase/service/ ──> usecase/port/inbound/ ─┘
       |
       └──> usecase/port/outbound/

infrastructure/ (no domain dependencies)
```

## Test Structure

Test packages mirror the main source layout:

```
src/test/kotlin/
  workflow/
    model/
    usecase/service/orchestration/
    usecase/service/phase/
    adapter/persistent/
    dsl/
  dispatch/
    model/
    usecase/service/algorithm/
    usecase/service/handler/
    usecase/service/simulation/
    adapter/storage/
    dsl/
  worker/
    usecase/service/execution/
    adapter/web/
    adapter/http/
    health/
  infrastructure/
    leader/
    shutdown/
    config/
    persistence/
    queryexporter/
  benchmark/              # stays as-is (cross-cutting); imports updated to new packages
  stress/                 # stays as-is (cross-cutting); imports updated to new packages
```

## Scope and Constraints

- **Package rename only** for files that don't need splitting; preserve all logic
- **Interface extraction** for classes that become ports (WorkflowEngine, BarrierService, WorkflowRepository, TaskRepository, PeerRegistry)
- **File splitting** for multi-class files (WorkflowModels.kt, DispatchModels.kt, PhaseStrategy.kt, ExporterConfig.kt, etc.)
- **No behavioral changes** — all existing tests must pass after refactoring
- **Config prefix** stays `framework.*` in property files for now (rename to `workflow.*` is a separate concern)
