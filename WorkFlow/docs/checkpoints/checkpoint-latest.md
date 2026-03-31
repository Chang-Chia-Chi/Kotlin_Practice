# Checkpoint 20260331-0100

## Session
- Task: docs/superpowers/plans/2026-03-30-hexagonal-refactoring.md
- Current Phase: Phase 3 (Build)
- Completed Phases: Phase 1 (Understand), Phase 2 (Contract Alignment)

## Locked Contract

### New Port Interfaces
| Interface | Package |
|-----------|---------|
| `WorkflowOperations` | `c.w.workflow.usecase.port.inbound.orchestration` |
| `BarrierOperations` | `c.w.workflow.usecase.port.inbound.orchestration` |
| `PhaseStrategy` | `c.w.workflow.usecase.port.inbound.phase` |
| `WorkflowRepository` (port) | `c.w.workflow.usecase.port.outbound.persistent` |
| `TaskRepository` (port) | `c.w.workflow.usecase.port.outbound.persistent` |
| `DispatchAlgorithm` | `c.w.dispatch.usecase.port.inbound.algorithm` |
| `DispatchAlgorithmFactory` | `c.w.dispatch.usecase.port.inbound.algorithm` |
| `CandidateMatcher` | `c.w.dispatch.usecase.port.inbound.algorithm` |
| `GapComputer` | `c.w.dispatch.usecase.port.inbound.algorithm` |
| `TerminationStrategy` | `c.w.dispatch.usecase.port.inbound.algorithm` |
| `PeerDiscovery` | `c.w.worker.usecase.port.outbound.peer` |

### Renamed Classes
| Old | New | Package |
|-----|-----|---------|
| `WorkflowRepository` (class) | `JdbiWorkflowRepository` | `c.w.workflow.adapter.persistent` |
| `TaskRepository` (class) | `JdbiTaskRepository` | `c.w.workflow.adapter.persistent` |

### New Config Interfaces
| Interface | Package | Prefix |
|-----------|---------|--------|
| `SweeperConfig` | `c.w.workflow.config` | `framework.sweeper` |
| `WorkerLoopConfig` | `c.w.worker.config` | `framework.worker` |
| `LeaderElectionConfig` | `c.w.infrastructure.leader` | `framework.leader-election` |
| `ShutdownConfig` | `c.w.infrastructure.shutdown` | `framework.shutdown` |
| `FrameworkConfig` (reduced) | `c.w.infrastructure.config` | `framework` (serviceName only) |

### Constructor Changes
| Class | New Deps |
|-------|----------|
| `WorkflowEngine` | `Jdbi, WorkflowRepository (port), TaskRepository (port), ObjectMapper, DispatchNotifier` |
| `BarrierService` | `Jdbi, WorkflowRepository (port), TaskRepository (port), ObjectMapper, PhaseStrategyRegistry, DispatchNotifier` |
| `Sweeper` | `Jdbi, WorkflowRepository (port), TaskRepository (port), BarrierService, SweeperConfig` |
| `WorkerLoop` | `WorkerLoopConfig, ShutdownConfig, TaskRepository (port), HandlerRegistry, BarrierService, MeterRegistry, InputResolver, WorkflowRepository (port), ObjectMapper, DispatchNotifier` |
| `WorkerLoopHealthCheck` | `WorkerLoop, WorkerLoopConfig` |
| `LeaderManager` | `LeaderElectionConfig, WorkerLoopConfig, ShutdownConfig, KubernetesClient, MeterRegistry, KubernetesDetector` |
| `ShutdownCoordinator` | `Instance<ShutdownParticipant>, MeterRegistry, ShutdownConfig` |
| `PeerRegistry` | `KubernetesClient, FrameworkConfig, WorkerLoopConfig, LeaderElectionConfig, KubernetesDetector` |
| `DispatchNotifierImpl` | `PeerDiscovery, HttpClient` |
| `ConfigValidator` | `WorkerLoopConfig, LeaderElectionConfig` |

### Key Rules
- Services inject port interfaces; tests inject concrete for integration / mock port for unit
- OracleTestContainer → `src/test/kotlin/infrastructure/persistence/`
- Dispatch adapters keep current names
- `BarrierService.recoverStuckWorkflow` drops `internal` to implement port
- `createTaskForActivity` stays `internal` (same module)
- JDBI Handle in ports accepted, defer cleanup

## Agent State
| Agent | Last Phase | Status | Output Location |
|-------|-----------|--------|-----------------|
| sdet | Phase 2 | RATE LIMITED | — |
| engineer | Phase 2 | DONE | — |
| reviewer | Phase 2 | DONE (APPROVE CONTRACT) | — |

## Plan Deviations
1. OracleTestContainer → `infrastructure/persistence/` (not `workflow/adapter/persistent/`)
2. PeerRegistry needs 3 config injections (FrameworkConfig + WorkerLoopConfig + LeaderElectionConfig)
3. LeaderManager needs WorkerLoopConfig + LeaderElectionConfig + ShutdownConfig

## Risk Mitigations
1. PeerRegistry triple-injection in Task 2
2. LeaderManager cross-domain config in Task 2
3. BarrierService.recoverStuckWorkflow — drop `internal` for port
4. Extension functions (readClob, readTimestamp, etc.) need explicit imports after move
5. Verify `quarkus.arc.remove-unused-beans=none` preserved
6. Verify `@Scheduled` annotation resolves after config split
7. Star-import extension functions must be explicit after package move

## Decisions Log
| # | Decision | Rationale |
|---|----------|-----------|
| 1 | `createTaskForActivity` stays `internal` | Same Kotlin module, cross-package internal works |
| 2 | Services inject port; tests inject concrete or mock port | Hexagonal correctness for services, practical for tests |
| 3 | Config property keys match, no app.properties changes | Verified prefix resolution identical |
| 4 | OracleTestContainer to infrastructure/persistence/ | Shared test infra, not single domain |
| 5 | Dispatch adapters keep names | No naming collision, different tech |
| 6 | JDBI Handle in ports accepted | Structural refactoring only, defer behavioral change |

## Files to Re-read
- docs/superpowers/plans/2026-03-30-hexagonal-refactoring.md
- src/main/kotlin/config/FrameworkConfig.kt
- src/main/kotlin/engine/WorkflowRepository.kt
- src/main/kotlin/engine/TaskRepository.kt
- src/main/kotlin/engine/BarrierService.kt
- src/main/kotlin/engine/WorkflowEngine.kt
- src/main/kotlin/engine/Sweeper.kt
