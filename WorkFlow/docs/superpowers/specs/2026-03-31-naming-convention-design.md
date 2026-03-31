# Naming Convention Overhaul

**Date:** 2026-03-31
**Status:** Approved

## Problem

Class naming across the framework is inconsistent:

1. **Port/interface naming is incoherent** -- `-Port` (`StoragePort`), `-Operations` (`WorkflowOperations`), `-Bean` (`QueryExporterBean`), and bare role names all coexist with no governing rule.
2. **Implementation naming uses three different strategies** -- `Default*`, `*Impl`, and tech-prefix (`Jdbi*`, `S3*`).
3. **Orchestrator-level classes lack a consistent suffix rule** -- `Service`, `Engine`, `Manager`, `Coordinator` are used interchangeably.
4. **Several names lack domain context or hide purpose** -- `Sweeper`, `InputResolver`, `SelectionEntry`, `PhaseStrategy`.

## Naming Convention

Four rules govern all future naming:

1. **Domain noun first, purpose/role second.** The primary domain concept appears first, followed by what it does: `WorkflowLifecycle`, `PhaseGate`, `AdvancementStrategy`.
2. **No architectural jargon.** No `-Port`, `-Operations`, `-Bean`. The package path encodes the hex architecture role; the class name encodes domain intent.
3. **Implementation naming by behavioral or tech prefix.**
   - Multiple impls with behavioral distinction: descriptive prefix (`Linear*`, `Parallel*`, `FailFast*`, `FirstFit*`, `GapBased*`).
   - Single impl with tech differentiator: tech prefix (`Jdbi*`, `S3*`, `Http*`).
   - Factory with single impl: `Default*` is acceptable (the behavioral distinction is in what they produce, not what they are).
   - Never `*Impl`.
4. **Port interfaces use standardized role suffixes** that implicitly signal direction:

| Role suffix | Implies |
|---|---|
| `Repository` | outbound persistence |
| `Gateway` | outbound external system |
| `Notifier` / `Publisher` | outbound messaging |
| `Strategy` | pluggable algorithm / decision logic |
| `Lifecycle` | application-layer lifecycle commands |

## Rename List (19 total)

### Workflow Domain

| # | Old | New | Rationale |
|---|---|---|---|
| 1 | `WorkflowOperations` | `WorkflowLifecycle` | start/cancel/replay = lifecycle commands |
| 2 | `BarrierOperations` | `PhaseGate` | sync gate at phase boundary |
| 3 | `BarrierService` | `DefaultPhaseGate` | single impl of PhaseGate |
| 4 | `PhaseStrategy` | `AdvancementStrategy` | strategy for advancement decisions |
| 5 | `LinearPhaseStrategy` | `LinearAdvancementStrategy` | follows interface rename |
| 6 | `ParallelPhaseStrategy` | `ParallelAdvancementStrategy` | follows interface rename |
| 7 | `PhaseStrategyRegistry` | `AdvancementStrategyRegistry` | follows interface rename |
| 8 | `Sweeper` | `WorkflowWatchdog` | monitors and recovers, not just cleanup |
| 9 | `SweeperConfig` | `WatchdogConfig` | follows class rename |
| 10 | `InputResolver` | `ActivityInputResolver` | adds domain context |

### Dispatch Domain

| # | Old | New | Rationale |
|---|---|---|---|
| 11 | `DefaultCsvFormatter` | `DispatchCsvFormatter` | domain prefix instead of lazy "Default" |
| 12 | `StoragePort` | `StorageGateway` | drop -Port, standard role suffix |
| 13 | `CandidateQueryPort` | `CandidateRepository` | drop -Port, standard role suffix |
| 14 | `SelectionEntry` | `GapEntry` | pairs with `selectByGap()` |
| 15 | `SelectionKernel` (file) | `GapKernel` (file) | follows `GapEntry` rename |
| 16 | `DefaultCandidateMatcher` | `FirstFitCandidateMatcher` | first-fit matching, contrasts with `QtyCandidateMatcher` |
| 17 | `DefaultDispatchAlgorithm` | `GapBasedDispatchAlgorithm` | gap-based selection, matches `GapComputer`/`GapEntry` |

### Worker Domain

| # | Old | New | Rationale |
|---|---|---|---|
| 18 | `DispatchNotifierImpl` | `HttpDispatchNotifier` | tech prefix, consistent with `Jdbi*`/`S3*` |

### Infrastructure Domain

| # | Old | New | Rationale |
|---|---|---|---|
| 19 | `QueryExporterBean` | `QueryExporterLifecycle` | domain purpose instead of CDI jargon |

## Models -- No Changes

Models use proper domain language and are stable:

- **Workflow:** `Task`, `WorkflowRun`, `WorkflowDefinition`, `ActivityDefinition`, `AdvancementDecision`, `PhaseContext`, `SequenceInfo`, `JoinPolicy`, `FailurePolicy`, `StartResult`, enums.
- **Dispatch:** `Baseline`, `CandidateProduct`, `CandidateIndex`, `SimulationContext`, `SimulationResult`, `DispatchDecision`, `DispatchConfig`, `SiteTarget`, `TargetBomAllocation`, `BomMapping`, `SiteBomKey`, `TargetSelection`, `TerminationDecision`, `DispatchMode`.
- **Worker:** `TransitionHandler`, `HandlerInput`, `HandlerOutput`.

`PhaseContext` was considered for rename to `AdvancementContext` but kept because the data describes a phase snapshot, not its consumer. Naming models after what the data *is* (not what uses it) keeps the model layer stable.

## Implementation Notes

- Each rename is a find-and-replace across source, test, and config files.
- Update all imports, class references, string literals (e.g., logger names), and test class names.
- No behavioral changes -- pure mechanical rename.
- Run full test suite after each rename to verify nothing breaks.
