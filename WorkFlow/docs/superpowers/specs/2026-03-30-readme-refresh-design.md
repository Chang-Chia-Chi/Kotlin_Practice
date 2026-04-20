# README Refresh — Design Spec

**Date:** 2026-03-30
**Status:** Approved

## Goal

Full refresh of `README.md` to reflect all architectural changes since the 2026-03-28 overhaul: independent fan-out, input resolution, PhaseStrategy pattern, event-driven dispatch, and updated configuration.

## Decisions

### Structure: Architecture Narrative (Approach B)

Reorganized the Architecture section as execution layers instead of three flat subsections:

1. **Declarative DSL** — three examples (linear, fan-out with inputs, advanced with queue routing/backoff)
2. **Engine Core** — PhaseStrategy pattern, barrier evaluation, CAS advancement, two-table model
3. **Worker Layer** — event-driven dispatch (SharedFlow + HTTP broadcast + K8s Endpoints Watch)
4. **Resilience** — leader sweeper, graceful shutdown, health probes

Rationale: tells a coherent story from definition → execution → recovery. Naturally surfaces PhaseStrategy and dispatch as first-class concepts without bolting them on.

### DSL Examples

Three examples showing the full range:

1. **Linear** — simple 3-activity ETL pipeline, no fan-out
2. **Fan-out with inputs** — scatter + independent fan-out + `inputs {}` block, with reading guide
3. **Advanced** — queue routing (`queue("io-bound")`), custom join policy (`Threshold(10)`), exponential backoff (`backoffBase`/`backoffCap`)

### Key Content Changes

| Section | Change |
|---------|--------|
| Title one-liner | Added "Workers wake instantly via event-driven dispatch" |
| DSL | Replaced old nested `fanOut { }` syntax with independent `fanOut("target")`. Added `inputs {}` examples. |
| Architecture | Replaced three flat subsections with four narrative layers. Added PhaseStrategy, InputResolver, DispatchNotifier as first-class concepts. |
| Project Structure | Updated file listing: added DispatchNotifier, PeerRegistry, PhaseStrategyRegistry, InputResolver, ShutdownCoordinator |
| Configuration | Replaced `poll-interval` with `fallback-poll-interval`. Added `max-batch-size`, `pod-ip`. Removed `service-name` (infrastructure detail). |
| Documentation links | Unchanged — `docs/design.md` + `docs/superpowers/specs/` |

### What Was Removed

- Old nested `fanOut { }` DSL example (syntax no longer exists)
- `poll-interval` config property (replaced by `fallback-poll-interval`)
- Reference to `FrameworkConfig (SmallRye @ConfigMapping)` in project structure was kept but description updated

### Out of Scope

- Updating `docs/design.md` — separate effort
- Adding dispatch simulation content — README stays engine-focused per user decision
- Feature highlights section — rejected in approach selection (Approach C)
