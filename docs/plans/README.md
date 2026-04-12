# DynaCache Implementation Plan — Index

> **Spec:** `docs/superpowers/specs/2026-04-12-dynacache-design.md`

4 phases, MIT 6.824-style. Each phase produces a working system. Sub-phases teach one concept each.

| Phase | Theme | Sub-phases | What works when done |
|---|---|---|---|
| **P1** | Data Engine + Single Node | 1A–1F | `redis-cli` works against a single-node DynaCache |
| **P2** | Distribution: Ring + Gossip + Replication | 2A–2D | 3-node cluster serves R/W, survives minority failure |
| **P3** | Fault Tolerance: Handoff + Repair + Convergence | 3A–3D | Partitioned cluster heals and converges |
| **P4** | Persistence + Snapshots | 4A–4C | RDB warm restart + Chandy-Lamport distributed snapshots |

## Reading Order

Before each phase, read the papers listed in the phase plan's "Pre-reading" section.

## Plan Files

- [P1 — Data Engine + Single Node](./2026-04-12-dynacache-p1-data-engine.md)
- [P2 — Distribution](./2026-04-12-dynacache-p2-distribution.md)
- [P3 — Fault Tolerance](./2026-04-12-dynacache-p3-fault-tolerance.md)
- [P4 — Persistence + Snapshots](./2026-04-12-dynacache-p4-persistence.md)
