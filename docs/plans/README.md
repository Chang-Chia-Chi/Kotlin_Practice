# DynaCache Implementation Plan — Index

> **AP Spec:** `docs/design-spec.md` · **CP Spec:** `docs/design-spec-cp.md`

5 phases, MIT 6.824-style. Each phase produces a working system. Sub-phases teach one concept each.

| Phase | Theme | Sub-phases | What works when done |
|---|---|---|---|
| **P1** | Data Engine + Single Node | 1A–1F | `redis-cli` works against a single-node DynaCache |
| **P2** | Distribution: Ring + Gossip + Replication | 2A–2D | 3-node cluster serves R/W, survives minority failure |
| **P3** | Fault Tolerance: Handoff + Repair + Convergence | 3A–3D | Partitioned cluster heals and converges |
| **P4** | Persistence + Snapshots | 4A–4C | RDB warm restart + WAL + Chandy-Lamport distributed snapshots |
| **P5** | CP Subsystem (Raft via MicroRaft) | 5A–5F | Linearizable locks, counters, semaphores, latches, CAS on `cp:*` |

## Reading Order

Before each phase, read the papers listed in the phase plan's "Pre-reading" section.

## Plan Files

- [P1 — Data Engine + Single Node](./p1-data-engine.md)
- [P2 — Distribution](./p2-distribution.md)
- [P3 — Fault Tolerance](./p3-fault-tolerance.md)
- [P4 — Persistence + Snapshots](./p4-persistence.md)
- [P5 — CP Subsystem (Raft via MicroRaft)](./p5-cp-subsystem.md)
