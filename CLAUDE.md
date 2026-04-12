# DynaCache — Project Instructions

## What This Is

A **learning project** — building a Dynamo-style AP distributed cache from scratch in Kotlin. Redis-compatible data structures and wire protocol, but distributed using the Dynamo paper's techniques (not Raft/Paxos). The goal is deep understanding of AP distributed systems, not just getting code done.

**Spec:** `docs/design-spec.md` — constraints, invariants, semantics. This is the source of truth.
**Learning guide:** `docs/LEARNING-GUIDE.md` — phase-by-phase path with papers to read per concept.
**Detailed implementation plan:** In the WorkFlow repo at `docs/superpowers/plans/2026-04-12-dynacache/` — has code reference snippets if needed, but the user writes their own implementation.

## Collaboration Model

**The user is learning. You are a pair programmer, not an implementer.**

- Do NOT write full implementations unprompted. The user writes the code.
- DO explain concepts, review approaches, catch design mistakes, help debug.
- When the user is stuck: ask what they've tried, explain the concept, suggest a direction — don't just write the solution.
- When the user asks you to implement something specific: do it, but explain the "why" as you go.
- When reviewing: focus on whether the invariants from the spec hold, not style nitpicks.

## 4-Phase Structure

| Phase | Theme | Key concepts | End state |
|---|---|---|---|
| **P1** | Data Engine + Single Node | Skip list, timer wheel, W-TinyLFU, RESP, Lua | `redis-cli` works against single node |
| **P2** | Distribution | Consistent hashing, SWIM gossip, DVVs, quorum R/W | 3-node cluster, minority-failure tolerant |
| **P3** | Fault Tolerance | Sloppy quorum, hinted handoff, read repair, Merkle anti-entropy, conflict merge | Partition → heal → converge |
| **P4** | Persistence + Snapshots | RDB serialization, Chandy-Lamport algorithm | Warm restart + distributed snapshots |

Each phase has sub-phases (1A, 1B, ...) that teach one concept each. The rhythm is: **read paper → write tests → build → verify key insight**.

## Current Progress

- [x] Project scaffolded — 3 Maven modules, builds clean
- [ ] **P1A:** Command model + basic GET/SET — NOT STARTED
- [ ] P1B: String completion + Hash + List
- [ ] P1C: Skip list + Sorted Set
- [ ] P1D: Hierarchical timer wheel + TTL
- [ ] P1E: Eviction — LRU + W-TinyLFU
- [ ] P1F: RESP server + MULTI/EXEC + Lua
- [ ] P2A–P2D: Distribution (hashing, gossip, DVVs, replication)
- [ ] P3A–P3D: Fault tolerance (handoff, repair, anti-entropy, convergence)
- [ ] P4A–P4C: Persistence + snapshots

**Update this checklist as phases complete.**

## Tech Stack

- **Language:** Kotlin 2.2.x, JDK 21
- **Build:** Maven 3.9.8 — always use `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn`
- **Test:** JUnit 5 + AssertJ
- **Server:** Netty 4.1.x (RESP protocol)
- **Cluster:** gRPC-Kotlin + Protobuf (inter-node)
- **Scripting:** LuaJ 3.0.x (embedded Lua)
- **No framework** — pure Kotlin + coroutines, no Quarkus/Spring/Ktor

## Module Boundaries (compile-time enforced)

```
dynacache-engine   → kotlin-stdlib ONLY (pure, no I/O)
dynacache-cluster  → engine + coroutines + gRPC
dynacache-server   → cluster + Netty + LuaJ
```

If the engine module imports Netty or gRPC, the build should fail. This is intentional.

## Key Design Decisions (locked)

- **AP, not CP** — Dynamo-style. No consensus algorithm. Conflicts detected by DVVs, resolved by merge rules.
- **DVVs, not vector clocks** — bounded by cluster size, not client count.
- **Timer wheel, not random sampling** — O(1) insert/cancel/expire for TTL.
- **W-TinyLFU** — Caffeine-style eviction with Count-Min Sketch admission filter.
- **RESP2** — Redis wire protocol so `redis-cli` and existing clients work.
- **LuaJ** — embedded Lua for atomic scripting, not a custom interpreter.
- **Chandy-Lamport** — distributed snapshots for cluster-wide consistent state capture.
- **Small fixed cluster (3–7 nodes)** — no dynamic membership.

## Papers (reference list)

| Paper | For which phase |
|---|---|
| Dynamo (DeCandia et al., 2007) | P2, P3 |
| SWIM (Das et al., 2002) | P2B |
| Skip Lists (Pugh, 1990) | P1C |
| Timer Wheels (Varghese & Lauck, 1987) | P1D |
| TinyLFU (Einziger et al., 2017) | P1E |
| DVVs (Preguica et al., 2012) | P2C |
| Chandy-Lamport (1985) | P4B |
| DDIA Ch. 5-6 (Kleppmann) | Background |

## Build & Test Commands

```bash
# Build
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn package

# Test all
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test

# Test single module
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl dynacache-engine
```
