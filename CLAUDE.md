# DynaCache — Project Instructions

## What This Is

A **learning project** — building a distributed cache from scratch in Kotlin. Redis-compatible data structures and wire protocol. Primary engine is Dynamo-style AP; a Raft-backed CP subsystem (via MicroRaft) provides linearizable primitives (locks, counters, semaphores, latches, CAS) on the `cp:*` namespace — usable as a safe distributed lock service. Goal: deep understanding of both AP and CP distributed systems, not just getting code done.

**Specs:**
- `docs/design-spec.md` — AP engine constraints, invariants, semantics. Source of truth for P1–P4.
- `docs/design-spec-cp.md` — CP subsystem constraints, invariants, semantics. Source of truth for P5.

**Learning guide:** `docs/LEARNING-GUIDE.md` — phase-by-phase path with papers to read per concept.
**Implementation plans:** `docs/plans/` — P1 through P5, has code reference snippets if needed, but the user writes their own implementation.

## Collaboration Model

**Learn first, build second. The agent writes code, but ONLY after the user demonstrates understanding.**

### The Gate: Concept Quiz Before Implementation

Before starting ANY sub-phase implementation, you MUST:

1. **Present the concept** — explain what the sub-phase teaches, referencing the paper/reading material
2. **Quiz the user** — ask 3-5 targeted questions that test understanding of the core concept (not trivia, but "could you design this?" level)
3. **Score the answers** — rate each answer and give an overall score out of 10
4. **Gate decision:**
   - **Score >= 7/10:** PASS — proceed to implementation. Explain the "why" as you write code.
   - **Score < 7/10:** FAIL — identify gaps, explain what's missing, suggest what to re-read. Do NOT start implementation. Quiz again when the user is ready.

### Quiz Design Principles

- Questions should test *understanding*, not memorization
- "Why does X work this way?" > "What is X called?"
- "What happens if Y fails?" > "List the steps of Y"
- "How would you handle Z edge case?" > "What paper describes Z?"
- Include at least one question that requires the user to reason about a scenario not directly covered in the reading

### After Passing the Gate

- The agent writes the implementation following the plan in `docs/plans/`
- Explain the "why" at key decision points as you code
- The user reviews, asks questions, and learns from the implementation
- When reviewing: focus on whether the invariants from the spec hold, not style nitpicks

## 5-Phase Structure

| Phase | Theme | Key concepts | End state |
|---|---|---|---|
| **P1** | Data Engine + Single Node | Skip list, timer wheel, W-TinyLFU, RESP, Lua, SCAN + custom hash table | `redis-cli` works against single node |
| **P2** | Distribution | Consistent hashing, SWIM gossip, DVVs, quorum R/W | 3-node cluster, minority-failure tolerant |
| **P3** | Fault Tolerance | Sloppy quorum, hinted handoff, read repair, Merkle anti-entropy, conflict merge | Partition → heal → converge |
| **P4** | Persistence + Snapshots | RDB serialization, WAL (write-ahead log), Chandy-Lamport algorithm | Warm restart + WAL durability + distributed snapshots |
| **P5** | CP Subsystem (Raft via MicroRaft) | Linearizability, Raft state machines, fencing tokens, session lifecycle, TTL-in-Raft | Distributed locks, atomic counters, semaphores, latches, CAS on `cp:*` keys |

Each phase has sub-phases (1A, 1B, ...) that teach one concept each. The rhythm is: **read paper → write tests → build → verify key insight**.

## Current Progress

- [x] Project scaffolded — 3 Maven modules, builds clean
- [ ] **P1A:** Command model + basic GET/SET — NOT STARTED
- [ ] P1B: String completion + Hash + List + SCAN (custom hash table + reverse binary iteration)
- [ ] P1C: Skip list + Sorted Set
- [ ] P1D: Hierarchical timer wheel + TTL
- [ ] P1E: Eviction — LRU + W-TinyLFU
- [ ] P1F: RESP server + MULTI/EXEC + Lua
- [ ] P2A–P2D: Distribution (hashing, gossip, DVVs, replication)
- [ ] P3A–P3D: Fault tolerance (handoff, repair, anti-entropy, convergence)
- [ ] P4A: RDB snapshots
- [ ] P4B: Write-Ahead Log (WAL) — fsync policies, group commit, checkpoint, crash recovery
- [ ] P4C: Chandy-Lamport distributed snapshots
- [ ] P5A: MicroRaft integration + AtomicLong state machine
- [ ] P5B: FencedLock + fencing tokens + lease TTL
- [ ] P5C: Sessions + session-tied resource release
- [ ] P5D: Semaphore + CountDownLatch + AtomicReference
- [ ] P5E: Command dispatcher + Redis-compat routing (`cp:*` namespace)
- [ ] P5F: Chaos tests + invariant verification (I13–I22)

**Update this checklist as phases complete.**

## Tech Stack

- **Language:** Kotlin 2.2.x, JDK 21
- **Build:** Maven 3.9.8 — always use `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn`
- **Test:** JUnit 5 + AssertJ
- **Server:** Netty 4.1.x (RESP protocol)
- **Cluster:** gRPC-Kotlin + Protobuf (inter-node)
- **Scripting:** LuaJ 3.0.x (embedded Lua)
- **Consensus (CP subsystem, P5):** MicroRaft (embedded Raft library, Java)
- **No framework** — pure Kotlin + coroutines, no Quarkus/Spring/Ktor

## Module Boundaries (compile-time enforced)

```
dynacache-engine   → kotlin-stdlib ONLY (pure, no I/O)
dynacache-cluster  → engine + coroutines + gRPC
dynacache-cp       → cluster + MicroRaft                     (added in P5)
dynacache-server   → cp + Netty + LuaJ
```

If the engine module imports Netty or gRPC, the build should fail. This is intentional.

## Key Design Decisions (locked)

- **AP core + CP subsystem** — Primary engine is Dynamo-style AP (no consensus; conflicts detected by DVVs, resolved by merge rules). A Raft-backed CP subsystem (MicroRaft) handles linearizable primitives on `cp:*` keys for safe distributed locks, atomic counters, etc. The two engines share nothing except the RESP dispatcher and gRPC transport. See `docs/design-spec-cp.md`.
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
| Redis `dict.c` — `dictScan()` + incremental rehashing | P1B (SCAN) |
| DVVs (Preguica et al., 2012) | P2C |
| ARIES (Mohan et al., 1992) — sections 1-6 | P4B (WAL) |
| Chandy-Lamport (1985) | P4C |
| Raft (Ongaro & Ousterhout, 2014) | P5A–P5B |
| Kleppmann — "How to do distributed locking" (2016) | P5B (fencing tokens) |
| ZooKeeper (Hunt et al., 2010) | P5C (session model) |
| DDIA Ch. 5-6, 9 (Kleppmann) | Background (Ch. 9 for P5) |

## Build & Test Commands

```bash
# Build
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn package

# Test all
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test

# Test single module
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl dynacache-engine
```
