# DynaCache - Implementation Plan

Version: v1.0 (2026-09-06; replaces the 2026-04 superpowers-style plans)
Companion to: `docs/dynamiccache/design-spec.md` (AP engine, C1 to C15, I1 to I12) and
`docs/dynamiccache/design-spec-cp.md` (CP subsystem, C16 to C23, I13 to I22)
Purpose: break DynaCache into tickets small enough for one fresh subagent each, with fixed
contracts and fixed assertions, so implementations may vary but assertions may not.

Tickets are `T01` to `T84`: `T01` to `T46` grouped into phases `P1` to `P5`; `T47` a
measurement addendum to P1; `T48` to `T76` and `T80` to `T84` the review fixes and deepening of
`P6`, where 74 to 76 and 80 to 84 are follow-ups the fix loop itself found; and `T77` to `T79`
the performance work of `P7`, from T47's benchmark (one file per phase under `plans/`). Ticket files live in `.scratch/dynacache/issues/NN-<slug>.md`. The progress log is
`docs/dynamiccache/progress.md`. This directory is the authority for spec, plan and progress;
`DynaCache/CONTEXT.md` is the glossary and `DynaCache/docs/adr/` holds the architecture
decisions. `DynaCache/` is an ordinary directory of this repository (folded in on 2026-09-06
from the `dynacache` branch, which remains as a backup).

---

## 1. Ground Rules for Agent-Driven Implementation

1. **Fixed vs free.** The module graph (2.2), the seams (2.3), the constraints C1 to C23, the
   invariants I1 to I22, every named test in spec 6 and CP spec 10, and the semantics of spec 5
   and CP spec 9 are FIXED. Class names, internal structure, and algorithms beyond what the spec
   names (skip list, timer wheel, DVV, Merkle, Chandy-Lamport, Raft via MicroRaft) are free. An
   agent that believes a fixed contract is wrong stops and reports; it does not adapt.
2. **One ticket, one change,** 200 to 600 lines including tests. Past that, stop and split.
3. **Tests are deliverables.** Spec-named tests keep the spec's name verbatim
   (`string_set_get_roundtrip`). Constraint tests are `C<n>_<description>`, invariant tests
   `I<n>_<description>`. A ticket is done when its listed tests exist and pass.
4. **Test tooling is JUnit 5 + Mockito only.** No AssertJ, no MockK, no Kotest, no Hamcrest.
   `org.junit.jupiter.api.Assertions.*` and `org.mockito.Mockito.*`. The April poms carry
   AssertJ; T01 removes it. Mocks only at true system boundaries (sockets, clock, randomness,
   filesystem); never an internal collaborator.
5. **No sleeps, no wall clock.** Time is an injected `java.time.Clock`; the timer wheel is
   advanced by an explicit call; coroutine tests run under `runTest`; the in-memory transport of
   T18 is deterministic. A spec test phrased "wait > 1s" advances the injected clock.
6. **No scope creep.** A stub throwing `NotImplementedError` is the placeholder for a later seam.
   The do-not-build list (2.4) is binding.
7. **Documents win** unless `progress.md` records a deliberate deviation. Every ticket appends
   an entry there.
8. **Boundary.** A ticket modifies only `DynaCache/` and, when a measurement forces it,
   `docs/dynamiccache/`. Nothing else in the repository.
9. **Skills.** Every subagent runs the Matt Pocock `implement` shape: `tdd` at the pre-agreed
   seams (vertical slices, red before green, no horizontal batches of tests), `codebase-design`
   vocabulary for any new interface (deep modules, a seam only where two adapters exist), and a
   `code-review` self-pass before the commit. The superpowers plugin is not used.

---

## 2. Architecture Overview

### 2.1 Design stance

Pure Kotlin, no framework. Four Maven modules whose dependencies are the boundary; Maven
enforces them, so no ArchUnit. The engine is a library with no I/O. Every technology (Netty,
gRPC, LuaJ, MicroRaft, the filesystem) sits in the module that owns it and reaches the engine
only through the seams of 2.3. A seam exists only where a second adapter is real, and every
seam has one in the test kit.

### 2.2 Module graph (Maven-enforced)

```
dynacache-engine   kotlin-stdlib only (no coroutines, no I/O; JDK executors are allowed)
                   packages: dynacache.engine (Command incl. the Cp sub-hierarchy, Reply, Key, CommandEngine,
                             partition executors and stores),
                             dynacache.engine.ds (hash table, skip list, timer wheel, sketch),
                             dynacache.engine.persist (RDB codec, WAL, snapshot engine)
dynacache-cluster  engine + kotlinx-coroutines + grpc-kotlin + protobuf
                   packages: dynacache.cluster (Ring, Transport, Membership/SWIM, DVV, replication,
                             hints, Merkle, anti-entropy, merge rules, Chandy-Lamport; protobuf
                             messages are the cluster's message model, ADR 0001 and CONTEXT.md)
dynacache-cp       cluster + MicroRaft                                (created in T38)
                   packages: dynacache.cp (Raft runtime, state machines, sessions, log time)
dynacache-server   cp (cluster until T38) + Netty + LuaJ
                   packages: dynacache.server (RESP codec, command parser, Netty pipeline, MULTI,
                             Lua bridge, gRPC adapters, dispatcher, main)
```

Rules: the engine never imports coroutines, Netty, gRPC, LuaJ or MicroRaft. LuaJ appears only
in `dynacache.server`. Generated protobuf and gRPC classes appear in `dynacache.cluster`,
`dynacache.cp` and the server's adapters, never in the engine. `java.nio.file` appears only in
`dynacache.engine.persist` and `dynacache.cp`. Vocabulary is `DynaCache/CONTEXT.md`;
architecture decisions are `DynaCache/docs/adr/`.

### 2.3 Seams (public surface budget)

Six seams, each with two real adapters (grilled 2026-09-06; ADR 0001, ADR 0002; T73):

| Seam | Module | Interface (everything a caller must know) | Adapters |
|---|---|---|---|
| `CommandEngine` | engine | `submit(Command): CompletableFuture<Reply>` runs the command on its key's partition executor, fanning a multi-key command out and joining in argument order (non-atomic across partitions, ADR 0002); `close()` shuts the partition executors down. One command at a time per partition (C1) holds by construction. The CP engine presents the same shape. A batch is not part of it: it is the `BatchEngine` capability below, so no adapter implements a batch it cannot run (T73). | AP engine; CP engine (T38); the test kit's recording fake (T18) |
| `BatchEngine` | engine | `atomically(keys) { ctx -> R }` runs a batch on the declared keys' partition with nothing interleaved, rejects keys on different partitions before running (C12), and answers an undeclared key inside the block with an error reply. Only the connection handler asks for it, for `MULTI`/`EXEC` and `EVAL`; the CP engine offers none, because its replicated log already serializes every entry. | AP engine, which runs a batch; `ClusterNode`, which refuses a batch whose keys this node does not coordinate and otherwise runs it on its own AP engine (T19 deviation 4, unreplicated per T22 deviation 5) |
| `Clock` and wheel tick | engine | `java.time.Clock` injected; the timer wheel exposes `advanceTo(instant)`; the server owns the scheduler that calls it. | fixed clock in tests |
| `Transport` | cluster | `send(to: NodeId, message)`, `inbound: Flow<message>`; the messages are the generated protobuf types, so no codec exists between the in-memory and the gRPC adapter. | `InMemoryTransport` (test kit, T18) with network partition, heal, drop, delay, kill; `GrpcTransport` (T23) |
| `Membership` | cluster | The gossip's current view: alive, suspect, dead; change events. | SWIM (T20); a scripted fake in the test kit |
| MicroRaft `Transport`, `StateMachine`, `RaftStore` | cp | MicroRaft's own interfaces. | in-memory (tests), gRPC-backed (T43), file store (T45) |

Frozen types, not seams: `Command` (sealed, with a `Cp` sub-hierarchy for the CP verbs and
compat commands) and `Reply` (exactly RESP2: `Simple`, `Error(kind, message)`, `Integer`,
`Bulk(bytes?)`, `Array`), frozen in T01; `Command` variants are added per ticket.
`CommandDispatcher` is a concrete class that only routes (CP spec 9.5) and knows nothing of
batches; the `cp:` refusal a batch needs (C16) is the connection handler's, beside the two
callers that ask for one. `Ring`, `Dvv`, `MerkleTree`, `HintStore` and the data structures are
concrete and tested through `CommandEngine` or their own public methods.

A **partition** is a fixed-count local hash bucket inside the engine, each with one JDK
single-thread executor and its own store; the ring's **vnodes** decide placement across nodes
and never the executor (two layers, CONTEXT.md). A batch needs its keys on the same
coordinator and the same partition; hash tags (`{tag}`) give both. That is the only
concurrency mechanism inside the engine: no locks inside data structures.

### 2.4 Do-not-build list

Redis Cluster protocol (`-MOVED`, `-ASK`); RESP3; Sets, Streams, HyperLogLog, Bitmap,
Geospatial, pub/sub; auth, ACL, TLS; dynamic membership or rebalancing; WAL rewrite or
compaction beyond checkpoint truncation; multiple Raft groups; blocking CP verbs; Raft from
scratch; metrics or Micrometer (`INFO` is the observability); a client library; benchmarks;
Testcontainers, Toxiproxy, ArchUnit, AssertJ, MockK.

### 2.5 Concurrency rule

Inside the engine: one JDK single-thread executor per partition, no shared mutable state
across partitions, no locks in data structures; callers await the returned future (the cluster
module bridges it to coroutines). In the cluster module: coroutines with structured concurrency,
every fan-out bounded (a replication request awaits at most N replies with a deadline), and
every background process (gossip period, anti-entropy, hint replay, TTL tick) is a single
coroutine started and cancelled by the node's lifecycle. Tests drive these processes by
calling their step function directly; nothing polls real time.

---

## 3. Phase Plan

```
P1 single node (engine + server)

T01 --+--> T02 --+--> T03 --> T05 (hash table + SCAN) --+
      |          +--> T04 ------------------------------+
      +--> T06 (skip list) --> T07 (zset, after T05) ---+--> T10 (LRU) --> T11 (TinyLFU) --+
      +--> T08 (timer wheel) --> T09 (TTL cmds) --------+                                  |
      +--> T12 (RESP) --> T13 (Netty) --> T14 (MULTI) --> T15 (Lua) -----------------------+--> T16 (P1 accept)

P2 distribution (cluster)

T01 --> T17 (ring) --+--> T18 (transport seam + in-process cluster) --+--> T19 (router, after T13)
                     |                                                 +--> T20 (SWIM)
                     +--> T21 (DVV)                                    +--> T23 (gRPC adapter)
T19, T20, T21 --> T22 (replication + quorum) --+
T22, T23 --------------------------------------+--> T24 (P2 accept, after T16)

P3 fault tolerance (cluster)

T22 --+--> T25 (hints + sloppy quorum) --+
      +--> T26 (read repair) ------------+
T21 --> T27 (Merkle) --> T28 (anti-entropy, after T22) --+--> T30 (convergence, after T24)
T21, T07 --> T29 (merge rules) ---------------------------+

P4 persistence

T07, T09 --> T31 (RDB codec) --> T32 (snapshot engine, after T14) --+
T01 --> T33 (WAL rw) --> T34 (fsync + group commit) --> T35 (WAL in the write path, after T32)
T32, T22 --> T36 (Chandy-Lamport)
T30, T35, T36 --> T37 (P4 accept)

P5 CP subsystem

T18 --> T38 (cp module + MicroRaft + AtomicLong) --> T39 (log time) --> T40 (FencedLock) --> T41 (sessions)
T41 --> T42 (semaphore, latch, reference)
T38, T23 --> T43 (gRPC CpService + RaftService + forwarding)
T42, T43, T13 --> T44 (dispatcher + compat routing + RESP)
T41, T42, T44 --> T45 (Raft snapshot, chaos, linearizability)
T45, T37 --> T46 (P5 accept)
```

Waves an orchestrator can run in parallel worktrees: after T01 {T02, T06, T08, T12, T17, T33};
after T02 {T03, T04}; after T18 {T20, T23, T38}; after T22 {T25, T26, T28}; the CP chain runs
beside P3 and P4.

---

## 4. Model Routing

| Tier | Model | Tickets | Why |
|---|---|---|---|
| 1 | Claude Fable 5.1 | T02, T05, T08, T18, T20, T21, T22, T25, T26, T28, T29, T30, T32, T34, T35, T36, T39, T40, T41, T45, T49, T50, T51, T65, T66, T67 | an interleaving, a causal order, a consistent cut or a monotonic counter is the deliverable |
| 2 | Claude Opus 5 | T01, T03, T04, T06, T07, T09, T10, T11, T12, T13, T14, T15, T16, T17, T19, T23, T24, T27, T31, T33, T37, T38, T42, T43, T44, T46, T47 (measurement), T48, T52 to T64, T68 to T73 | commands, codecs, data-structure craft with a sequential spec, adapters, wiring, acceptance |

Escalation: an Opus ticket that fails compile or tests on its second attempt, or that tries to
change a seam of 2.3, is terminated and relaunched fresh on Fable with the error context. A
Fable subagent that dies (HTTP 429) is relaunched fresh, never resumed.

---

## 5. Traceability

| Constraint | Ticket |
|---|---|
| C1 | T02 |
| C2 | T21 |
| C3 | T17 |
| C4 | T22 |
| C5 | T25 |
| C6 | T27 |
| C7 | T08, T09 |
| C8 | T12, T13, T16 |
| C9 | T32 |
| C10 | T36 |
| C11 | T15 |
| C12 | T14, T15 |
| C13 | T04 |
| C14 | T35 |
| C15 | T05 |
| C16, C22 | T44 |
| C17 | T40 |
| C18 | T41 |
| C19, C23 | T39 |
| C20 | T45 |
| C21 | T38 |

| Invariant | Ticket |
|---|---|
| I1, I2 | T30 |
| I3 | T06, T07 |
| I4 | T21 |
| I5 | T17 |
| I6 | T10 |
| I7 | T08 |
| I8 | T20 |
| I9 | T25 |
| I10 | T15 |
| I11 | T14 |
| I12 | T36 |
| I13, I14 | T40, T45 |
| I15 | T41, T45 |
| I16, I17 | T38, T45 |
| I18, I19 | T40 |
| I20 | T45 |
| I21 | T42 |
| I22 | T44 |

| Spec test group | Ticket |
|---|---|
| 6.1 String | T02, T03 |
| 6.1 Hash | T03 |
| 6.1 List, WRONGTYPE | T04 |
| 6.1 ZSet | T07 |
| 6.1b SCAN, hash table | T05 |
| 6.2 skip list | T06 |
| 6.3 timer wheel | T08 |
| 6.4 eviction | T10, T11 |
| 6.5 DVV | T21 |
| 6.6 RESP | T12 |
| 6.7 ring_determinism | T17 |
| 6.7 gossip_* | T20 |
| 6.7 write_read_quorum, minority_*, majority_* | T22 |
| 6.7 hinted_handoff_replays | T25 |
| 6.7 read_repair_fixes_stale | T26 |
| 6.7 anti_entropy_heals_divergence | T28 |
| 6.7 convergence_after_partition | T30 |
| 6.8 rdb_* | T31, T32 |
| 6.8b wal_* | T33, T34, T35 |
| 6.8 chandy_lamport_* | T36 |
| 6.9 multi_*, discard_* | T14 |
| 6.9 lua_* | T15 |
| CP 10.2 | T38 |
| CP 10.1 | T40 |
| CP 10.6 | T41 |
| CP 10.3, 10.4, 10.5 | T42 |
| CP 10.7 | T38, T43, T45 |
| CP 10.8 | T44 |
| CP 10.9 | T45 |

---

## 6. Orchestrator Protocol (the system prompt, condensed)

The orchestrator writes no production or test code. Per ticket:

1. Read the ticket, its phase entry, and the spec sections it names. Confirm the model tier
   from section 4. Declare the pre-agreed seams (2.3) the ticket may touch.
2. Spawn a fresh subagent (never resume one) with: the ticket file verbatim, the phase entry,
   plan sections 1, 2 and 6, the spec sections named, the model tier, and the Maven command
   from `DynaCache/CLAUDE.md`. Mandate: Matt Pocock `tdd` at the declared seams, red-green
   vertical slices, `codebase-design` vocabulary, JUnit 5 + Mockito only, no sleeps, the size
   budget, stop-and-report on any frozen contract that does not survive contact.
3. Parallel tickets run in git worktrees; every brief begins with a reset to the branch head,
   forbids `git stash`, and refuses to run git outside its worktree.
4. Verify: run the module's suite yourself, count passing tests, check the named tests exist,
   check no banned dependency entered a pom.
5. Commit `feat(dynacache): T<nn> <title> (passes C<n>, I<n>)`, merge the worktree, tick the
   ticket's status, and advance to the frontier of the DAG.

### Per-ticket agent briefing

1. The spec sections it implements and the phase entry.
2. The seams it may touch and the ones it must not.
3. The fixed assertions: named tests, constraint and invariant ids.
4. The do-not-build list (2.4) and the concurrency rule (2.5).
5. The size budget and the instruction to stop and report, updating `progress.md` first, if the
   budget or a frozen contract does not survive contact with reality.
