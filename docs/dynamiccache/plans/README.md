# DynaCache Plan Files - Index

Read `../plan.md` first: ground rules, module graph, seams, the ticket DAG, model routing,
traceability and the orchestrator protocol. Each file below holds one phase's ticket entries.
Ticket files live in `.scratch/dynacache/issues/`; the progress log is `../progress.md`.

| Phase | File | Tickets | What works when done |
|---|---|---|---|
| P1 | [p1-data-engine.md](p1-data-engine.md) | T01 to T16 | `redis-cli` and Jedis against one node: every command, TTL, SCAN, MULTI, EVAL |
| P2 | [p2-distribution.md](p2-distribution.md) | T17 to T24 | three nodes, quorum R/W, gossip, DVVs, minority failure tolerated |
| P3 | [p3-fault-tolerance.md](p3-fault-tolerance.md) | T25 to T30 | partition, write both sides, heal, converge |
| P4 | [p4-persistence.md](p4-persistence.md) | T31 to T37 | RDB plus WAL warm restart, Chandy-Lamport snapshot and restore |
| P5 | [p5-cp-subsystem.md](p5-cp-subsystem.md) | T38 to T46 | Raft-backed locks, counters, semaphores, latches, CAS on `cp:*` |
| P6 | [p6-review-fixes.md](p6-review-fixes.md) | T48 to T76, T80 to T84 | review bugs closed at root cause, codec, versioned store, one inbound loop; 74 to 76 and 80 to 84 are what the fix loop itself found |
| P7 | [p7-performance.md](p7-performance.md) | T77 to T79, T85 to T87 | the three benchmark anomalies fixed and re-measured, plus a shutdown drain, a timer fix and a clean baseline |

The April 2026 plans (superpowers-style, with code listings and AssertJ) are gone; the spec
sections and named tests they cited are unchanged and are the fixed assertions here.
