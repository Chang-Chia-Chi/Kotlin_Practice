# DynaCache

Dynamo-style AP distributed cache with Redis-compatible data structures and RESP2 wire protocol.

Built from scratch in Kotlin as a distributed systems learning project.

## What This Is

A distributed cache that implements:
- **Redis data structures** — String, Hash, List, Sorted Set (skip list)
- **RESP2 protocol** — `redis-cli` and Jedis/Lettuce clients work out of the box
- **Dynamo distribution** — consistent hashing, SWIM gossip, tunable quorum (R/W/N)
- **AP fault tolerance** — DVVs, sloppy quorum, hinted handoff, read repair, Merkle anti-entropy
- **Persistence** — RDB snapshots + Chandy-Lamport distributed snapshots
- **Cache semantics** — hierarchical timer wheel TTL, W-TinyLFU eviction, Lua scripting

## Project Structure

```
DynaCache/
├── dynacache-engine/    # Pure data structures + command engine (no I/O deps)
├── dynacache-cluster/   # Distribution: hashing, gossip, replication, DVVs
├── dynacache-server/    # Netty RESP server + gRPC cluster transport
└── docs/
    ├── design-spec.md   # Constraints, invariants, semantics
    └── LEARNING-GUIDE.md # Phase-by-phase learning path
```

## Build

```bash
mvn package
```

## Papers

| Paper | Teaches |
|---|---|
| Dynamo (DeCandia et al., 2007) | The architectural blueprint |
| SWIM (Das et al., 2002) | Gossip membership |
| Skip Lists (Pugh, 1990) | Sorted Set internals |
| Timer Wheels (Varghese & Lauck, 1987) | TTL expiration |
| TinyLFU (Einziger et al., 2017) | Eviction policy |
| DVVs (Preguica et al., 2012) | Causal tracking |
| Chandy-Lamport (1985) | Distributed snapshots |
