# DynaCache

Two engines behind one Redis wire. An AP cache in the shape of Dynamo — consistent hashing,
gossip, tunable quorum, eventual convergence — and beside it a CP subsystem on Raft for the
handful of things a cache cannot get wrong: a counter, a fenced lock, a semaphore, a latch, a
reference. A client picks between them by naming a key: `foo` is the AP engine's, `cp:counter:foo`
is the log's. `redis-cli`, Jedis and Lettuce work unmodified against either.

Built from scratch in Kotlin as a distributed systems learning project. No Spring, no framework:
four Maven modules whose dependency edges are the architecture, and Maven enforces them.

## What runs today

**The AP engine** — String, Hash, List and Sorted Set (a skip list), RESP2, `MULTI`/`EXEC`, Lua
via LuaJ, TTLs on a hierarchical timer wheel, W-TinyLFU eviction under a memory threshold, an RDB
codec, a write-ahead log with a choice of fsync policies, and periodic snapshots.

**The AP cluster** — consistent hashing with virtual nodes, SWIM gossip, N/W/R quorum over gRPC,
dotted version vectors for causality, sloppy quorum with hinted handoff, read repair, Merkle
anti-entropy, and Chandy-Lamport distributed snapshots.

**The CP subsystem** — MicroRaft over its own gRPC service, a composite state machine (AtomicLong,
FencedLock, Semaphore, CountDownLatch, AtomicReference), sessions that release what they held,
TTLs evaluated against log time rather than any node's clock, Raft snapshots with a real on-disk
store, and `CP.*` verbs alongside a Redis-compat spelling for the counter and the reference. A
node outside the Raft group forwards to whoever leads and rediscovers on failover.

**The dispatcher** — the one place the two meet. A `cp:` key goes to the log, everything else to
the ring, and no command ever reaches both.

## Running it

A single node, with everything optional defaulted:

```
dynacache [port] [partitions] [dir] [ALWAYS|EVERY_SECOND|NEVER|GROUP_COMMIT] [cp-self] [cp-members]
```

`dir` turns on persistence: the last snapshot and the log after it are restored before the port
opens, and one more snapshot is written at shutdown. `cp-members` is `id@host:port,...` and
`cp-self` says which entry this node is; a node not named in the group forwards its CP work.

```bash
dynacache                                   # port 6379, 16 partitions, no disk, no CP
dynacache 6379 16 /var/lib/dynacache ALWAYS # persistent, fsync on every write
dynacache 6379 16 /var/lib/dynacache GROUP_COMMIT # persistent, one fsync per 2 ms of writers
dynacache 6379 16 /var/lib/dynacache EVERY_SECOND n1 n1@10.0.0.1:9001,n2@10.0.0.2:9001,n3@10.0.0.3:9001
```

A cluster is the same command line plus the flags that describe the ring. Every node gets the
same `--peers`; `--node` says which one it is.

```bash
dynacache 6379 16 /var/lib/dynacache EVERY_SECOND "" n1@10.0.0.1:9001,n2@10.0.0.2:9001,n3@10.0.0.3:9001 \
  --peers=n1=10.0.0.1:7379,n2=10.0.0.2:7379,n3=10.0.0.3:7379 --node=n1 --quorum=3/2/2
```

`--grpc=<port>` overrides the cluster port `--peers` gave this node, and `--quorum=n/w/r` the
replication factor. In cluster mode `--node` already names this node, so the `cp-self` positional
is ignored and the CP group is read from `cp-members` alone. The CP members must be an odd number,
at least three, and fixed at startup: adding or removing one is a planned operation nothing
implements yet.

## Build and test

```bash
mvn package          # offline-capable; JDK 22
```

Four tiers, all in one run, no sleeps anywhere — every wait is a poll to a deadline or an
advance of an injected clock.

| Module | Tests | Roughly |
|---|---|---|
| `dynacache-engine` | 144 | 3 s |
| `dynacache-cluster` | 83 | 5 s |
| `dynacache-cp` | 89 | 30 s |
| `dynacache-server` | 83 | 12 s |

Most of the CP tier is one class: `ChaosInvariantTest` runs five seeds of kills, restarts and
partitions against a live Raft group and checks mutual exclusion, fencing-token monotonicity,
session release and linearizability, in about 20 seconds. The four acceptance classes drive real
sockets with an unmodified Jedis: `P1` a single node, `P2` a three-node cluster, `P4` persistence,
restart, a distributed snapshot and eviction, and `P5` both engines in one three-node cluster
through a leader failover. A whole `mvn clean package` is under two minutes.

## Layout

```
dynacache-engine/    kotlin-stdlib only, no I/O: data structures, command engine, RDB, WAL
dynacache-cluster/   + coroutines and gRPC: ring, SWIM, DVVs, replication, Merkle, snapshots
dynacache-cp/        + MicroRaft: Raft runtime, state machines, sessions, log time
dynacache-server/    + Netty and LuaJ: RESP codec, parser, dispatcher, Lua, main
docs/adr/            the decisions and why
CONTEXT.md           the vocabulary; read this before the code
```

## Known debts

Real ones, each with a repair that is understood and not yet done.

- **A connection's CP session outlives its socket.** Nothing closes it on disconnect, so a lock a
  dead client held waits out the session timeout instead of going immediately. Closing the session
  on `channelInactive` is the repair.
- **An installed value is not written to the WAL.** Anti-entropy and read repair reach the engine
  through a restore-shaped path that bypasses the log, so a node that crashes and replays its own
  log lacks what a peer had just given it, until the next anti-entropy round hands it back. A
  graceful shutdown hides this completely; the repair is an install entry in the WAL codec, or a
  stated decision that a crashed node is repaired by its peers rather than by its log.
- **Nothing deletes, so anti-entropy has no tombstones.** A key one side lost is handed back, and
  a deleted key a lagging replica still holds is resurrected. Dynamo's own gap; the repair is
  tombstone leaves in the Merkle tree with a grace period.
- **The router's inbound loop dies on any exception a handler throws.** One loop demuxes forwards,
  replication, anti-entropy, gossip and snapshots, so a bug in any one of them silently stops all
  of them: the node keeps its socket open and answers nothing that needs a peer. A `try`/`catch`
  per envelope inside `Router.run` is the repair.
- **Anti-entropy runs on a 60-second interval and no test crosses it.** The loop is launched and
  its envelopes reach it, and the exchange itself is covered in the cluster tier, but nothing
  end-to-end waits for a round to fire on its own schedule.

## Papers

| Paper | Teaches |
|---|---|
| Dynamo (DeCandia et al., 2007) | The architectural blueprint |
| SWIM (Das et al., 2002) | Gossip membership |
| Raft (Ongaro & Ousterhout, 2014) | The replicated log under the CP subsystem |
| Skip Lists (Pugh, 1990) | Sorted Set internals |
| Timer Wheels (Varghese & Lauck, 1987) | TTL expiration |
| TinyLFU (Einziger et al., 2017) | Eviction policy |
| DVVs (Preguica et al., 2012) | Causal tracking |
| Chandy-Lamport (1985) | Distributed snapshots |
| Wing & Gong (1993) | The linearizability checker in the CP tier |
