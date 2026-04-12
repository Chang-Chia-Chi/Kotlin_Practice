# DynaCache — Learning Guide

This is a learning project. The goal is not "get it done" but "deeply understand each concept by building it." Each phase has a **read → think → build → verify** rhythm.

## How to Use This Guide

1. **Read the papers first.** Each phase lists specific papers. Read them *before* writing code. Take notes. The papers are short and the concepts directly map to what you'll build.
2. **Write tests before implementation.** For every concept, write the test that asserts the property you just learned from the paper. Then make it pass. The test is your proof of understanding.
3. **Build incrementally.** Each sub-phase produces something runnable. Don't move on until you can demo it.
4. **Ask me for help on specific concepts.** When you're stuck on "how does X work in practice" or "how should I model Y in Kotlin", bring the question. I'll explain and we'll work through it together.
5. **Don't copy the plan's code.** The implementation plan in the WorkFlow repo has code snippets — those are reference starting points, not answers. Your implementation should reflect your understanding.

---

## Phase 1 — Data Engine + Single Node

**End state:** `redis-cli` works against your single-node cache. All data structures, TTL, eviction, transactions, Lua scripting.

### 1A: Project scaffold + Command model

**Concept:** Module boundaries as compile-time constraints.

The `dynacache-engine` module has **zero runtime deps** beyond kotlin-stdlib. This isn't just organization — it's a compile-time guarantee that the engine is pure. If you accidentally import Netty or gRPC in the engine, the build fails.

**Build:** Define the `Command` sealed hierarchy and `Response` types. Write `DataEngine.execute(cmd): Response` with just PING + SET + GET. Get the first test passing.

### 1B: String + Hash + List

**Read:**
- Redis commands documentation for the exact semantics of each command (edge cases matter — what does LRANGE return for out-of-bounds? what does INCR do on a missing key?)

**Concept:** Type-checking discipline. Redis returns WRONGTYPE when you run a list command on a string key. This must be enforced at the engine level.

**Build:** Implement one data type at a time. For each:
1. Write the tests that assert the spec's §6.1 invariants
2. Implement the commands
3. Verify WRONGTYPE is returned for cross-type operations

### 1C: Skip List + Sorted Set

**Read:**
- **Skip Lists: A Probabilistic Alternative to Balanced Trees** (Pugh, 1990) — 8 pages
- Redis source: `t_zset.c` — focus on the `zslInsert`, `zslDelete`, `zslGetRank` functions

**Concept:** A skip list is a probabilistic balanced structure. Each node has a random "height" — on average, half of nodes are height 1, a quarter are height 2, etc. This gives O(log n) search by allowing you to skip ahead at higher levels. The *span* field at each level tracks how many elements are skipped, enabling O(log n) rank queries.

The sorted set uses a **dual index**: skip list for ordering + HashMap for O(1) score lookup by member. This is exactly what Redis does. Neither index alone is sufficient — the skip list can't do O(1) score-by-member, the HashMap can't do range queries.

**Build:**
1. Build the `SkipList` first as a standalone data structure with its own tests
2. Test the O(log n) property — insert 100k elements, verify average comparisons per search is ≤ 2 * log2(N)
3. Then wrap it in `SortedSetValue` (skip list + HashMap)
4. Wire into the engine's Z* commands

**Key insight to verify:** After building the skip list, insert 100,000 random elements and traverse forward. If it's sorted, you understand the insert algorithm. If rank queries return the right position, you understand span tracking.

### 1D: Hierarchical Timer Wheel

**Read:**
- **Hashed and Hierarchical Timing Wheels** (Varghese & Lauck, 1987) — 14 pages
- Netty source: `HashedWheelTimer.java` — ~600 LOC
- Kafka source: `TimingWheel.scala` — ~200 LOC

**Concept:** A timer wheel is like a clock. Level 0 is the seconds hand (fine resolution). Level 1 is the minutes hand (coarser). Level 2 is the hours hand. When the seconds hand completes a revolution, the minutes hand advances and "cascades" entries down to the seconds level.

Key properties:
- **O(1) insert** — compute which slot, put it there
- **O(1) cancel** — maintain an index from key to (level, slot), remove directly
- **O(1) per-expiration** — each tick processes one slot

This replaces Redis's probabilistic random-sampling approach with something deterministic.

**Build:**
1. Build `TimerWheel` as a standalone data structure
2. Test with a virtual clock (you control `advance(toMs)`, not real time)
3. Verify: no early fire, cancel works, reschedule works, ordering preserved
4. Then integrate into `DataEngine` — SET EX/PX schedules into the wheel, PERSIST cancels

**Key insight to verify:** Insert 1M keys with random TTLs. Advance the wheel. Every key should fire within one tick of its deadline. If this passes, you understand cascading.

### 1E: Eviction — LRU + W-TinyLFU

**Read:**
- **TinyLFU: A Highly Efficient Cache Admission Policy** (Einziger et al., 2017) — ~15 pages
- Caffeine source: `FrequencySketch.java` — ~200 LOC

**Concept:** LRU is simple but bad for scan workloads (one full scan evicts all your hot keys). W-TinyLFU fixes this with an **admission filter**: a new key only enters the main cache if it's more "popular" than the key it would evict. Popularity is estimated by a **Count-Min Sketch** (4 hash functions, 4-bit counters, aging by halving).

Build order:
1. **LRU first** — approximate LRU via sampling (pick 5 random keys, evict the least-recently-used). This is how Redis does it.
2. **FrequencySketch** — the Count-Min Sketch. Test that frequent keys have higher frequency than rare ones. Test that the counter caps at 15. Test that reset halves counters.
3. **W-TinyLFU** — wire the sketch into eviction decisions. Test that hot keys survive eviction while cold keys don't.

**Key insight to verify:** Create a workload where 20% of keys get 80% of accesses. Run it against LRU and W-TinyLFU. W-TinyLFU should have a meaningfully higher hit rate.

### 1F: RESP Protocol + Netty Server + MULTI/EXEC + Lua

**Read:**
- Redis RESP2 protocol spec (redis.io) — ~5 pages
- Redis source: `networking.c` — how Redis reads/writes RESP

**Concept:** RESP is absurdly simple. `*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`. That's `SET foo bar`. The simplicity is the point — you can implement a parser in an hour.

Build order:
1. **RESP codec** — parser + encoder. Test with roundtrip encode/decode. Fuzz with random bytes to verify no crashes.
2. **CommandParser** — convert RESP tokens (`["SET", "foo", "bar"]`) to `Command.Set("foo", "bar".toByteArray())`. A big `when` on the command name.
3. **Netty pipeline** — `ByteBuf → RespDecoder → RespServerHandler → RespEncoder`. This is where bytes become commands become responses become bytes.
4. **Test with `redis-cli`** — this is the "it works" moment. When `redis-cli SET foo bar` returns `OK` and `GET foo` returns `"bar"`, you have a working cache server.
5. **MULTI/EXEC** — per-connection state. Buffer commands, execute atomically. Straightforward.
6. **Lua (LuaJ)** — embed the interpreter, sandbox it (remove os/io/debug), register `redis.call()`. Test that scripts can't access the filesystem.

**Key insight to verify:** Open `redis-cli`. Run `ZADD leaderboard 100 alice 200 bob`. Then `ZRANGE leaderboard 0 -1 WITHSCORES`. If you see the sorted results, Phase 1 is done.

---

## Phase 2 — Distribution: Ring + Gossip + Replication

**End state:** 3-node cluster. SET on one node, GET on another. Kill a minority node, still works.

### 2A: Consistent Hashing

**Read:**
- **Consistent Hashing and Random Trees** (Karger et al., 1997) — 12 pages
- Dynamo paper §4.1–4.3

**Concept:** Hash the key, hash each node's vnodes, walk clockwise on the ring to find the owner. Virtual nodes solve load balancing — without them, adding a node steals ~1/2 of one neighbor's keys. With 128 vnodes per node, the load is uniform ± a few percent.

**Build:** `HashRing`. Test determinism (two rings with same nodes → same owners). Test distribution evenness (30k keys across 3 nodes → each gets ~10k ± 20%). Test that adding a 4th node only remaps ~25% of keys.

**Key insight to verify:** Your preference list (the N distinct physical nodes clockwise from the key's hash) must be deterministic. Every node in the cluster, computing independently, must produce the same list.

### 2B: SWIM Gossip

**Read:**
- **SWIM paper** (Das et al., 2002) — 12 pages

**Concept:** Each gossip round: ping one random peer. If no ack → ask K others to relay (ping-req). If still no ack → suspect. After T rounds as suspect → dead. Membership propagates by piggybacking on every message.

**Build:** You'll need an `InProcessCluster` test harness — N gossip instances communicating via in-memory message passing. This is crucial: you can't test gossip convergence with real network timing.

**Key insight to verify:** Kill a node. Count how many gossip rounds until all survivors agree it's dead. Should be O(log N) — for 5 nodes, under 15 rounds.

### 2C: Dotted Version Vectors

**Read:**
- **Dotted Version Vectors** (Preguica et al., 2012) — ~20 pages
- Riak source: `dvvset.erl` — ~300 LOC

**Concept:** Each write gets a `dot = (node_id, counter)`. The `causal_context` tracks what the writer has seen. Two DVVs can be compared: A dominates B (A has seen everything B has seen), or they're concurrent. The key property: DVV size is bounded by cluster size (3–7 nodes), not client count.

**Build:** `Dvv` and `DvvClock`. Test dominance, concurrent detection, merge, bounded size.

**Key insight to verify:** Run 10,000 writes from 100 different clients through 3 nodes. The DVV size should never exceed 3 entries.

### 2D: Replication + Quorum

**Read:**
- Dynamo paper §4.4–4.5

**Concept:** Write to N replicas, return after W acks. Read from R replicas, return the newest by DVV. R + W > N means at least one node in every read set saw the latest write. This isn't consensus — it's just counting.

**Build:** Wire the replication manager. Test: write with W=2, read with R=2, value is there. Kill 1 of 3 nodes — still works. Kill 2 of 3 — writes fail. This is the "quorum arithmetic works" moment.

---

## Phase 3 — Fault Tolerance: Handoff + Repair + Convergence

**End state:** Partition the cluster, write to both sides, heal, replicas converge.

### 3A: Sloppy Quorum + Hinted Handoff

**Read:**
- Dynamo paper §4.6

**Concept:** Strict quorum fails when a preference-list node is down. Sloppy quorum sends the write to the *next* healthy node as a "hint." The hint is stored temporarily and replayed when the target recovers. The hint must be a complete copy — key, value, DVV, TTL — an exact replay.

**Build:** `HintStore` + modify the write path. Test: partition a node, write, heal, hints drain, partitioned node has the data.

### 3B: Read Repair

**Read:**
- Dynamo paper §4.7 (first half)

**Concept:** On every read, if the R responses disagree (different DVVs), push the newest version to the stale replicas asynchronously. This is "free" — you're already reading from R nodes.

**Build:** Modify the read path. Force one replica stale, read, verify the stale replica gets updated.

### 3C: Merkle Tree Anti-Entropy

**Read:**
- Dynamo paper §4.7 (second half)
- Any Merkle tree summary

**Concept:** Read repair only fixes keys that are *read*. Keys that sit unread need a background process. Merkle trees compare hashes of key ranges — if roots match, the ranges are identical. If not, walk down the tree to find the divergent keys. This turns an O(n) comparison into O(log n) + O(divergent keys).

**Build:** `MerkleTree.build()`, `MerkleTree.diff()`, then `AntiEntropySync` that periodically compares Merkle trees between replica pairs and syncs divergent keys.

### 3D: Conflict Resolution + Convergence

**Read:**
- Shapiro et al., 2011 (CRDT survey) — skim §2-3 for merge properties

**Concept:** Concurrent writes (detected by DVVs) need merge rules. The rules must be commutative, associative, and idempotent — so replicas converge regardless of merge order. String uses LWW. Hash merges per-field. Sorted Set takes the union of elements with max score on conflict.

**Build:** `ConflictResolver`. Then the full convergence test: partition → concurrent writes on both sides → heal → anti-entropy → all replicas agree.

**This is the "Dynamo works" moment.** When this test passes, you've built the core of a Dynamo-style system.

---

## Phase 4 — Persistence + Snapshots

**End state:** Kill all nodes, restart from RDB, data intact. Chandy-Lamport snapshot during traffic, restore later.

### 4A: RDB Serialization

**Read:**
- Redis source: `rdb.c` — `rdbSave` / `rdbLoad`

**Concept:** Serialize every data type (String, Hash, List, ZSet) + metadata (DVV, TTL) to a binary format with CRC integrity checking. The hard part: snapshotting without stopping the world (capture a point-in-time copy while commands continue).

**Build:** `RdbSerializer` (pure, in engine module). Test roundtrip for every type. Test CRC detects corruption. Then `SnapshotEngine` for save/restore.

### 4B: Chandy-Lamport Distributed Snapshots

**Read:**
- **Chandy-Lamport** (1985) — 10 pages, the full paper
- **Mattern** (1989) — formalizes consistent cuts

**Concept:** Send a **marker** on all outgoing channels. When a node receives a marker, it records its local state and starts recording in-flight messages on all other channels. When markers have been received on all channels, the snapshot is complete. The result is a **consistent cut**: if event A causally precedes B and B is in the snapshot, A is too.

**Prerequisite:** FIFO channel ordering. gRPC over a single connection gives you this.

**Build:** `ChandyLamport` coordinator + participant. Test the consistent cut property. Test timeout/abort on node failure.

**This is the hardest concept in the project.** Take time with the paper. Draw the spacetime diagrams. The algorithm is elegant but the "why does this guarantee a consistent cut" reasoning is subtle.

### 4C: Integration + Demo

Wire everything. Run the success signal demo from the spec. When `redis-cli` works end-to-end against a 3-node cluster that survives partitions, heals, and snapshots — you're done.

---

## Working With Me

When you're ready to start a phase:
1. Tell me which sub-phase you're starting
2. I'll review your approach before you write code
3. When you get stuck, show me your test + where you're blocked
4. After each sub-phase, I'll review what you built

I'm your pair programmer, not your implementer. You write the code, I help you understand the concepts and catch design mistakes.
