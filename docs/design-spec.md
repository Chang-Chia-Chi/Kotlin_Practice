# DynaCache — Distributed Cache Design Spec

**Date:** 2026-04-12
**Project:** DynaCache (standalone, sibling to WorkFlow)
**Target location:** `~/GitHub/Kotlin_Practice/DynaCache/`
**Status:** Design locked, pending implementation plan

---

## 0. What This Document Is

A constraints document. It defines what properties must hold, what invariants tests assert, and what semantics are guaranteed. It does **not** prescribe interfaces, class names, or project layout — those emerge during implementation planning.

---

## 1. Goals & Non-Goals

### Goal

Build a Dynamo-style AP distributed cache in Kotlin that implements Redis-compatible data structures and wire protocol, providing a learning vehicle for AP distributed systems while remaining usable as a drop-in cache for the WorkFlow engine.

### In Scope

| Feature | Why |
|---|---|
| Redis core data structures (String, Hash, List, Sorted Set) | Cache utility + data structure craft |
| RESP2 wire protocol (client-facing) | Redis client compatibility — `redis-cli`, Jedis, Lettuce just work |
| gRPC cluster protocol (inter-node) | Efficient binary protocol for replication, gossip, anti-entropy |
| Consistent hashing with virtual nodes | Dynamo-style partitioning without a coordinator |
| SWIM gossip membership + failure detection | Decentralized liveness |
| Tunable quorum (R, W, N) | Explicit consistency/availability knob |
| Sloppy quorum + hinted handoff | Availability during partial failure |
| Dotted Version Vectors (DVVs) | Causal tracking without vector clock bloat |
| Read repair + Merkle-tree anti-entropy | Replica convergence |
| Hierarchical timer wheel for TTL expiration | O(1) insert/cancel/fire for key expiry |
| LRU / LFU / W-TinyLFU eviction | Memory-bounded cache behavior |
| MULTI/EXEC transactions | Atomic multi-command batches (single partition) |
| Embedded Lua scripting (LuaJ) | Redis-compatible atomic scripting |
| Local RDB snapshots | Node-level persistence for warm restart |
| Chandy-Lamport distributed snapshots | Cluster-wide consistent state capture |
| Small fixed cluster (3–7 nodes) | Sufficient to exercise all distributed code paths |

### Non-Goals

- Dynamic membership / auto-rebalancing — design for it, don't build it
- TLS, auth, ACLs — production hardening, not learning
- Redis Cluster protocol (`-MOVED` / `-ASK` slot migration) — we use Dynamo partitioning, not hash slots
- Redis Streams, HyperLogLog, Bitmap, Geospatial — future extensions
- Redis pub/sub — different system, different design
- Disk-backed storage (AOF) — this is a cache, not a database
- Jepsen / formal verification
- Performance benchmarking (correctness first)

---

## 2. Core Features

### 2.1 Data Structures

**String** — byte-safe values. Supports atomic increment/decrement on numeric values. `SET` supports combinable flags: `NX` (not exists), `XX` (exists only), `EX` (TTL seconds), `PX` (TTL millis). `SETNX` is `SET NX` alias.

Commands: `GET`, `SET`, `SETNX`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `MGET`, `MSET`, `APPEND`, `STRLEN`.

**Hash** — field-value map under a single key. All field operations are O(1). Entire-hash reads (`HGETALL`) are O(n) in fields.

Commands: `HGET`, `HSET`, `HDEL`, `HGETALL`, `HMGET`, `HMSET`, `HEXISTS`, `HKEYS`, `HVALS`, `HLEN`.

**List** — ordered sequence, push/pop from both ends. `LRANGE` is O(n) in range size. Internal representation must support O(1) push/pop at both ends.

Commands: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LREM`.

**Sorted Set** — elements scored by a float64, unique by member name. Backed by a skip list for O(log n) insertion, deletion, and range queries, plus a hash map for O(1) score lookup by member. This dual-index structure is the defining characteristic.

Commands: `ZADD`, `ZREM`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZRANK`, `ZREVRANK`, `ZSCORE`, `ZCARD`, `ZINCRBY`.

**Key Expiry** — every key of any type can carry a TTL.

Commands: `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `TTL`, `PTTL`, `PERSIST`.

**Server** — introspection and management.

Commands: `PING`, `INFO`, `DBSIZE`, `FLUSHDB`, `COMMAND`, `DEL`, `EXISTS`, `TYPE`, `KEYS` (pattern match), `RANDOMKEY`, `SCAN` (cursor-based iteration), `HSCAN`, `SSCAN`, `ZSCAN`.

**SCAN** — cursor-based key iteration using reverse binary iteration over a custom hash table with incremental rehashing. Stateless cursor, non-blocking, handles rehash mid-scan. Guarantees: every key present for the entire scan is returned at least once; may return duplicates (client deduplicates). `HSCAN`/`SSCAN`/`ZSCAN` are per-key variants for Hash, Set, and Sorted Set respectively. Pattern filtering via `MATCH` parameter.

### 2.2 Atomic Execution

**MULTI/EXEC** — buffer commands between `MULTI` and `EXEC`, execute as an atomic batch. All keys in the batch must hash to the same partition. If they don't, reject at `EXEC` time with an error. `DISCARD` aborts.

**EVAL (Lua)** — embedded LuaJ interpreter. Script runs atomically on a single partition (same as Redis). `redis.call()` and `redis.pcall()` bridge Lua into the command engine. `KEYS[]` / `ARGV[]` convention. All keys must hash to same partition — enforced before execution.

Atomicity guarantee: the command engine processes one command (or one script / one MULTI-EXEC batch) at a time per partition. No interleaving. This is the single-threaded-per-partition invariant.

### 2.3 Wire Protocol

**Client-facing: RESP2.** Full RESP2 implementation (Simple Strings, Errors, Integers, Bulk Strings, Arrays). Enough to pass `redis-cli` interaction for all supported commands. Inline command format (space-separated, no `*` prefix) also supported for `redis-cli` interactive use.

**Cluster-facing: gRPC + Protobuf.** All inter-node communication — replication, gossip, anti-entropy, hinted handoff, Chandy-Lamport markers — uses gRPC. Protobuf schemas define the contract.

### 2.4 Distribution

**Consistent hashing** — SHA-256 hash of key mapped onto a ring. Each physical node owns ≥128 virtual nodes (vnodes) for uniform distribution. A key's **preference list** is the N distinct physical nodes encountered walking clockwise from the key's hash position.

**SWIM gossip** — protocol period configurable (default 1s). Each period: ping a random peer; if no ack within RTT bound, ping-req via K random intermediaries; if still no ack, mark suspect; if suspect not refuted within T rounds, declare dead. Membership table propagated via piggybacked gossip on every message.

**Replication** — coordinator (first node in preference list) handles the write, replicates to N-1 successors asynchronously. Write returns to client after W acks. Read queries R replicas, returns the most recent (by DVV), triggers read repair if divergence detected.

**Sloppy quorum** — if a node in the preference list is unreachable, the next healthy node on the ring temporarily takes its slot and stores a **hint**. When the original node recovers, hints are forwarded and deleted.

**Anti-entropy** — periodic background process compares Merkle trees of vnode key ranges between replica pairs. Divergent ranges trigger key-level sync. Frequency configurable (default every 60s per vnode pair).

### 2.5 Conflict Resolution — Dotted Version Vectors

Every value carries a **Dotted Version Vector (DVV)** — a compact causal context that tracks which node wrote which version, without the client-scaling problem of classic vector clocks.

Structure: a DVV is `(dot, causal_context)` where:
- `dot = (node_id, counter)` — the event that created this version
- `causal_context = Map<NodeId, Long>` — the version history this write has seen

**Why DVVs over vector clocks:** In classic vector clocks, every writing client adds an entry, causing unbounded growth. DVVs track causality per *server node*, so the version vector size is bounded by cluster size (3–7), not client count. This is the approach Riak adopted after learning the hard way.

Merge rules per data type:

| Type | Concurrent write resolution |
|---|---|
| String | Last-writer-wins by DVV ordering; true concurrent → deterministic tiebreak (highest node-ID) |
| Hash | Field-level merge — per-field LWW by DVV |
| List | Concurrent appends: union (both preserved). Concurrent pop: LWW |
| Sorted Set | Element-level merge — union of adds, max score on conflict |

### 2.6 TTL & Expiration — Hierarchical Timer Wheel

All key expiry is managed by a **hierarchical timer wheel** — O(1) insert, O(1) cancel, O(1) per-expiration fire. No random sampling, no sweep coroutines.

Structure: multi-level wheel (e.g., 3 levels: 1-second slots × 256, 256-second slots × 256, ~18-hour slots × 256). Covers TTLs from milliseconds to days. On each tick, the wheel advances and fires all expired entries in the current slot.

Properties:
- Adding or removing a TTL does not scan any data structure
- A key's TTL change (re-EXPIRE) cancels the old slot entry and inserts a new one, both O(1)
- The wheel tick is driven by a single coroutine at millisecond resolution
- Expired keys are deleted lazily on access AND actively by the wheel — belt and suspenders

### 2.7 Eviction

When memory usage exceeds the configured threshold, the eviction policy selects victims. Three policies, switchable at config time:

**LRU** — approximate LRU via sampling (Redis-style: sample K random keys, evict the least-recently-used among them). Simple, well-understood baseline.

**LFU** — approximate LFU via a logarithmic frequency counter per key (Redis 4.0 style). Decays over time to avoid frequency fossilization.

**W-TinyLFU** — admission window (1% of capacity, LRU) + main space (99%, segmented LRU). New entries enter the window; on eviction from the window, a frequency sketch (Count-Min Sketch) decides whether the candidate beats the main-space victim. This is the Caffeine algorithm — highest hit ratio of the three.

### 2.8 Persistence

**Local RDB snapshots** — periodic serialization of all owned partitions to a binary file. Format: `[header][entry]*[checksum]` where each entry is `[key_len:u32][key][type:u8][dvv][ttl:i64][value_bytes]`. Triggered by configurable interval (default 300s) and on graceful shutdown. Snapshot must not block the command path — use a consistent point-in-time copy of the data structures (COW or versioned references).

**Write-Ahead Log (WAL)** — every mutation is appended to a sequential log on disk before the in-memory state is updated. On crash, replay the WAL from the last RDB checkpoint to recover all acknowledged writes. Format per entry: `[CRC32:u32][length:u32][seq_no:u64][op_type:u8][payload:variable]`. Three fsync policies: `ALWAYS` (safest, ~1k-5k ops/sec), `EVERY_SECOND` (sweet spot — lose at most 1s), `NEVER` (OS decides). Group commit amortizes fsync across concurrent writers. The WAL is checkpointed (truncated) after each successful RDB snapshot. Recovery sequence: load RDB → replay WAL entries after checkpoint sequence number → ready.

**Chandy-Lamport distributed snapshots** — cluster-wide consistent snapshot coordinated across all nodes:

1. **Initiator** records own local state, sends a **marker** on all outgoing gRPC channels
2. **Receiver** on first marker: records own local state, sends marker on all its outgoing channels, begins recording in-flight messages on all other incoming channels
3. **Receiver** on marker from channel C: stops recording messages on C
4. When all nodes have received markers on all channels: the snapshot = (all local states + all recorded in-flight messages) is globally consistent
5. Snapshot set is persisted (one file per node + channel message logs)

The snapshot captures a consistent cut — if event A causally precedes event B, and B is in the snapshot, then A is in the snapshot. This is the Chandy-Lamport guarantee.

Timeout: if any node fails to respond within the configured timeout (default 30s), the snapshot is aborted. Snapshots never block the write path.

### 2.9 Scripting (Lua)

Embedded via LuaJ (`org.luaj:luaj-jse`). Redis-compatible semantics:

- `redis.call(cmd, ...)` — execute command, propagate errors
- `redis.pcall(cmd, ...)` — execute command, catch errors as Lua table
- `KEYS[n]` — key arguments (1-indexed)
- `ARGV[n]` — non-key arguments
- All `KEYS` must hash to the same partition — validated before script execution
- Script execution is atomic: no other command on the same partition interleaves
- Scripts must be pure (same inputs → same outputs) for replication correctness
- No access to OS, filesystem, network, or random from within scripts

---

## 3. Core Constraints

Rules that the implementation must never violate. These are hard gates — a violation means a bug.

| ID | Constraint |
|---|---|
| **C1** | **Single-writer-per-partition.** Each partition's command engine processes exactly one command/script/transaction at a time. No concurrent mutation of the same partition's data structures. |
| **C2** | **DVV monotonicity.** A node's DVV counter for itself is strictly monotonically increasing. A node never decrements or reuses a counter value. |
| **C3** | **Preference list correctness.** A key's preference list is deterministically derived from the hash ring. All nodes must compute the same preference list for the same key given the same ring state. |
| **C4** | **Quorum arithmetic.** A write succeeds only after W acks from distinct nodes. A read queries R nodes and returns the value with the highest DVV. R + W > N must hold for configured values. |
| **C5** | **Hinted handoff fidelity.** A hint stored on a temporary node must contain the full write (key + value + DVV + TTL). When the target node recovers, the hint is replayed exactly as if the original write arrived. No data loss, no mutation. |
| **C6** | **Merkle tree consistency.** A Merkle tree for a vnode range must be computed from the actual key-value-DVV data in that range. Two nodes with identical data must produce identical Merkle roots. |
| **C7** | **Timer wheel correctness.** A key with TTL T set at time t must expire no later than t + T + (one wheel tick interval). Early expiry is never permitted — a key must be readable until its TTL elapses. |
| **C8** | **RESP fidelity.** Responses must be byte-identical to what Redis returns for supported commands. A Redis client library must not need modification. |
| **C9** | **Snapshot atomicity (local).** An RDB snapshot represents a consistent point-in-time view. It must not contain a half-applied MULTI/EXEC or a partially-replicated write. |
| **C10** | **Chandy-Lamport consistent cut.** A distributed snapshot must satisfy the consistent-cut property: if event A causally precedes B and B is in the snapshot, then A is in the snapshot. |
| **C11** | **Lua isolation.** A Lua script cannot access the OS, filesystem, network, system clock, or random number generator. Scripts are pure functions of their inputs + current cache state. |
| **C12** | **Partition-scoped atomicity.** MULTI/EXEC and EVAL operate on a single partition. If keys in a transaction or script span multiple partitions, the operation must be rejected before execution. |
| **C13** | **Type safety.** Executing a command on the wrong type (e.g., LPUSH on a String key) must return a WRONGTYPE error. The key's data must not be corrupted. |
| **C14** | **WAL write-ahead.** A write is appended to the WAL and (per fsync policy) durable on disk before the response is sent to the client. On crash, replaying the WAL from the last RDB checkpoint must recover every acknowledged write. |
| **C15** | **SCAN completeness.** A full SCAN iteration (cursor 0 → 0) must return every key that existed for the entire duration of the scan. Keys inserted or deleted mid-scan may or may not appear. Duplicates are permitted. |

---

## 4. Core Invariants

Properties that must hold at all times. Every invariant maps to at least one test.

| ID | Invariant | Assertable by |
|---|---|---|
| **I1** | **Convergence.** After all partitions heal, all messages drain, and one full anti-entropy cycle completes, all replicas of every key hold the same value and DVV. | Convergence checker: heal network → drain messages → run anti-entropy → read all replicas → assert equality |
| **I2** | **Crashing a minority of nodes (< N-W+1) does not lose any acknowledged write.** | Kill minority nodes → verify all previously-acked keys readable from survivors with quorum R |
| **I3** | **Skip list ordering.** For every sorted set, a full traversal in forward order produces elements in strictly non-decreasing score order, with lexicographic tiebreak on equal scores. | `ZRANGE 0 -1 WITHSCORES` result is sorted; random ZADD/ZREM sequences preserve ordering |
| **I4** | **DVV dominance.** If write A happened-before write B (A is in B's causal context), then B's DVV strictly dominates A's. No replica ever serves A after accepting B. | Track causal chains in test, verify DVV ordering matches |
| **I5** | **Ring determinism.** Given identical node-set and vnode count, every node computes the same hash ring and the same preference list for any key. | Compute ring on N separate instances, assert identical preference lists for 10,000 random keys |
| **I6** | **Eviction never deletes an unexpired key while expired keys exist.** Under memory pressure, expired keys are evicted before live keys. (Within live keys, eviction follows the configured policy.) | Fill cache to pressure, add expired keys, trigger eviction, assert expired removed first |
| **I7** | **Timer wheel fire order.** Keys with earlier expiry times fire before keys with later expiry times within the same wheel tick resolution. No inversion. | Insert keys with ascending TTLs, advance wheel, assert fire order matches insertion order |
| **I8** | **Gossip protocol convergence.** A membership change (node death or recovery) propagates to all live nodes within O(log N) gossip rounds. | Simulate node failure, count gossip rounds until all nodes agree |
| **I9** | **Hinted handoff completeness.** After a partitioned node rejoins and all hints drain, its data matches what it would have had if it were never partitioned. | Partition node → write K keys → heal → drain hints → compare with reference replica |
| **I10** | **Lua determinism.** The same script with the same KEYS/ARGV against the same cache state produces the same result on every node. | Execute identical script on two replicas with synchronized state, assert identical results |
| **I11** | **MULTI/EXEC atomicity.** If any command in a MULTI/EXEC batch fails, no commands in the batch take effect (all-or-nothing). | MULTI → SET a → deliberate error → SET b → EXEC → verify neither a nor b changed |
| **I12** | **Chandy-Lamport recoverability.** A cluster restored from a Chandy-Lamport snapshot behaves identically to the cluster at the moment of the snapshot for all subsequent reads. | Take snapshot → continue writes → restore snapshot → verify reads return snapshot-time values, not post-snapshot values |

---

## 5. Semantics

Precise behavioral definitions that resolve ambiguity.

### 5.1 Write Path

1. Client sends command to any node (the **contact node**)
2. Contact node hashes the key → identifies the **coordinator** (first node in preference list)
3. If contact ≠ coordinator: forward via gRPC. If contact = coordinator: proceed locally.
4. Coordinator writes to its own partition engine, bumps DVV with its own node-ID + next counter
5. Coordinator sends replication request (with value + DVV) to N-1 successors in preference list
6. If W-1 acks received (coordinator counts as one of W): return success to client
7. If W-1 acks not received and some nodes in preference list are dead: use sloppy quorum — send to next healthy node on ring, which stores a hint
8. If W acks still not achievable: return error to client

### 5.2 Read Path

1. Contact node identifies coordinator from preference list
2. Coordinator sends read requests to R-1 other replicas (or reads locally + R-1 remote)
3. Collect R responses. Compare DVVs.
4. Return the value with the dominating DVV (or the tiebreak winner if truly concurrent)
5. If any of the R responses was stale: trigger **read repair** — push the winning value+DVV to stale replicas asynchronously

### 5.3 DVV Merge

When a node receives a value with DVV_remote and holds DVV_local for the same key:
- If DVV_remote dominates DVV_local → replace local with remote
- If DVV_local dominates DVV_remote → keep local (remote is stale)
- If concurrent (neither dominates) → apply type-specific merge rule (§2.5), create new DVV that descends from both

### 5.4 Expiry Semantics

- A key with TTL is readable until `creation_time + TTL`. After that instant, the key does not exist.
- The timer wheel fires the deletion. If the wheel hasn't ticked yet but a read/write arrives for the key, the lazy check deletes it on access.
- `PERSIST` removes the TTL. The timer wheel entry is cancelled in O(1).
- `EXPIRE` on a key with existing TTL replaces it. Old wheel entry cancelled, new one inserted.
- A key that has expired is not included in RDB snapshots.
- TTL is replicated as absolute timestamp (not relative duration) to avoid clock skew issues across replicas.

### 5.5 Eviction Semantics

- Eviction is local to each node. Nodes do not coordinate eviction decisions.
- Eviction runs when memory usage crosses the configured threshold.
- Order: (1) remove all expired keys first, (2) apply eviction policy to live keys.
- Eviction is asynchronous to the command path — it does not block reads/writes beyond the per-partition lock.
- An evicted key may still exist on replicas. This is acceptable for a cache — clients handle cache misses.

### 5.6 MULTI/EXEC Semantics

- Commands between MULTI and EXEC are buffered, not executed.
- On EXEC: all keys are verified to belong to the same partition. If not → error, nothing executed.
- Commands execute sequentially in buffer order within a single partition lock.
- If a command fails mid-batch: the batch continues (Redis behavior — MULTI/EXEC is not transactional rollback). However, the error is reported in the response array.
- DISCARD clears the buffer.

Note: Redis MULTI/EXEC also does not roll back on error. This matches Redis semantics exactly.

### 5.7 Lua Semantics

- `EVAL script numkeys key [key ...] arg [arg ...]`
- All `KEYS` must hash to the same partition — checked before execution
- Script runs inside the partition lock — atomic, no interleaving
- `redis.call()` raises on error; `redis.pcall()` returns error as Lua table
- Return values follow Redis-Lua type conversion rules (number → integer reply, string → bulk string, table → array, boolean → integer 1/nil)
- No global state persists between EVAL calls
- `math.random` / `os.*` / `io.*` are removed from the Lua environment

---

## 6. Tests That Must Always Pass

Named tests that serve as the project's definition of correctness. Organized by area. Every milestone must keep all prior tests green.

### 6.1 Data Structure Tests

| Test | Asserts |
|---|---|
| `string_set_get_roundtrip` | SET then GET returns the value |
| `string_set_nx_rejects_existing` | SET NX on existing key returns nil, value unchanged |
| `string_set_xx_rejects_missing` | SET XX on missing key returns nil |
| `string_set_ex_expires` | SET EX 1 → wait > 1s → GET returns nil |
| `string_incr_atomic` | INCR on "10" returns 11; INCR on non-numeric returns error; INCR on missing key returns 1 |
| `hash_field_independence` | HSET/HGET/HDEL on individual fields don't affect others |
| `hash_getall_complete` | HGETALL returns all fields, no more, no less |
| `list_push_pop_order` | LPUSH a b c → RPOP returns a, LPOP returns c (LIFO from each end) |
| `list_lrange_bounds` | LRANGE with out-of-bounds indices clamps, does not error |
| `zset_ordering_invariant` | Random ZADD/ZREM sequence → ZRANGE always sorted by score, lex tiebreak |
| `zset_rank_consistency` | ZRANK matches position in ZRANGE output |
| `zset_score_update` | ZADD existing member with new score updates, preserves ordering |
| `wrongtype_rejected` | LPUSH on a String key → WRONGTYPE error, key unchanged |

### 6.1b SCAN + Hash Table Tests

| Test | Asserts |
|---|---|
| `scan_returns_all_keys` | Full SCAN iteration returns every key, no key missed |
| `scan_cursor_zero_terminates` | SCAN loop terminates with cursor 0 |
| `scan_match_filters` | SCAN with MATCH pattern returns only matching keys |
| `scan_during_rehash_no_miss` | Insert keys mid-scan to trigger rehash → all pre-existing keys still returned |
| `scan_may_duplicate` | Duplicates are acceptable (client deduplicates) |
| `incremental_rehash_no_block` | Rehash spreads across operations, no single operation migrates all buckets |
| `hashtable_put_get_remove` | Basic hash table operations work correctly |

### 6.2 Skip List Tests

| Test | Asserts |
|---|---|
| `skiplist_insert_order` | Insert N random elements → forward traversal is sorted |
| `skiplist_delete_preserves_order` | Delete random elements → remaining still sorted |
| `skiplist_range_query` | Range [lo, hi] returns exactly the elements with scores in that range |
| `skiplist_rank_correct` | Rank of element matches its 0-based position in sorted order |
| `skiplist_duplicate_score_lex_order` | Elements with equal score sort lexicographically by member |
| `skiplist_log_n_property` | Insert 100,000 elements → average comparisons per search ≤ 2 × log₂(N) |

### 6.3 Timer Wheel Tests

| Test | Asserts |
|---|---|
| `wheel_fires_on_time` | Key with TTL 5s fires between 5000ms and 5000ms + tick_interval |
| `wheel_no_early_fire` | Key with TTL 10s is readable at 9999ms |
| `wheel_cancel_prevents_fire` | PERSIST cancels the timer; key survives past original TTL |
| `wheel_replace_ttl` | Re-EXPIRE updates the fire time; old timer does not fire |
| `wheel_ordering` | Keys inserted with TTL 1s, 2s, 3s fire in that order |
| `wheel_high_volume` | Insert 1M keys with random TTLs; all fire within tick_interval of their deadline |

### 6.4 Eviction Tests

| Test | Asserts |
|---|---|
| `eviction_respects_max_memory` | After eviction, memory usage ≤ threshold |
| `eviction_prefers_expired` | Expired keys evicted before live keys |
| `lru_evicts_oldest_access` | Under LRU: least-recently-accessed key is evicted first |
| `tinylfu_admits_frequent` | Under W-TinyLFU: frequently-accessed new key admitted; infrequent new key rejected |
| `eviction_does_not_corrupt` | After eviction, remaining keys return correct values |

### 6.5 DVV Tests

| Test | Asserts |
|---|---|
| `dvv_dominance_detection` | A → B chain: B dominates A |
| `dvv_concurrent_detection` | Independent writes on two nodes: neither dominates |
| `dvv_merge_preserves_causality` | Merge of concurrent DVVs descends from both |
| `dvv_bounded_size` | After 10,000 writes from 100 clients through 3 nodes, DVV size ≤ 3 (node count) |
| `dvv_no_counter_reuse` | Crash and restart a node; its counter resumes above the pre-crash value |

### 6.6 RESP Protocol Tests

| Test | Asserts |
|---|---|
| `resp_encode_decode_roundtrip` | Every RESP type survives encode → decode |
| `resp_bulk_string_nil` | `$-1\r\n` decodes to nil |
| `resp_error_format` | Error responses start with `-` and contain error type |
| `resp_inline_command` | `PING\r\n` (no `*` prefix) parses correctly |
| `resp_fuzz_no_crash` | 10,000 random byte sequences → no crash, all return error or valid parse |

### 6.7 Cluster Integration Tests

| Test | Asserts |
|---|---|
| `ring_determinism` | 3 nodes compute identical preference lists for 10,000 keys |
| `write_read_quorum` | W=2, R=2, N=3: write then read returns value |
| `minority_failure_available` | Kill 1 of 3 nodes: reads and writes still succeed |
| `majority_failure_unavailable` | Kill 2 of 3 nodes: writes fail (cannot reach W=2) |
| `gossip_detects_failure` | Kill a node; all survivors mark it dead within O(log N) gossip rounds |
| `gossip_detects_recovery` | Restart a node; all peers mark it alive within O(log N) gossip rounds |
| `hinted_handoff_replays` | Partition a node → write → heal → hints drain → partitioned node has the data |
| `read_repair_fixes_stale` | Force one replica stale → read triggers repair → all replicas converge |
| `anti_entropy_heals_divergence` | Silently corrupt one replica → anti-entropy cycle detects and fixes it |
| `convergence_after_partition` | Network partition → concurrent writes on both sides → heal → all replicas converge to merged state |

### 6.8 Snapshot Tests

| Test | Asserts |
|---|---|
| `rdb_save_restore_roundtrip` | Snapshot → restart → all keys + TTLs + types intact |
| `rdb_excludes_expired` | Expired keys not present in snapshot |
| `rdb_concurrent_writes` | Snapshot taken during active writes → snapshot is a valid point-in-time (no torn writes) |
| `chandy_lamport_consistent_cut` | Snapshot during traffic → if B is in snapshot and A→B, then A is in snapshot |
| `chandy_lamport_restorable` | Restore cluster from snapshot → reads return snapshot-time values |
| `chandy_lamport_timeout_aborts` | Kill a node mid-snapshot → snapshot aborts cleanly, no state corruption |

### 6.8b WAL Tests

| Test | Asserts |
|---|---|
| `wal_write_read_roundtrip` | Write entries to WAL, read back — all entries intact with correct sequence numbers |
| `wal_crash_recovery` | Truncate last entry mid-write (simulated crash) → reader recovers all complete entries, skips partial |
| `wal_crc_detects_corruption` | Flip a byte mid-file → reader stops at corrupted entry, returns all prior entries |
| `wal_checkpoint_truncates` | After RDB snapshot, WAL checkpoint removes entries before snapshot sequence number |
| `wal_full_recovery` | Write keys → RDB snapshot → more writes → "crash" → restore from RDB + WAL replay → all keys present |
| `wal_fsync_always_durable` | With ALWAYS policy, each append triggers fsync |
| `wal_fsync_every_second_batches` | With EVERY_SECOND policy, fsync count << write count |
| `wal_group_commit_amortizes` | Concurrent writers share a single fsync — fsync count << writer count |
| `wal_replay_idempotent` | Replaying the same WAL entry twice produces the same state as replaying once |

### 6.9 Transaction & Scripting Tests

| Test | Asserts |
|---|---|
| `multi_exec_atomic` | Concurrent reader never sees partial MULTI/EXEC state |
| `multi_exec_cross_partition_rejected` | Keys on different partitions → error at EXEC |
| `discard_clears_buffer` | MULTI → SET → DISCARD → key unchanged |
| `lua_redis_call` | EVAL with redis.call('SET',...) then redis.call('GET',...) returns correct value |
| `lua_keys_argv` | KEYS and ARGV arrays accessible and correct |
| `lua_cross_partition_rejected` | Script with keys on different partitions → error before execution |
| `lua_no_side_effects` | `os.execute`, `io.open`, `math.random` → nil or error |
| `lua_deterministic` | Same script + same state on two replicas → identical result |

---

## 7. Reading List

Organized by topic. Read the relevant section before starting the corresponding milestone.

### Distributed Systems — The Foundations

| Resource | Pages | Relevant to |
|---|---|---|
| **Dynamo: Amazon's Highly Available Key-value Store** (DeCandia et al., 2007) | 16 | Consistent hashing, quorum, vector clocks, hinted handoff, anti-entropy — the architectural blueprint |
| **SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol** (Das et al., 2002) | 12 | Gossip membership and failure detection |
| **Distributed Snapshots: Determining Global States of Distributed Systems** (Chandy & Lamport, 1985) | 10 | The distributed snapshot algorithm |
| **Dotted Version Vectors: Logical Clocks for Optimistic Replication** (Preguiça et al., 2012) | ~20 | DVVs — replaces classic vector clocks, solves sibling explosion |
| **Cassandra: A Decentralized Structured Storage System** (Lakshman & Malik, 2010) | 10 | Dynamo ideas at scale — practical implementation of gossip + hinted handoff + anti-entropy together |

### Data Structures

| Resource | Pages | Relevant to |
|---|---|---|
| **Skip Lists: A Probabilistic Alternative to Balanced Trees** (Pugh, 1990) | 8 | Sorted Set implementation |
| Redis source: `t_zset.c`, `server.h` (struct `zskiplist`) | ~500 LOC | Reference for dual skip-list + hash-map ZSet structure |
| **TinyLFU: A Highly Efficient Cache Admission Policy** (Einziger et al., 2017) | ~15 | W-TinyLFU eviction — Count-Min Sketch + windowed LRU |
| Caffeine source: `FrequencySketch.java`, `BoundedLocalCache.java` | ~200 LOC (sketch) | Production W-TinyLFU implementation |
| Redis source: `dict.c` — `dictScan()` | ~80 LOC | Reverse binary iteration algorithm for SCAN |
| Redis source: `dict.c` — `_dictRehashStep()` | ~50 LOC | Incremental rehashing — one bucket per operation |
| **Hashed and Hierarchical Timing Wheels** (Varghese & Lauck, 1987) | 14 | Timer wheel for TTL expiration |
| Netty source: `HashedWheelTimer.java` | ~600 LOC | JVM reference implementation of timer wheel |
| Kafka source: `TimingWheel.scala` | ~200 LOC | Hierarchical timer wheel variant |

### Wire Protocol

| Resource | Pages | Relevant to |
|---|---|---|
| Redis Protocol specification (redis.io RESP2 docs) | ~5 | RESP2 parser/encoder |
| Redis source: `networking.c` | ~1000 LOC | How Redis reads/writes RESP over sockets |

### Consistent Hashing

| Resource | Pages | Relevant to |
|---|---|---|
| **Consistent Hashing and Random Trees** (Karger et al., 1997) | 12 | Original paper — proves load-balancing properties |
| **Jump Consistent Hash** (Lamping & Veach, 2014) | 6 | Simpler alternative — worth knowing even if we use ring-based |

### Conflict Resolution & Replication

| Resource | Pages | Relevant to |
|---|---|---|
| **Riak DVV source** (`dvvset.erl` in riak_dt) | ~300 LOC | Production DVV implementation — Erlang but readable |
| **A comprehensive study of Convergent and Commutative Replicated Data Types** (Shapiro et al., 2011) | ~50 | CRDTs — background for understanding merge semantics |
| **Merkle Hash Trees** — any modern summary or Ralph Merkle's thesis | varies | Anti-entropy divergence detection |

### Persistence

| Resource | Pages | Relevant to |
|---|---|---|
| Redis source: `rdb.c` | ~2000 LOC | RDB serialization format — `rdbSave` / `rdbLoad` cycle |
| **ARIES: A Transaction Recovery Method** (Mohan et al., 1992) | ~20 (sections 1-6) | The foundational WAL paper — write-ahead logging, checkpointing, crash recovery |
| SQLite WAL mode documentation | ~5 | Most readable WAL explanation |
| Redis source: `aof.c` | ~1500 LOC | Practical WAL implementation (Redis calls it AOF) |
| **Virtual Time and Global States of Distributed Systems** (Mattern, 1989) | ~15 | Formalizes vector clocks and consistent cuts — useful alongside Chandy-Lamport |

### General Background (optional, high-value)

| Resource | Why |
|---|---|
| **Designing Data-Intensive Applications** (Kleppmann, 2017) — Ch. 5 (Replication), Ch. 6 (Partitioning), Ch. 7 (Transactions) | Best single-source summary. If you read one book, this one. |
| **Scaling Memcache at Facebook** (Nishtala et al., 2013) | Cache-specific problems: thundering herd, lease tokens, stale sets. Different system, overlapping concerns. |

---

## 8. Milestones

Sequenced for incremental correctness. Each milestone keeps all prior tests green. No interface prescriptions — just deliverables and exit criteria.

| M | Theme | Deliverable | Exit criteria |
|---|---|---|---|
| **M0** | Scaffold | Maven multi-module project, Kotlin 2.x, CI stub, empty module stubs | `mvn package` compiles |
| **M1** | Data engine — String, Hash, List | Core command execution for String (incl. SET NX/XX/EX/PX, INCR/DECR), Hash, List | §6.1 String/Hash/List tests pass |
| **M1b** | SCAN + custom hash table | Custom hash table with incremental rehashing, SCAN with reverse binary iteration, HSCAN/ZSCAN | §6.1b SCAN tests pass |
| **M2** | Skip list + Sorted Set | Skip list implementation, ZSet dual-index, all ZADD–ZCARD commands | §6.1 ZSet tests + §6.2 skip list tests pass |
| **M3** | Timer wheel + TTL | Hierarchical timer wheel, EXPIRE/PEXPIRE/TTL/PTTL/PERSIST, lazy + active expiry | §6.3 timer wheel tests pass |
| **M4** | Eviction | LRU baseline, then W-TinyLFU (frequency sketch + admission window) | §6.4 eviction tests pass |
| **M5** | RESP + single-node server | RESP2 parser/encoder, Netty pipeline, single-node serving all commands via `redis-cli` | §6.6 RESP tests pass; `redis-cli` SET/GET/ZADD/ZRANGE works |
| **M6** | Transactions + Lua | MULTI/EXEC/DISCARD, LuaJ embedding, EVAL with `redis.call`, partition-scoped enforcement | §6.9 transaction + scripting tests pass |
| **M7** | Consistent hashing + ring | Hash ring with vnodes, preference list computation, request routing (local + forward) | §6.7 `ring_determinism` passes; keys distribute evenly |
| **M8** | SWIM gossip | Membership protocol: ping, ping-req, suspect, dead, recovery detection | §6.7 `gossip_detects_failure` + `gossip_detects_recovery` pass |
| **M9** | Replication + quorum | Async replication, W/R quorum, DVVs on every value | §6.5 DVV tests + §6.7 `write_read_quorum` + `minority_failure_available` pass |
| **M10** | Sloppy quorum + hinted handoff | Temporary hint storage, hint replay on recovery | §6.7 `hinted_handoff_replays` passes |
| **M11** | Read repair + Merkle anti-entropy | Read-path divergence detection, Merkle tree per vnode range, background sync | §6.7 `read_repair_fixes_stale` + `anti_entropy_heals_divergence` pass |
| **M12** | Conflict resolution | DVV merge + type-specific merge rules for concurrent writes | §6.7 `convergence_after_partition` passes |
| **M13** | Local RDB snapshots | Background snapshot without blocking commands, restore on startup | §6.8 `rdb_*` tests pass |
| **M13b** | Write-Ahead Log | WAL writer/reader, fsync policies, group commit, checkpoint with RDB, crash recovery | §6.8b WAL tests pass |
| **M14** | Chandy-Lamport distributed snapshots | Marker protocol, consistent cut capture, cluster-wide restore | §6.8 `chandy_lamport_*` tests pass |
| **M15** | Full integration | 3-node cluster under chaos: random kills, partitions, concurrent traffic → convergence | All §6.7 + §6.8 tests pass; `redis-cli` works end-to-end |

---

## 9. Success Signal

DynaCache is done when this demo works:

> Start a 3-node cluster. Terminal A runs `redis-cli` against node 1: `SET foo bar EX 60`, `GET foo` returns `bar` from any node. `ZADD leaderboard 100 alice 200 bob`, `ZRANGE leaderboard 0 -1 WITHSCORES` returns sorted. Run a Lua script: `EVAL "redis.call('SET', KEYS[1], redis.call('GET', KEYS[1]) + 1); return redis.call('GET', KEYS[1])" 1 counter` — atomic increment works. Kill node 2 — reads and writes still succeed. Restart node 2 — hinted handoff replays missed writes, anti-entropy verifies Merkle trees, DVVs converge. Run concurrent writes to the same key on two partitioned nodes — after heal, replicas agree on a merged value. Trigger a Chandy-Lamport snapshot while traffic is running — restore later, cluster state is consistent. TTLs fire on time via the timer wheel. Under memory pressure, W-TinyLFU evicts cold keys, keeps hot ones. `redis-cli` never knows it's not talking to Redis.

---

## 10. Open Questions

None — all design decisions are locked.
