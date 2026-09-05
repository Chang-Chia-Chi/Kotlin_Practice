# DynaCache P1 - Data Engine + Single Node (T01 to T16)

Companion to `../plan.md` (ground rules, seams, DAG, model routing). Spec: `../design-spec.md`
sections 2.1 to 2.3, 2.6, 2.7, 2.9, 3, 5.4 to 5.7, 6.1 to 6.4, 6.6, 6.9.

**Goal:** `redis-cli` and an unmodified Jedis work against one DynaCache node for every
supported command: String, Hash, List, Sorted Set, TTL, SCAN, MULTI/EXEC, EVAL.

**Architecture:** the engine owns one JDK single-thread executor per fixed local partition and
exposes `submit(command)` returning a future (ADR 0001). The server parses RESP into `Command`,
submits, and encodes the `Reply`. Multi-key commands (`MGET`, `MSET`, `DEL k1 k2`, `EXISTS k1
k2`, `KEYS`, `SCAN`, `DBSIZE`, `FLUSHDB`) are fanned out and joined inside the engine and are
atomic only within a partition (ADR 0002). Vocabulary: `DynaCache/CONTEXT.md`.

Reading for the human, not the agent: spec 7 rows for Pugh (skip lists), Varghese and Lauck
(timer wheels), Einziger (TinyLFU), Redis `dict.c` (SCAN).

---

### T01 - Skeleton, frozen reply model, test tooling

- **Goal:** the April scaffold becomes the plan's scaffold.
- **Deliverables:** parent pom drops AssertJ and adds Mockito (core, test scope) next to JUnit
  5; every module pom loses AssertJ; `Reply` sealed class (`Simple`, `Error(kind, message)`,
  `Integer`, `Bulk(bytes?)`, `Array`) with byte-array equality; `Key` value type (binary-safe,
  equality by content, hash-tag aware: `{tag}` is the hashed part when present); `PartitionId`;
  `Command` sealed root with `Ping` only; `CommandEngine` interface with `submit` and
  `atomically` (both throwing `NotImplementedError`).
- **Blocked by:** none.
- **Fixed contracts:** plan 2.2, 2.3; the five RESP2 reply types (C8); CONTEXT.md vocabulary.
- **Acceptance:** `mvn package` green from `DynaCache/`; one test per module compiles and
  passes; `grep assertj` over the poms finds nothing; `Reply.Bulk(null)` and
  `Reply.Bulk(bytes)` compare by content; `Key("{user1}.a")` and `Key("{user1}.b")` hash alike.
- **Model:** Opus. **Size:** small.

### T02 - Engine walking skeleton and partition executors

- **Goal:** GET and SET through the real path, and C1 enforced where concurrency exists.
- **Deliverables:** the engine owns a fixed number of partitions (constructor argument), each
  with one JDK single-thread executor and its own store; `Key` hashes (hash tag aware) to a
  partition; `submit(command)` returns a `CompletableFuture<Reply>` completed on the partition's
  thread; `Clock` injected; `GET`, `SET` with `NX`, `XX`, `EX`, `PX` (expiry stored as an
  absolute instant, checked lazily on access; the wheel comes in T09), single-key `DEL`,
  `EXISTS`, `TYPE`, `PING`; `close()` shuts the executors down. The cluster module gets a
  one-line `await()` bridge to coroutines.
- **Blocked by:** T01.
- **Fixed contracts:** C1; spec 2.1 String `SET` flag semantics; plan 2.5; ADR 0001.
- **Acceptance:** `string_set_get_roundtrip`, `string_set_nx_rejects_existing`,
  `string_set_xx_rejects_missing`, `string_set_ex_expires` (clock advanced, not slept);
  `C1_one_command_at_a_time_per_partition` (Lincheck or an interleaving test: two commands on
  one partition never overlap, commands on two partitions may);
  `keys_with_same_hash_tag_share_a_partition`.
- **Model:** Fable. **Size:** medium.

### T03 - String completion and Hash

- **Goal:** spec 2.1 String and Hash command sets complete.
- **Deliverables:** `INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN`, `MGET`, `MSET`
  (the engine fans multi-key commands out per partition and joins in argument order, ADR
  0002; multi-key `DEL` and `EXISTS` join the same path); `HGET`, `HSET`, `HDEL`, `HGETALL`,
  `HMGET`, `HMSET`, `HEXISTS`, `HKEYS`, `HVALS`, `HLEN`; Redis error text for non-integer `INCR`.
- **Blocked by:** T02.
- **Fixed contracts:** spec 2.1 String and Hash; Redis reply shapes for each command; ADR 0002.
- **Acceptance:** `string_incr_atomic`, `hash_field_independence`, `hash_getall_complete`;
  `mget_spans_partitions` (keys on two partitions, one reply array in argument order);
  `mget_across_partitions_is_not_atomic` (a write parked between two partitions of one `MGET`
  is visible in the result; pins ADR 0002).
- **Model:** Opus. **Size:** medium.

### T04 - List and key management, WRONGTYPE

- **Goal:** spec 2.1 List and Server command sets, and C13.
- **Deliverables:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LREM`
  on a deque with O(1) both ends; `DBSIZE`, `FLUSHDB`, `KEYS` (glob), `RANDOMKEY`, `COMMAND`
  (minimal), `INFO` (minimal); type check before any mutation so a wrong-type command leaves
  the entry untouched.
- **Blocked by:** T02.
- **Fixed contracts:** spec 2.1 List and Server; C13.
- **Acceptance:** `list_push_pop_order`, `list_lrange_bounds`, `wrongtype_rejected`,
  `C13_wrongtype_leaves_value_intact` (LPUSH on a String key, then GET returns the original).
- **Model:** Opus. **Size:** medium.

### T05 - Hash table with incremental rehash, SCAN family

- **Goal:** spec 2.1 SCAN and C15 on a hand-built table.
- **Deliverables:** open hash table with two tables during rehash and one bucket migrated per
  operation; reverse binary iteration cursor; the table replaces the key map in every
  `PartitionStore` and the field map of Hash; `SCAN` with `COUNT` and `MATCH` whose cursor
  encodes partition plus inner cursor; `HSCAN`. `ZSCAN` lands in T07.
- **Blocked by:** T03.
- **Fixed contracts:** C15; spec 2.1 SCAN guarantees.
- **Acceptance:** `scan_returns_all_keys`, `scan_cursor_zero_terminates`,
  `scan_match_filters`, `scan_during_rehash_no_miss`, `scan_may_duplicate`,
  `incremental_rehash_no_block`, `hashtable_put_get_remove`, `C15_scan_completeness`
  (seeded insert and delete storm during a scan; every key present throughout is returned).
- **Model:** Fable. **Size:** large.

### T06 - Skip list

- **Goal:** spec 2.1 Sorted Set's ordered index.
- **Deliverables:** skip list keyed by (score, member bytes) with seeded level generation,
  insert, delete, update score, range by score, range by rank, rank of member, forward and
  reverse traversal.
- **Blocked by:** T01.
- **Fixed contracts:** I3 ordering with lexicographic tiebreak; spec 6.2.
- **Acceptance:** `skiplist_insert_order`, `skiplist_delete_preserves_order`,
  `skiplist_range_query`, `skiplist_rank_correct`, `skiplist_duplicate_score_lex_order`,
  `skiplist_log_n_property` (100,000 inserts, average comparisons per search at most 2 log2 N).
- **Model:** Opus. **Size:** medium.

### T07 - Sorted Set commands

- **Goal:** spec 2.1 Sorted Set on the dual index.
- **Deliverables:** `ZADD`, `ZREM`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZRANK`,
  `ZREVRANK`, `ZSCORE`, `ZCARD`, `ZINCRBY` (`WITHSCORES` where Redis has it), `ZSCAN`; member
  to score in the T05 table, order in the T06 skip list, both updated together.
- **Blocked by:** T05, T06.
- **Fixed contracts:** spec 2.1 Sorted Set; I3.
- **Acceptance:** `zset_ordering_invariant` (seeded ZADD and ZREM storm), `zset_rank_consistency`,
  `zset_score_update`, `I3_zrange_sorted_with_lex_tiebreak`, `zscan_returns_all_members`.
- **Model:** Opus. **Size:** medium.

### T08 - Hierarchical timer wheel

- **Goal:** spec 2.6 and C7, I7 as a standalone structure.
- **Deliverables:** three-level wheel (slot widths and counts configurable, spec's defaults),
  `schedule(key, deadline)`, `cancel(key)`, `reschedule`, `advanceTo(instant)` firing every
  due entry in deadline order and cascading lower levels; O(1) schedule and cancel; no thread,
  no clock read inside the wheel.
- **Blocked by:** T01.
- **Fixed contracts:** C7 (never early, at most one tick late); I7 fire order.
- **Acceptance:** `wheel_fires_on_time`, `wheel_no_early_fire`, `wheel_cancel_prevents_fire`,
  `wheel_replace_ttl`, `wheel_ordering`, `wheel_high_volume` (1,000,000 seeded deadlines, each
  fires within one tick of its deadline), `I7_fire_order_never_inverts`,
  `C7_never_fires_before_deadline`.
- **Model:** Fable. **Size:** medium.

### T09 - TTL commands and active expiry

- **Goal:** spec 5.4 through the engine.
- **Deliverables:** `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `TTL`, `PTTL`, `PERSIST`; `SET EX/PX` and
  every TTL command schedule on the partition's wheel; re-`EXPIRE` cancels and reschedules;
  `PERSIST` cancels; lazy check on access stays; expired keys are absent from `KEYS`, `SCAN`,
  `DBSIZE`, `RANDOMKEY`; TTL stored as an absolute instant; the server owns one scheduler
  coroutine per partition that calls `advanceTo(clock.instant())` on the executor.
- **Blocked by:** T02, T08.
- **Fixed contracts:** spec 5.4; C7 at the command level.
- **Acceptance:** `expire_replaces_wheel_entry`, `persist_cancels_expiry`,
  `expireat_absolute`, `ttl_reports_remaining_and_minus_values` (Redis -1 and -2),
  `C7_key_readable_until_deadline_then_absent` (clock at deadline minus one ms, then at
  deadline plus one tick), `string_set_ex_expires` now through the wheel.
- **Model:** Opus. **Size:** medium.

### T10 - Memory accounting and LRU eviction

- **Goal:** spec 2.7 LRU and 5.5.
- **Deliverables:** approximate byte size per entry maintained on every mutation; per-node
  threshold split evenly per partition; eviction step run on the partition executor after any
  write that crosses the threshold, bounded per step; expired keys removed first, then sampled
  LRU (K random keys, evict the least recently accessed) until under threshold; access
  timestamps from the injected clock; `INFO` reports used memory.
- **Blocked by:** T07, T09.
- **Fixed contracts:** spec 5.5; I6.
- **Acceptance:** `eviction_respects_max_memory`, `eviction_prefers_expired`,
  `lru_evicts_oldest_access`, `eviction_does_not_corrupt`, `I6_expired_evicted_before_live`.
- **Model:** Opus. **Size:** medium.

### T11 - Count-Min Sketch and W-TinyLFU

- **Goal:** spec 2.7 W-TinyLFU as a switchable policy.
- **Deliverables:** four-row Count-Min Sketch with periodic halving (aging); admission window
  (1 percent, LRU) and main space (segmented LRU, probation and protected); on window
  eviction, the candidate's estimated frequency against the main victim's decides admission;
  policy chosen at engine construction (`LRU` or `W_TINYLFU`); LFU (spec 2.7) is a
  frequency-counter variant of the same sampling loop, or recorded as a deviation if it does
  not fit the budget.
- **Blocked by:** T10.
- **Fixed contracts:** spec 2.7.
- **Acceptance:** `tinylfu_admits_frequent`, `sketch_estimate_never_underestimates`,
  `sketch_ages_halves_counts`, `eviction_respects_max_memory` under `W_TINYLFU`.
- **Model:** Opus. **Size:** medium.

### T12 - RESP2 codec

- **Goal:** spec 2.3 client-facing protocol, C8 at the byte level.
- **Deliverables:** incremental decoder (partial frames resume) for arrays of bulk strings
  and the inline form; encoder from `Reply` to bytes; Redis error prefix conventions
  (`-ERR`, `-WRONGTYPE`, `-EXECABORT`); a fuzz test with a seeded generator.
- **Blocked by:** T01.
- **Fixed contracts:** C8; the RESP2 grammar.
- **Acceptance:** `resp_encode_decode_roundtrip`, `resp_bulk_string_nil`, `resp_error_format`,
  `resp_inline_command`, `resp_fuzz_no_crash` (10,000 seeded byte sequences),
  `C8_reply_bytes_match_redis` (a golden table of reply bytes taken from real Redis for every
  reply type).
- **Model:** Opus. **Size:** small.

### T13 - Command parser and Netty server

- **Goal:** a socket in front of the executors.
- **Deliverables:** token-to-`Command` parser for every command of T02 to T09 (case-
  insensitive names, arity errors and unknown-command errors in Redis wording); Netty pipeline
  decoder, `engine.submit`, encoder on future completion; per-connection reply order
  preserved under pipelining; `main` with port and partition count from arguments; a test-kit
  RESP client over a plain socket.
- **Blocked by:** T09, T12.
- **Fixed contracts:** C8; plan 2.3 (the server only submits; it never serializes or splits).
- **Acceptance:** `server_ping_pong` over a socket; `server_pipelined_replies_in_order` (100
  pipelined commands); `server_unknown_command_error`; `server_arity_error`;
  `parser_maps_every_command` (one row per command name).
- **Model:** Opus. **Size:** large (if past budget: parser first, pipeline second).

### T14 - MULTI, EXEC, DISCARD and `atomically`

- **Goal:** spec 2.2, 5.6, C12, I11.
- **Deliverables:** `CommandEngine.atomically(keys) { ctx -> }` runs its block on the declared
  keys' partition executor, rejects keys on different partitions before running, and answers
  `ctx.execute` on an undeclared key with an error reply while the batch continues;
  per-connection queue state (`MULTI` buffers, a parse error while queued makes `EXEC` reply
  `-EXECABORT`); `EXEC` runs the buffer in order inside `atomically`, replies an array with
  per-command errors in place, does not roll back; `DISCARD` clears.
- **Blocked by:** T13.
- **Fixed contracts:** spec 5.6; C12; I11 (isolation, not rollback); CONTEXT.md "batch".
- **Acceptance:** `multi_exec_atomic` (a reader on the same partition sees pre-batch or
  post-batch state, never between), `multi_exec_cross_partition_rejected`,
  `multi_exec_hash_tags_allow_two_keys`, `discard_clears_buffer`,
  `I11_failing_command_does_not_undo_neighbours`,
  `C12_atomically_rejects_span_before_running`, `C12_undeclared_key_inside_batch_is_an_error`.
- **Model:** Opus. **Size:** medium.

### T15 - Lua scripting

- **Goal:** spec 2.9, 5.7, C11, I10.
- **Deliverables:** LuaJ globals with `os`, `io`, `math.random`, `math.randomseed`, `require`,
  `load`, `dofile` removed; `redis.call` raising and `redis.pcall` returning an error table;
  `KEYS` and `ARGV`; Redis-to-Lua and Lua-to-Redis type conversion of spec 5.7; the script runs
  inside `atomically` over its `KEYS`; no state survives between calls; `EVAL` only (`EVALSHA`
  is out).
- **Blocked by:** T14.
- **Fixed contracts:** C11; C12; I10; spec 5.7.
- **Acceptance:** `lua_redis_call`, `lua_keys_argv`, `lua_cross_partition_rejected`,
  `lua_no_side_effects`, `lua_deterministic` (two engines, same state, same result),
  `C11_clock_and_random_unavailable`, `lua_type_conversion_table`.
- **Model:** Opus. **Size:** medium.

### T16 - P1 acceptance

- **Goal:** the single-node part of spec 9 as a test, with an unmodified client.
- **Deliverables:** Jedis in test scope of the server module only; one acceptance class that
  starts a node on an ephemeral port and runs: `SET foo bar EX 60`, `GET`, the leaderboard
  `ZADD` and `ZRANGE WITHSCORES`, the counter `EVAL`, a `SCAN` loop over 1,000 keys, a
  `MULTI/EXEC`, and a TTL that fires through the real scheduler (the one place the
  acceptance tier may await real time, with a deadline, never a fixed sleep); a `redis-cli`
  transcript recorded in `progress.md`.
- **Blocked by:** T04, T05, T07, T11, T15.
- **Fixed contracts:** C8 (Jedis unmodified); spec 9.
- **Acceptance:** `P1_acceptance_redis_client_unmodified`; every test from T02 to T15 still
  green in the same run.
- **Model:** Opus. **Size:** small.

---

## P1 Exit Criteria

All spec 6.1, 6.1b, 6.2, 6.3, 6.4, 6.6, 6.9 tests green; `redis-cli` transcript in
`progress.md`; no module imports a banned dependency; the engine module has no dependency but
kotlin-stdlib.
