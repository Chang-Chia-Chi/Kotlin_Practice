# DynaCache P3 - Fault Tolerance: Handoff, Repair, Anti-Entropy, Convergence (T25 to T30)

Companion to `../plan.md`. Spec: `../design-spec.md` sections 2.4 (sloppy quorum,
anti-entropy), 2.5 (merge table), 3 (C5, C6), 4 (I1, I2, I9), 5.1 step 7, 5.2 step 5, 5.3,
6.7 remaining tests.

**Goal:** partition the cluster, write on both sides, heal, and every replica converges to
one merged value. A node that was away receives everything it missed.

**Architecture:** all in `dynacache-cluster`, on the T18 test kit. No new seams: hints,
Merkle trees, anti-entropy and merge rules are concrete classes behind the replication
manager of T22. T30 adds the I1 convergence checker to the test kit.

---

### T25 - Hinted handoff and sloppy quorum

- **Goal:** spec 5.1 step 7; C5; I9.
- **Deliverables:** `HintStore` per node holding (target node, key, value, DVV, absolute TTL);
  the coordinator, when a preference-list node is dead per `Membership`, sends the write to the
  next healthy node on the ring, which stores a hint and acks toward W; on a `Membership`
  alive event the holder replays hints to the target as ordinary replication writes and
  deletes each on ack; replay is one bounded coroutine per node.
- **Blocked by:** T22.
- **Fixed contracts:** C5; I9; spec 2.4 sloppy quorum; plan 2.5.
- **Acceptance:** `hinted_handoff_replays`, `I9_rejoined_node_matches_reference_replica`
  (K keys written during the partition, compared key by key with DVVs),
  `C5_hint_carries_full_write` (value, DVV and TTL identical after replay),
  `sloppy_quorum_reaches_w_with_one_dead_node`, `hint_deleted_after_ack`.
- **Model:** Fable. **Size:** medium.

### T26 - Read repair

- **Goal:** spec 5.2 step 5.
- **Deliverables:** after a quorum read, replicas whose DVV is dominated receive the winning
  value and DVV asynchronously; concurrent siblings are left for T29; the repair fan-out is
  bounded and does not delay the client reply.
- **Blocked by:** T22.
- **Fixed contracts:** spec 5.2 step 5; plan 2.5.
- **Acceptance:** `read_repair_fixes_stale` (one replica forced stale, a read, then all
  replicas equal), `read_repair_does_not_delay_reply`, `read_repair_skips_concurrent_siblings`.
- **Model:** Fable. **Size:** small.

### T27 - Merkle tree per vnode range

- **Goal:** spec 2.4 anti-entropy's data structure; C6.
- **Deliverables:** `MerkleTree` built from the (key, value hash, DVV) triples of one vnode
  range in key order, fixed fan-out, root and level hashes, `diff(other)` yielding the
  divergent leaf ranges and their keys; deterministic across nodes with identical data.
- **Blocked by:** T21.
- **Fixed contracts:** C6.
- **Acceptance:** `C6_identical_data_identical_root`, `merkle_one_changed_key_changes_root`,
  `merkle_diff_names_only_divergent_ranges`, `merkle_empty_range_has_stable_root`.
- **Model:** Opus. **Size:** small.

### T28 - Anti-entropy sync

- **Goal:** spec 2.4 anti-entropy.
- **Deliverables:** a per-node background step (`tick()`): choose a vnode range and a replica
  peer, exchange roots, descend on mismatch, exchange the divergent keys with DVVs, and apply
  the DVV rule of spec 5.3 on both sides (type merge from T29 when concurrent; until T29 lands,
  concurrent pairs are left in place); interval configurable, one coroutine per node.
- **Blocked by:** T22, T27.
- **Fixed contracts:** spec 2.4 anti-entropy; plan 2.5.
- **Acceptance:** `anti_entropy_heals_divergence` (one replica silently corrupted, one cycle,
  all equal), `anti_entropy_step_is_bounded` (one range per step), `anti_entropy_noop_when_equal`.
- **Model:** Fable. **Size:** medium.

### T29 - Type-specific merge rules

- **Goal:** spec 2.5 table and 5.3 concurrent branch.
- **Deliverables:** `merge(local, remote)` per type: String last-writer by DVV with highest
  node id tiebreak; Hash per field; List union of concurrent appends and last-writer for pops;
  Sorted Set union of adds with the maximum score; the merged value carries a DVV descending
  from both; the replication manager, read repair and anti-entropy call it.
- **Blocked by:** T07, T21.
- **Fixed contracts:** spec 2.5 table; spec 5.3.
- **Acceptance:** `merge_string_concurrent_tiebreak_highest_node`, `merge_hash_field_level`,
  `merge_list_union_of_concurrent_appends`, `merge_zset_union_max_score`,
  `merge_is_commutative_associative_idempotent` (seeded triples per type),
  `merge_result_dvv_descends_from_both`.
- **Model:** Fable. **Size:** medium.

### T30 - Convergence and minority-crash safety

- **Goal:** I1 and I2 as executable checkers.
- **Deliverables:** the I1 checker in the test kit: heal, drain, one full anti-entropy cycle,
  read every replica of every key, assert equal value and DVV; a seeded chaos driver over
  `InProcessCluster` (random writes on random nodes, partitions, kills, restarts, heals);
  `convergence_after_partition` and I2 on top of it.
- **Blocked by:** T24, T25, T26, T28, T29.
- **Fixed contracts:** I1; I2.
- **Acceptance:** `convergence_after_partition`, `I1_all_replicas_equal_after_heal_drain_sync`
  (five seeds), `I2_minority_crash_loses_no_acked_write` (kill fewer than N-W+1 nodes, every
  acked key readable at quorum R).
- **Model:** Fable. **Size:** medium.

---

## P3 Exit Criteria

All spec 6.7 tests green; the chaos driver runs five seeds green in the default tier.
