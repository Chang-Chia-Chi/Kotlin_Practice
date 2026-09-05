# DynaCache P2 - Distribution: Ring, Transport, Gossip, DVVs, Replication (T17 to T24)

Companion to `../plan.md`. Spec: `../design-spec.md` sections 2.3 (cluster-facing), 2.4, 2.5
(structure only; merge rules are P3), 3 (C2, C3, C4), 4 (I4, I5, I8), 5.1 steps 1 to 6 and 8,
5.2 steps 1 to 4, 6.5, 6.7.

**Goal:** three nodes serve reads and writes through any node, with quorum R/W, gossip
membership, and a DVV on every value. Minority failure keeps the cluster available.

**Architecture:** everything lands in `dynacache-cluster` except the gRPC adapter, which
lands in the server. The `Transport` seam (T18) has two adapters from the day it exists: the
in-memory one drives every cluster test; gRPC is verified by a round-trip test and the
acceptance tier. The engine stays cluster-unaware.

---

### T17 - Hash ring and preference lists

- **Goal:** spec 2.4 consistent hashing; C3, I5.
- **Deliverables:** `NodeId`; ring of SHA-256 positions (hash tag aware, same rule as `Key`)
  with at least 128 vnodes per node; `preferenceList(key, n)` walking clockwise for N distinct
  physical nodes; `vnodeOf(key)` for Merkle ranges and anti-entropy; the ring decides
  placement only and never the engine's partition (two layers, CONTEXT.md, ADR 0001); ring
  built from a sorted node set and vnode count only.
- **Blocked by:** T01.
- **Fixed contracts:** C3; I5; spec 2.4 consistent hashing paragraph.
- **Acceptance:** `ring_determinism` (three independently built rings, identical preference
  lists for 10,000 keys), `I5_same_inputs_same_ring`, `C3_preference_list_has_n_distinct_nodes`,
  `ring_load_is_even` (100,000 keys, max over min node load below 1.25).
- **Model:** Opus. **Size:** small.

### T18 - Transport seam and in-process cluster test kit

- **Goal:** the one seam every distributed test crosses.
- **Deliverables:** `cluster.proto` with the `Envelope` message (a oneof that later tickets
  extend with their own messages) and the generated classes as the cluster's message model;
  `Transport` interface (`send`, `inbound`) over those classes; `InMemoryTransport` with
  `networkPartition(sides)`, `heal()`, `drop(rate, seed)`, `delay(range, seed)`, `kill(node)`,
  `restart(node)`, deterministic delivery order under `runTest`; `InProcessCluster(nodeCount,
  n, w, r)` wiring engine, ring and transport per node, with `drainMessages()`, `writeVia`,
  `readVia`, `readAllReplicas(key)`; a recording fake `CommandEngine` for router and
  dispatcher tests.
- **Blocked by:** T17.
- **Fixed contracts:** plan 2.3 `Transport` (protobuf is the model, no codec); plan 2.5.
- **Acceptance:** `transport_delivers_in_order_per_pair`, `network_partition_blocks_both_directions`,
  `heal_restores_delivery`, `kill_stops_delivery_and_restart_resumes`,
  `drop_is_reproducible_by_seed`, `cluster_boots_three_nodes_sharing_one_ring`.
- **Model:** Fable. **Size:** large.

### T19 - Request router

- **Goal:** spec 5.1 steps 1 to 3 and 5.2 step 1.
- **Deliverables:** contact node computes the coordinator from the preference list; local
  `engine.submit` when the contact is the coordinator, otherwise a `Forward` message carrying
  the command tokens and a `ForwardReply` carrying the `Reply`; the router presents the
  `CommandEngine` shape itself so the server's T13 pipeline submits to it unchanged;
  forwarded errors pass through unchanged.
- **Blocked by:** T13, T18.
- **Fixed contracts:** spec 5.1 steps 1 to 3.
- **Acceptance:** `router_executes_locally_when_coordinator`, `router_forwards_to_coordinator`,
  `router_forwarded_reply_identical_to_local`, `router_forward_timeout_is_an_error`.
- **Model:** Opus. **Size:** small.

### T20 - SWIM gossip membership

- **Goal:** spec 2.4 SWIM; I8; the `Membership` seam.
- **Deliverables:** protocol period as a step function (`tick()`), ping, ping-req through K
  intermediaries, suspect after RTT bound, dead after T rounds, refutation by incarnation
  number, membership piggybacked on every message, `Membership` view with alive, suspect, dead
  and a change flow; period, K, RTT and T configurable; a scripted `Membership` fake for
  replication tests.
- **Blocked by:** T18.
- **Fixed contracts:** spec 2.4 SWIM paragraph; I8; plan 2.5 (one coroutine, step-driven in tests).
- **Acceptance:** `gossip_detects_failure`, `gossip_detects_recovery`,
  `I8_membership_change_reaches_all_within_log_n_rounds` (5 and 7 nodes, seeded),
  `gossip_suspect_refuted_by_incarnation`, `gossip_ping_req_masks_one_lost_link`.
- **Model:** Fable. **Size:** large.

### T21 - Dotted Version Vectors

- **Goal:** spec 2.5 structure and 5.3 ordering; C2; I4.
- **Deliverables:** `Dvv(dot, context)`, `dominates`, `isConcurrent`, `merge` producing a
  descendant of both, `bump(nodeId)`; per-node counter that on restart resumes above the
  highest own counter found in local data; a compact wire encoding for the transport and,
  later, RDB.
- **Blocked by:** T17.
- **Fixed contracts:** C2; I4; spec 2.5 structure; spec 5.3 ordering rules (type merge is T29).
- **Acceptance:** `dvv_dominance_detection`, `dvv_concurrent_detection`,
  `dvv_merge_preserves_causality`, `dvv_bounded_size` (10,000 writes, 100 clients, 3 nodes,
  size at most 3), `dvv_no_counter_reuse` (restart resumes above), `I4_later_write_dominates`,
  `C2_counter_strictly_increases`.
- **Model:** Fable. **Size:** medium.

### T22 - Replication and quorum

- **Goal:** spec 5.1 steps 4 to 6 and 8, spec 5.2 steps 1 to 4; C4.
- **Deliverables:** coordinator write: bump DVV, apply locally, replicate to N-1 successors
  with a deadline, reply after W acks counting itself, error when W is unreachable (sloppy
  quorum arrives in T25); coordinator read: R replies, the dominating DVV wins, deterministic
  tiebreak (highest node id) when concurrent (repair arrives in T26); R + W > N validated at
  construction; TTL replicated as an absolute instant; every stored value carries its DVV.
- **Blocked by:** T19, T20, T21.
- **Fixed contracts:** C4; spec 5.1, 5.2 as scoped; plan 2.5 bounded fan-out.
- **Acceptance:** `write_read_quorum`, `minority_failure_available`,
  `majority_failure_unavailable`, `C4_write_needs_w_distinct_acks`,
  `C4_read_returns_highest_dvv`, `quorum_config_rejects_r_plus_w_not_above_n`.
- **Model:** Fable. **Size:** large.

### T23 - gRPC transport adapter

- **Goal:** spec 2.3 cluster-facing protocol; the second adapter of the `Transport` seam.
- **Deliverables:** a `ClusterService` with a `Deliver` RPC over the T18 `Envelope`;
  `GrpcTransport` implementing `Transport` with one channel per peer; the server module hosts
  the gRPC server next to Netty. No codec: the envelope goes on the wire as is.
- **Blocked by:** T18.
- **Fixed contracts:** plan 2.3 `Transport`; plan 2.2.
- **Acceptance:** `grpc_transport_roundtrip_every_message_type` (two transports on localhost
  ephemeral ports, same suite the in-memory adapter passes), `grpc_peer_down_is_a_send_error`.
- **Model:** Opus. **Size:** small.

### T24 - P2 acceptance

- **Goal:** the cluster half of spec 9 up to "kill node 2".
- **Deliverables:** an acceptance harness that starts three server nodes in one JVM on
  ephemeral RESP and gRPC ports with real gossip; Jedis writes through node 1, reads through
  node 3; forwarding exercised by writing a key whose coordinator is not the contact; node 2
  stopped, reads and writes still succeed; `INFO` shows membership; gossip and quorum timing
  is the acceptance tier's only real time, awaited with deadlines.
- **Blocked by:** T16, T22, T23.
- **Fixed contracts:** spec 9; C8.
- **Acceptance:** `P2_acceptance_three_nodes_quorum_and_minority_failure`; every P1 and P2
  test green in the same run.
- **Model:** Opus. **Size:** small.

---

## P2 Exit Criteria

Spec 6.5 and the 6.7 tests `ring_determinism`, `gossip_detects_failure`,
`gossip_detects_recovery`, `write_read_quorum`, `minority_failure_available`,
`majority_failure_unavailable` green; the in-memory and gRPC transports pass the same
message round-trip suite.
