# DynaCache P5 - CP Subsystem: Raft-backed primitives via MicroRaft (T38 to T46)

Companion to `../plan.md`. Spec: `../design-spec-cp.md` in full (C16 to C23, I13 to I22,
tests 10.1 to 10.9).

**Goal:** a three-member Raft group on the same nodes serves linearizable locks with fencing
tokens, counters, semaphores, latches and CAS on the `cp:*` namespace, through `CP.*` verbs
and the Redis-compat set. Minority failure stays available; majority failure blocks; leader
failover preserves every held lock; a dead client's resources are released.

**Architecture:** new module `dynacache-cp` between cluster and server. MicroRaft's own
`Transport`, `StateMachine` and `RaftStore` interfaces are the seams; in-memory adapters
drive every test, gRPC-backed ones arrive in T43. One Raft group, one `CpEngine` applying
`CpOp`s to five state machines plus the session registry. The AP engine is untouched; the
dispatcher of T44 is the only place both engines meet.

---

### T38 - CP module, MicroRaft runtime, AtomicLong

- **Goal:** CP spec 2, 3.2; C21; the first primitive end to end.
- **Deliverables:** `dynacache-cp` pom (MicroRaft 0.7; server depends on cp from now on);
  `CpConfig`; `RaftRuntime` forming a group from the configured CP members over MicroRaft's
  in-memory transport in tests; the `Command.Cp` sub-hierarchy in the engine module for the
  AtomicLong verbs; `CpEngine` presenting the `CommandEngine` shape (`submit(command)` returns
  a future completed when the entry is committed and applied); `AtomicLongStateMachine`
  with SET, GET, INCR, DECR, INCRBY, DECRBY, CAS; a three-member in-process CP test kit with
  `killMember`, `restartMember`, `leader()`.
- **Blocked by:** T18.
- **Fixed contracts:** C21; CP spec 3.2; plan 2.2.
- **Acceptance:** `long_set_get_roundtrip`, `long_incr_decr`, `long_cas_success`,
  `long_cas_failure`, `long_concurrent_incr_linearizable`, `cp_minority_failure_available`,
  `cp_majority_failure_unavailable`, `C21_success_implies_majority_commit`.
- **Model:** Opus. **Size:** large.

### T39 - Log-carried time and TTL ticks

- **Goal:** CP spec 5; C19; C23.
- **Deliverables:** the leader stamps every entry with `max(clock.now, lastCommittedTs + 1)`;
  `TTL_TICK` entries appended every tick interval when idle, driven by the injected clock;
  every state machine tracks `lastAppliedTs` and evaluates expiry against it, never the local
  clock; TTL on AtomicLong (`SET ... EX`, `EXPIRE`, `TTL`, `PERSIST` semantics of CP spec 9.4).
- **Blocked by:** T38.
- **Fixed contracts:** C19; C23; CP spec 5 and 9.4.
- **Acceptance:** `C19_log_timestamps_monotonic_across_leader_change` (leader killed with a
  clock ahead of the successor's), `C23_every_member_agrees_on_expiry_at_same_index`,
  `ttl_tick_advances_time_when_idle`, `long_ttl_expires`.
- **Model:** Fable. **Size:** medium.

### T40 - FencedLock

- **Goal:** CP spec 3.1, 6.1; C17; I13, I14, I18, I19.
- **Deliverables:** `FencedLockStateMachine`: TRY with lease TTL returning a strictly
  increasing token per key, reentrance by the same session with a hold count, UNLOCK checked
  against session and token, RENEW by the holder only, FORCE_UNLOCK, STATE; lease expiry on
  log time; the token counter survives leader change because it is state-machine state.
- **Blocked by:** T39.
- **Fixed contracts:** C17; I13; I14; I18; I19; CP spec 3.1.
- **Acceptance:** CP spec 10.1 all ten (`lock_try_acquire_release_roundtrip` to
  `lock_force_unlock_overrides`), `cp_leader_failover_preserves_state`,
  `I18_lock_held_across_leader_failover`, `I19_lease_expires_late_never_early_across_failover`.
- **Model:** Fable. **Size:** large.

### T41 - Sessions and session-tied release

- **Goal:** CP spec 4, 9.3; C18; I15.
- **Deliverables:** `SessionRegistry` state machine: SESSION_CREATE, HEARTBEAT, CLOSE;
  the leader checks session timeouts on every `TTL_TICK` and appends `SESSION_CLOSED`;
  applying `SESSION_CLOSED` releases every lock the session holds in the same entry (permits
  join in T42); a lock or permit op with an unknown session is `-NOSESSION`.
- **Blocked by:** T40.
- **Fixed contracts:** C18; I15; CP spec 4, 9.3.
- **Acceptance:** `session_create_heartbeat_close`, `session_timeout_closes`,
  `session_op_without_session_rejected`, `I15_no_lock_owned_after_session_closed_index`,
  `C18_release_is_one_log_entry`.
- **Model:** Fable. **Size:** medium.

### T42 - Semaphore, CountDownLatch, AtomicReference

- **Goal:** CP spec 3.3 to 3.5, 6.3 to 6.5; I21.
- **Deliverables:** `SemaphoreStateMachine` (INIT, ACQUIRE, RELEASE, AVAILABLE, DRAIN, permits
  owned per session and released by `SESSION_CLOSED`), `CountDownLatchStateMachine` (SET,
  DOWN, GET, RESET only at zero), `AtomicReferenceStateMachine` (SET, GET, CAS on byte
  equality, TTL through log time).
- **Blocked by:** T41.
- **Fixed contracts:** I21; CP spec 3.3 to 3.5.
- **Acceptance:** CP spec 10.3 all six, 10.4 all four, 10.5 all three,
  `I21_concurrent_cas_exactly_one_wins`.
- **Model:** Opus. **Size:** large.

### T43 - gRPC CpService, RaftService, leader forwarding

- **Goal:** CP spec 2.2, 2.4, 9.1 steps 2 and 3.
- **Deliverables:** `cp.proto` with `RaftService` (MicroRaft inter-member messages) and
  `CpService` (`Apply`, `GetInfo`, `Heartbeat`); a MicroRaft `Transport` adapter over gRPC;
  an AP-only node forwards `CpOp`s to the known leader and re-discovers it through `GetInfo`;
  a follower answers `-NOTLEADER <hint>`; `CP.INFO` data.
- **Blocked by:** T23, T38.
- **Fixed contracts:** CP spec 2.2, 2.4; plan 2.3 MicroRaft seams.
- **Acceptance:** `cp_non_leader_forwards`, `cp_notleader_hint_on_follower`,
  `raft_group_forms_over_grpc_on_localhost`, `cp_forwarding_rediscovers_leader_after_failover`.
- **Model:** Opus. **Size:** medium.

### T44 - Command dispatcher, Redis-compat routing, RESP verbs

- **Goal:** CP spec 6, 9.5; C16; C22; I22.
- **Deliverables:** `CommandDispatcher`, a concrete class with the `CommandEngine` shape that
  applies the three routing rules in order and submits to the AP router or the CP engine; the
  T13 parser extended with every `CP.*` verb of CP spec 6 as `Command.Cp` variants; the compat
  set on `cp:*` keys (`SET` with `NX`, `EX`, `PX`, `GET`, `DEL`, `EXISTS`, `INCR` family,
  `SETEX`, TTL commands, `TYPE`) handled by the CP engine on the ordinary `Command` variants;
  `-NOTCP` for every rejection; the Netty pipeline submits to the dispatcher; CP error kinds
  of CP spec 6.8 as `Reply.Error` kinds.
- **Blocked by:** T13, T42, T43.
- **Fixed contracts:** C16; C22; I22; CP spec 6, 9.5.
- **Acceptance:** CP spec 10.8 all five, `long_redis_compat_incr`,
  `I22_namespaces_never_cross`, `C16_ap_engine_never_sees_cp_key` (recording fake engine).
- **Model:** Opus. **Size:** large.

### T45 - Raft snapshots, chaos, linearizability

- **Goal:** I20; CP spec 10.9; I16 and I17 under chaos; C20.
- **Deliverables:** `RaftStore` adapter on the filesystem and state machine snapshot install
  and restore for every state machine; a seeded chaos driver over the CP test kit (leader
  kills, follower kills, partitions of the MicroRaft in-memory transport, restarts from
  snapshot plus log suffix); a small linearizability checker over recorded histories
  (search over permutations for histories of a few dozen operations); the four 10.9 tests.
- **Blocked by:** T41, T42, T44.
- **Fixed contracts:** C20; I13 to I17, I20; CP spec 10.7 `cp_snapshot_restore_roundtrip`.
- **Acceptance:** `cp_snapshot_restore_roundtrip`, `I20_restore_equals_continuous_replay`,
  `invariant_fencing_token_monotonic_under_chaos`, `invariant_mutual_exclusion_under_chaos`,
  `invariant_session_release_complete`, `invariant_linearizable_ops`,
  `I16_minority_kill_keeps_cp_available`, `I17_majority_kill_never_false_succeeds`.
- **Model:** Fable. **Size:** large.

### T46 - P5 acceptance

- **Goal:** both engines in one cluster with an unmodified client.
- **Deliverables:** the acceptance harness extended: `SET cp:counter:x 5 EX 10` and
  `INCR cp:counter:x` through Jedis, `CP.LOCK.TRY` through a raw RESP client (Jedis has no
  custom-verb API), kill the leader, the lock is still held with the same token, a second
  client cannot take it, session timeout releases it; the spec 9 AP demo still passes in the
  same run.
- **Blocked by:** T37, T45.
- **Fixed contracts:** CP spec 12 exit criteria; spec 9.
- **Acceptance:** `P5_acceptance_two_engines_one_cluster`; every test of P1 to P5 green.
- **Model:** Opus. **Size:** small.

---

## P5 Exit Criteria

All CP spec 10 tests green; the chaos driver runs five seeds green; the acceptance class of
T46 passes with the AP demo in the same JVM.

## Deferred out of P5

Blocking verbs (`CP.LATCH.AWAIT`), multiple Raft groups, dynamic CP membership, `CP.LOCK.LIST`
(CP spec 13).
