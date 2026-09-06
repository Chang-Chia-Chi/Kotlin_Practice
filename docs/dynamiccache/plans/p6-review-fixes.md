# DynaCache P6 - Review fixes and deepening (T48 to T73)

Companion to `../plan.md`. Source: the four-axis review of 2026-09-06 at adf9d349 (standards,
spec, architecture, bug hunt), whose seven confirmed bugs each came with a red test, parked
uncommitted in the review worktree `kp-wt/review` as `BugHunt*Test` classes. Specs:
`../design-spec.md` and `../design-spec-cp.md` in full; the glossary is `DynaCache/CONTEXT.md`;
the ADRs are `DynaCache/docs/adr/`.

**Goal:** close the confirmed bugs at their root cause, bring the code back inside the
documented standards, settle every unrecorded spec gap, then deepen the seams the review
found shallow, in the order the bugs point to: one command encoding, one owner of the
(value, version) pair, one inbound loop, primitives testable without Raft.

**Architecture:** no new module. The engine gains a total command codec (its WAL codec, widened)
and a versioned store beside it; the cluster loses its file I/O, its two injected parse
functions and its five transport wrappers; the CP module's primitives become self-contained
for tests and snapshots. Every ADR stands; ADR 0002 and 0003 each gain one clarifying line.

**Ticket DAG** (an arrow means "blocks"; everything not listed can start immediately):

```
48 -> 63 -> 64 -> 65 -> 66 -> 67        49 -> 55 -> 66        51 -> 67
                    65 -> 73             58 -> 59              54 -> 71 <- 61
                                         69 -> 70
Independent: 50 52 53 56 57 60 62 68 72
```

Frontier at start: 48, 49, 50, 51, 52, 53, 54, 56, 57, 58, 60, 61, 62, 68, 69, 72. Run the
four High bugs (48, 49, 50, 51) first; 68, 72 and 73 overlap files with the codec chain, so
under parallel agents schedule 68 after 64.

---

### T48 - WAL logs nothing for a refused conditional ZADD

- **Goal:** C14; spec 2.8. Bug 1 of the review: a no-op `ZADD NX/XX` answers `:0`, is logged
  without its condition, and moves the score on replay.
- **Deliverables:** the what-is-logged rule covers conditional `ZADD` (logs nothing when
  nothing changed, replays as taken otherwise); T35 wording corrected.
- **Blocked by:** none.
- **Fixed contracts:** C14; the WAL replays exactly the effect a command had.
- **Acceptance:** `C14_refused_conditional_zadd_replays_nothing`,
  `C14_taken_conditional_zadd_replays_as_taken`.
- **Model:** Opus. **Size:** small.

### T49 - A snapshot cuts state before it opens channels

- **Goal:** C10; I12; spec 2.8 steps 1 and 2. Bug 3: channel recording begins before the
  state cut, so an envelope applied in between is restored twice.
- **Deliverables:** order is cut, then open channels, then markers; the gRPC no-FIFO gap is
  recorded as a known limitation.
- **Blocked by:** none.
- **Fixed contracts:** C10; I12.
- **Acceptance:** `I12_write_during_the_cut_is_restored_once`,
  `C10_state_is_cut_before_any_channel_opens`, `chandy_lamport_consistent_cut`.
- **Model:** Fable. **Size:** small.

### T50 - The idle TTL tick runs on log time

- **Goal:** CP spec 5; C19; I19. Bug 4: after failover to a leader whose clock trails log
  time the idle tick's wall-clock gate stays shut, so no lease expires and no session lapses.
- **Deliverables:** the idle tick is due on the leader's own elapsed time since log time last
  moved; T39 wording corrected.
- **Blocked by:** none.
- **Fixed contracts:** C19; C23; I19; ticks every interval when idle.
- **Acceptance:** `I19_idle_ticks_continue_after_failover_to_a_trailing_clock`,
  `C17_lease_expires_after_skewed_failover`, `C18_session_lapses_after_skewed_failover`.
- **Model:** Fable. **Size:** small.

### T51 - A node's dot counter survives restart

- **Goal:** C2; I2. Bug 2: a restarted coordinator reuses its old dots; a replica holding the
  dot keeps its old value but still acks, so an acked write is lost.
- **Deliverables:** a persisted reserved ceiling for the counter (block reservation, persisted
  before use, through a persist adapter; the cluster module does no file I/O); start above it.
- **Blocked by:** none.
- **Fixed contracts:** C2; I2; plan 2.2.
- **Acceptance:** `C2_dot_counter_never_reuses_a_dot_across_restart`,
  `I2_acknowledged_write_survives_coordinator_restart`, `dvv_no_counter_reuse` across restart.
- **Model:** Fable. **Size:** medium.

### T52 - An invalid expiry answers -ERR, never drops the connection

- **Goal:** C8. Bug 7: overflowing or non-positive TTLs throw out of the parser and the
  pipeline closes the socket, or are accepted.
- **Deliverables:** validation before arithmetic on every expiry-taking command; Redis's
  `invalid expire time` error; fuzz test extended.
- **Blocked by:** none.
- **Fixed contracts:** C8; a protocol error is a reply, never a disconnect.
- **Acceptance:** `C8_invalid_expire_answers_err_not_disconnect`.
- **Model:** Opus. **Size:** small.

### T53 - A connection's session cache clears on CLOSE

- **Goal:** CP spec 4. Bug 5: the handler memoises the first session forever, so CREATE after
  CLOSE returns the closed id and the connection is stuck at `-NOSESSION`.
- **Deliverables:** the cache clears on CLOSE of that session and on a `-NOSESSION` reply.
- **Blocked by:** none.
- **Fixed contracts:** CP spec 4 session lifecycle; 6.8 `-NOSESSION`.
- **Acceptance:** `session_create_after_close_returns_a_new_session`,
  `session_verbs_after_close_use_the_new_session`, `session_lapse_clears_the_cache`.
- **Model:** Opus. **Size:** small.

### T54 - TTL verbs on cp:ref: keys reach the reference

- **Goal:** C16; CP spec 9.4. Bug 6: `TTL`/`EXPIRE`/`PERSIST` on a reference are re-targeted
  onto the counter, so a reference with a TTL reads `-2`.
- **Deliverables:** the compat re-target branches on the key's kind for the TTL verbs.
- **Blocked by:** none.
- **Fixed contracts:** C16; CP spec 9.4 and 9.5.
- **Acceptance:** `ref_ttl_via_compat_reports_reference_ttl`, `ref_expire_and_persist_via_compat`.
- **Model:** Opus. **Size:** small.

### T55 - A snapshot-set part is a persist adapter

- **Goal:** plan 2.2 (no `java.nio.file` in the cluster); C10. The standards review's High:
  the cluster writes snapshot parts itself with a delimited-protobuf loop that has no crc or
  torn-tail recovery.
- **Deliverables:** an adapter in the engine's persist package writes and reads a part; each
  channel log is a WAL; the cluster receives an append/replay pair.
- **Blocked by:** T49.
- **Fixed contracts:** plan 2.2; C10; snapshot set restored as a whole (I12).
- **Acceptance:** no `java.nio.file` under the cluster's main sources;
  `snapshot_part_with_torn_channel_log_replays_the_complete_prefix`; existing P4 tests.
- **Model:** Opus. **Size:** medium.

### T56 - Settle the batch cross-partition error

- **Goal:** C12; ADR 0002; CONTEXT.md. Decision taken in the review: keep the `CROSSSLOT` kind
  (clients switch on it; ADR 0002 rejected it for fan-out, not batches), reword the message
  in glossary words, add one line to ADR 0002.
- **Deliverables:** `-CROSSSLOT keys of a batch must share a partition (use a hash tag)`;
  three pinned tests updated; ADR line.
- **Blocked by:** none.
- **Fixed contracts:** C12; the kind `CROSSSLOT`.
- **Acceptance:** the three pinned tests with the new wording; "slot" nowhere else.
- **Model:** Opus. **Size:** small.

### T57 - Glossary renames

- **Goal:** CONTEXT.md "Avoid" words out of code and tests, no behaviour change.
- **Deliverables:** batch not transaction; lease not ttl on the lock verbs; no "barrier" on the
  latch; CP replies not responses; wire-visible names kept and listed.
- **Blocked by:** none.
- **Fixed contracts:** CONTEXT.md.
- **Acceptance:** the search criteria in the ticket; test count unchanged.
- **Model:** Opus. **Size:** small.

### T58 - One MutableClock in an engine test-jar; drop the two ModuleGraphTests

- **Goal:** one clock double for every module's tests; prefactor for T59.
- **Deliverables:** engine test-jar with `MutableClock`; four copies and `RecordingClock`
  folded; both `ModuleGraphTest`s deleted.
- **Blocked by:** none.
- **Fixed contracts:** plan 1.5; plan 2.2 (test scope only).
- **Acceptance:** exactly one `MutableClock`; offline reactor build from clean; test count
  minus two.
- **Model:** Opus. **Size:** small.

### T59 - Acceptance tests run on the injected clock

- **Goal:** plan rule 1.5. Four acceptance tests spin on wall time and two build the engine
  on the system clock, unrecorded.
- **Deliverables:** clock advances replace the spins; any remaining thread wait recorded.
- **Blocked by:** T58.
- **Fixed contracts:** plan 1.5; rule 1.7 for what remains.
- **Acceptance:** no `Clock.systemUTC()` in tests; acceptance suites pass; wall time not up.
- **Model:** Opus. **Size:** small.

### T60 - CP.LONG.GETADD

- **Goal:** CP spec 3.2 and 6.2; deferred by T38, never picked up.
- **Deliverables:** the verb through parser, wire, engine and AtomicLong; `:old`.
- **Blocked by:** none.
- **Fixed contracts:** CP spec 3.2, 6.2; I21-style atomicity.
- **Acceptance:** `long_getadd_returns_old_value_and_adds`, `long_getadd_concurrent_linearizable`.
- **Model:** Opus. **Size:** small.

### T61 - SET NX and SET XX on cp: keys

- **Goal:** CP spec 1 (`SET NX PX` in scope) and 9.5. Today answered `-NOTCP`. Decision: the
  compat `SET NX/XX` re-targets to the conditional form of the kind's SET verb, condition and
  TTL applied in one entry, reply `+OK` or nil.
- **Deliverables:** conditional SET on the counter and the reference state machines; the
  compat re-target; `CP.*` syntax for the condition is free.
- **Blocked by:** none.
- **Fixed contracts:** C16; I21; CP spec 9.5.
- **Acceptance:** `compat_set_nx_on_ref_key_acquires_once`,
  `compat_set_nx_px_expires_on_log_time`, `compat_set_xx_on_missing_key_is_nil`,
  `compat_set_xx_on_present_key_replaces`, the same on a counter key.
- **Model:** Opus. **Size:** medium.

### T62 - Reply shapes and the spec ledger

- **Goal:** no unrecorded divergence left. Fix `-NOTLEADER` hint shape, `LOCK_UNLOCK` reply,
  delete dead `LongDecrBy` and the unrequested `EXAT`/`PXAT`/`PSETEX`; record `-CAPACITY` and
  the fanned-in-batch refusal; add the six missing `C`/`I` test names.
- **Deliverables:** as listed; progress entries for each recorded deviation.
- **Blocked by:** none.
- **Fixed contracts:** CP spec 3.1, 6.1, 6.8; plan rule 3.
- **Acceptance:** `notleader_hint_is_the_leader_id`, `lock_unlock_reply_shape`, `C17_`,
  `C20_`, `C22_`, `I10_`, `I13_`, `I14_`.
- **Model:** Opus. **Size:** small.

### T63 - The engine's command codec encodes every keyed command

- **Goal:** architecture candidate 1, expand step. One encoding of a keyed command, total and
  round-tripping; the what-changed decision becomes one named function.
- **Deliverables:** widened WAL codec; the what-changed function; WAL unchanged in behaviour.
- **Blocked by:** T48.
- **Fixed contracts:** C8; C14; ADR 0003.
- **Acceptance:** `command_codec_round_trips_every_keyed_variant` (exhaustive `when`),
  what-changed tests, `wal_reads_append_nothing`.
- **Model:** Opus. **Size:** medium.

### T64 - A forward carries codec bytes

- **Goal:** candidate 1, migrate step 1. The router forwards engine-codec bytes; its two
  injected functions go; the server's command-to-tokens encoder is deleted if unused.
- **Blocked by:** T63.
- **Fixed contracts:** spec 5.1 steps 1 to 3; ADR 0003.
- **Acceptance:** `forward_round_trips_every_keyed_variant`; existing router and P2 tests.
- **Model:** Opus. **Size:** small.

### T65 - A replicate carries codec bytes

- **Goal:** candidate 1, contract step. A replica replays exactly the logged entry; the
  hand-written what-changed copies in replication and the test kit's codec are deleted.
- **Blocked by:** T64.
- **Fixed contracts:** ADR 0003 (plus one line: bytes of the engine codec); C4; C5.
- **Acceptance:** `replica_applies_exactly_the_logged_entry`; every existing replication,
  hint, repair, anti-entropy, convergence and acceptance test unchanged.
- **Model:** Fable. **Size:** medium.

### T66 - A versioned store beside the engine

- **Goal:** candidate 2. One owner of the (value, version) pair and of spec 5.3; the read race
  found in the review gets its seam and test; the recording engine double goes.
- **Blocked by:** T55, T65.
- **Fixed contracts:** spec 2.5, 5.3; C2; ADR 0003's consequence.
- **Acceptance:** `read_never_pairs_a_value_with_another_installs_version`,
  `spec_5_3_decided_once`; existing repair, anti-entropy, convergence, snapshot tests.
- **Model:** Fable. **Size:** large (up to 800 lines).

### T67 - Versions survive restart

- **Goal:** the durable half of bug 2. RDB version slot filled, WAL entry carries the version
  as an opaque trailer, the store rebuilds on load, the counter derives from the table with
  T51's ceiling as the floor.
- **Blocked by:** T51, T66.
- **Fixed contracts:** C2; I2; spec 2.8 formats (versions bumped).
- **Acceptance:** `I2_versions_survive_restart`,
  `restarted_replica_answers_quorum_read_with_its_version`, `dvv_no_counter_reuse`.
- **Model:** Fable. **Size:** medium.

### T68 - One inbound loop, and a send-only transport

- **Goal:** candidate 3. The handler order and the drop-on-unreachable promise exist once;
  the five transport wrappers and SWIM's second inbound path go.
- **Blocked by:** none (after T64 under parallel agents).
- **Fixed contracts:** plan 2.3 Transport seam (two adapters); I8.
- **Acceptance:** `inbound_order_is_snapshots_forwards_replication_antientropy_gossip`,
  `unreachable_peer_is_a_drop_on_both_adapters`; existing transport, SWIM, router tests.
- **Model:** Opus. **Size:** medium.

### T69 - CP primitives tested at the state machine, without Raft

- **Goal:** candidate 4, test surface. Five suites drive the composite state machine with
  stamped operations; the kit stays for log-time, failover, snapshot and chaos.
- **Blocked by:** none.
- **Fixed contracts:** CP spec 10.1 to 10.5 test names; C18; I15.
- **Acceptance:** the five suites without a Raft member, under one second together; a direct
  session-close cascade test.
- **Model:** Opus. **Size:** medium.

### T70 - Each CP primitive owns its snapshot bytes

- **Goal:** candidate 4, snapshot ownership. Snapshot is a list of (primitive id, opaque
  bytes); the composite iterates; the wire codec carries blobs; encoding version bumped.
- **Blocked by:** T69.
- **Fixed contracts:** C17 across snapshots; CP spec 10.8.
- **Acceptance:** per-primitive round trips; `cp_snapshot_install_preserves_tokens_and_sessions`;
  old format rejected with a named error.
- **Model:** Opus. **Size:** medium.

### T71 - One home for the cp: namespace rule

- **Goal:** candidate 5. Key-to-kind and the compat set defined once beside the CP command
  hierarchy; the parser emits CP verbs directly; the dispatcher only routes.
- **Blocked by:** T54, T61.
- **Fixed contracts:** C16; C22; I22; CP spec 9.4, 9.5.
- **Acceptance:** `cp_kind_lookup_covers_every_prefix`, `compat_set_matches_cp_spec_9_5`;
  `I22_namespaces_never_cross`; kind-mismatch reply tested and recorded.
- **Model:** Opus. **Size:** medium.

### T72 - The partition's store is split from its command interpreter

- **Goal:** candidate 6. A kind-agnostic store owns entries, expiry, the wheel, accounting
  and the policy; the interpreter never touches used bytes.
- **Blocked by:** none.
- **Fixed contracts:** spec 2.7, 5.4, 5.5; I6; ADR 0001 (executor ownership unchanged).
- **Acceptance:** `store_used_bytes_equals_sum_of_entries_after_any_sequence`,
  `eviction_never_evicts_the_key_being_written`; existing engine tests unchanged.
- **Model:** Opus. **Size:** large (up to 800 lines).

### T73 - Narrow the command engine seam to submit

- **Goal:** candidate 7. The seam is `submit`; a batch is a capability of the AP engine; six
  dead implementations deleted; plan 2.3's seam table updated.
- **Blocked by:** T65.
- **Fixed contracts:** plan 2.3 (amended by this ticket); C1; I11.
- **Acceptance:** MULTI/EXEC, EVAL, Lua, P1 and P5 acceptance unchanged; no batch code outside
  the AP engine and the handler.
- **Model:** Opus. **Size:** small.
