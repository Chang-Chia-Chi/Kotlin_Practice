# 45: Raft snapshots, chaos, linearizability

**What to build:** A `RaftStore` adapter on the filesystem and state machine snapshot install
and restore for every state machine; a seeded chaos driver over the CP test kit (leader kills,
follower kills, partitions of the MicroRaft in-memory transport, restarts from snapshot plus
log suffix); a small linearizability checker over recorded histories (search over
permutations for histories of a few dozen operations); the four CP spec 10.9 tests.

**Blocked by:** 41 (Sessions), 42 (Semaphore, latch, reference), 44 (Dispatcher and compat routing)

**Nature:** invariants under chaos, C20, I13 to I17, I20 (Fable)

**Status:** ready-for-agent

- [ ] `cp_snapshot_restore_roundtrip`, `I20_restore_equals_continuous_replay`
- [ ] `invariant_fencing_token_monotonic_under_chaos`, `invariant_mutual_exclusion_under_chaos`, `invariant_session_release_complete`, `invariant_linearizable_ops`
- [ ] `I16_minority_kill_keeps_cp_available`, `I17_majority_kill_never_false_succeeds`
- [ ] Five seeds green in the default tier
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec-cp.md, the plan is docs/dynamiccache/plan.md and this ticket's
entry is docs/dynamiccache/plans/p5-cp-subsystem.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only DynaCache/ and,
when a measurement forces it, docs/dynamiccache/.
