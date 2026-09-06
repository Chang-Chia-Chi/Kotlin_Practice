# 43: gRPC CpService, RaftService, leader forwarding

**What to build:** `cp.proto` with `RaftService` (MicroRaft inter-member messages) and
`CpService` (`Apply`, `GetInfo`, `Heartbeat`); a MicroRaft `Transport` adapter over gRPC; an
AP-only node forwards `CpOp`s to the known leader and re-discovers it through `GetInfo`; a
follower answers `-NOTLEADER <hint>`; the data behind `CP.INFO`.

**Blocked by:** 23 (gRPC transport), 38 (CP module, MicroRaft runtime, AtomicLong)

**Nature:** technology adapter and forwarding (Opus)

**Status:** done (DynaCache dfeca3a, merged into misc/ai_gen)

- [x] `cp_non_leader_forwards`, `cp_notleader_hint_on_follower`
- [x] `raft_group_forms_over_grpc_on_localhost`
- [x] `cp_forwarding_rediscovers_leader_after_failover`
- [x] Progress entry appended

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
