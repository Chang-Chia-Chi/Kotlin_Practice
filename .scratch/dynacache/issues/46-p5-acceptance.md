# 46: P5 acceptance

**What to build:** The acceptance harness extended so both engines run in one three-node
cluster: `SET cp:counter:x 5 EX 10` and `INCR cp:counter:x` through Jedis, `CP.LOCK.TRY`
through the raw RESP client, kill the leader and the lock is still held with the same token,
a second client cannot take it, session timeout releases it; the spec 9 AP demo still passes
in the same run.

**Blocked by:** 37 (P4 acceptance), 45 (Raft snapshots, chaos, linearizability)

**Nature:** acceptance (Opus)

**Status:** ready-for-agent

- [ ] `P5_acceptance_two_engines_one_cluster` green
- [ ] Every test of P1 to P5 green in the same run
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
