# 38: CP module, MicroRaft runtime, AtomicLong

**What to build:** The `dynacache-cp` module (MicroRaft 0.7; the server depends on cp from
now on); `CpConfig`; `RaftRuntime` forming a group from the configured CP members over
MicroRaft's in-memory transport in tests; the `Command.Cp` sub-hierarchy in the engine module
for the AtomicLong verbs; `CpEngine` presenting the `CommandEngine` shape (`submit(command)`
returns a future completed when the entry is committed and applied, replies are ordinary
`Reply` values with CP error kinds); `AtomicLongStateMachine` with SET, GET, INCR, DECR, INCRBY,
DECRBY and CAS; a three-member in-process CP test kit with `killMember`, `restartMember` and
`leader()`.

**Blocked by:** 18 (Transport seam and test kit)

**Nature:** library integration and the first primitive (Opus)

**Status:** ready-for-agent

- [ ] `long_set_get_roundtrip`, `long_incr_decr`, `long_cas_success`, `long_cas_failure`, `long_concurrent_incr_linearizable`
- [ ] `cp_minority_failure_available`, `cp_majority_failure_unavailable`
- [ ] `C21_success_implies_majority_commit`
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
when a measurement forces it, docs/dynamiccache/. DynaCache/ is its own git repository; commit
code there and docs in the parent.
