# 69: CP primitives tested at the state machine, without Raft

**What to build:** The five primitive suites (lock, session, semaphore, latch, reference)
drive the composite state machine directly with a stamped operation and no Raft group, and
finish in milliseconds. Today every primitive test spins three MicroRaft members and waits
for an election with a ten-second deadline, although each primitive is a pure function of
(command, log time). After this ticket the state machine's run-operation entry point is the
test surface for primitive semantics, the CP test kit stays for what needs a log (log-carried
time, failover, snapshot install, chaos), and the spec-named tests keep their names.

**Blocked by:** None (can start immediately)

**Nature:** the interface is the test surface (Opus)

**Status:** ready-for-agent

- [ ] The five primitive suites construct a state machine and call it with stamped operations;
      none of them starts a Raft member
- [ ] Every spec-named primitive test (CP spec 10.1 to 10.5) keeps its name and passes; the
      session-close cascade (C18, I15) has a direct test through the state machine
- [ ] The five suites together run in under one second; the kit-backed suites are unchanged
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200 to 600 lines including tests;
JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named
tests keep their names, constraint tests `C<n>_<description>`, invariant tests
`I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before green,
one slice at a time, `codebase-design` vocabulary for any new interface, and a `code-review`
self-pass before the commit; append a progress entry to docs/dynamiccache/progress.md
describing what was done and every deviation. The spec is docs/dynamiccache/design-spec-cp.md,
the plan is docs/dynamiccache/plan.md and this ticket's entry is
docs/dynamiccache/plans/p6-review-fixes.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/.
