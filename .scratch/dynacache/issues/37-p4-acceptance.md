# 37: P4 acceptance

**What to build:** The spec 9 demo in full on the P2 acceptance harness: kill all three nodes
and restart from RDB plus WAL with data intact; trigger a Chandy-Lamport snapshot under Jedis
traffic, keep writing, restore the cluster from it, reads return snapshot-time values; TTLs
fire; the memory-pressure and W-TinyLFU line exercised with a small threshold.

**Blocked by:** 30 (Convergence), 35 (WAL in the write path), 36 (Chandy-Lamport)

**Nature:** acceptance (Opus)

**Status:** ready-for-agent

- [ ] `P4_acceptance_success_signal` green
- [ ] Every P1 to P4 test green in the same run
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p4-persistence.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/.
