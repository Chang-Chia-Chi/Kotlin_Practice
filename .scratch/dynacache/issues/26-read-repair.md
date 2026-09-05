# 26: Read repair

**What to build:** After a quorum read, every replica whose DVV is dominated receives the
winning value and DVV asynchronously; concurrent siblings are left alone for ticket 29; the
repair fan-out is bounded and never delays the client reply.

**Blocked by:** 22 (Replication and quorum)

**Nature:** asynchronous convergence step, spec 5.2 step 5 (Fable)

**Status:** ready-for-agent

- [ ] `read_repair_fixes_stale`: one replica forced stale, a read, then every replica equal
- [ ] `read_repair_does_not_delay_reply`, `read_repair_skips_concurrent_siblings`
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p3-fault-tolerance.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only DynaCache/ and,
when a measurement forces it, docs/dynamiccache/.
