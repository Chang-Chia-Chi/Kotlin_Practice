# 28: Anti-entropy sync

**What to build:** A per-node background step (`tick()`): choose a vnode range and a replica
peer, exchange roots, descend on mismatch, exchange the divergent keys with DVVs, and apply
the DVV rule of spec 5.3 on both sides (concurrent pairs are left in place until ticket 29
lands, then merged); interval configurable; one coroutine per node.

**Blocked by:** 22 (Replication and quorum), 27 (Merkle tree)

**Nature:** background convergence protocol (Fable)

**Status:** ready-for-agent

- [ ] `anti_entropy_heals_divergence`: one replica silently corrupted, one cycle, every replica equal
- [ ] `anti_entropy_step_is_bounded` (one range per step), `anti_entropy_noop_when_equal`
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
