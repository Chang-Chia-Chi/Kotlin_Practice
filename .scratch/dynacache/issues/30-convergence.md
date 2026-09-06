# 30: Convergence and minority-crash safety

**What to build:** The I1 checker in the test kit (heal, drain, one full anti-entropy cycle,
read every replica of every key, assert equal value and DVV); a seeded chaos driver over
`InProcessCluster` (random writes on random nodes, partitions, kills, restarts, heals); and
`convergence_after_partition` plus I2 on top of it.

**Blocked by:** 24 (P2 acceptance), 25 (Hinted handoff), 26 (Read repair), 28 (Anti-entropy), 29 (Merge rules)

**Nature:** invariant checkers under chaos, I1 and I2 (Fable)

**Status:** done (DynaCache 02a0a53 + merge 6a48f3a, merged into misc/ai_gen)

- [x] `convergence_after_partition`
- [x] `I1_all_replicas_equal_after_heal_drain_sync` over five seeds
- [x] `I2_minority_crash_loses_no_acked_write`: kill fewer than N-W+1 nodes, every acked key readable at quorum R
- [x] Progress entry appended

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
