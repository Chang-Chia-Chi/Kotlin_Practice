# 08: Hierarchical timer wheel

**What to build:** A three-level timer wheel as a standalone structure: `schedule(key,
deadline)`, `cancel(key)`, `reschedule`, and `advanceTo(instant)` that fires every due entry in
deadline order and cascades lower levels; O(1) schedule and cancel; no thread and no clock
read inside the wheel. Ticket 09 wires it to the TTL commands.

**Blocked by:** 01 (Skeleton)

**Nature:** timing invariants, C7 and I7 (Fable)

**Status:** ready-for-agent

- [ ] `wheel_fires_on_time`, `wheel_no_early_fire`, `wheel_cancel_prevents_fire`, `wheel_replace_ttl`, `wheel_ordering`
- [ ] `wheel_high_volume`: 1,000,000 seeded deadlines, each fires within one tick of its deadline
- [ ] `I7_fire_order_never_inverts`, `C7_never_fires_before_deadline`
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p1-data-engine.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/.
