# 39: Log-carried time and TTL ticks

**What to build:** The leader stamps every entry with `max(clock.now, lastCommittedTs + 1)`;
`TTL_TICK` entries are appended every tick interval when idle, driven by the injected clock;
every state machine tracks `lastAppliedTs` and evaluates expiry against it, never the local
clock; TTL on AtomicLong with the `SET ... EX`, `EXPIRE`, `TTL` and `PERSIST` semantics of CP
spec 9.4.

**Blocked by:** 38 (CP module, MicroRaft runtime, AtomicLong)

**Nature:** monotonic time across leader changes, C19 and C23 (Fable)

**Status:** ready-for-agent

- [ ] `C19_log_timestamps_monotonic_across_leader_change`: the leader killed with a clock ahead of its successor's
- [ ] `C23_every_member_agrees_on_expiry_at_same_index`
- [ ] `ttl_tick_advances_time_when_idle`, `long_ttl_expires`
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
