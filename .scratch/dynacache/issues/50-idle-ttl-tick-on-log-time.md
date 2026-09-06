# 50: The idle TTL tick runs on log time

**What to build:** Leases and sessions keep expiring after a leader change, whatever the new
leader's wall clock says. Today the idle tick is gated on the leader's wall clock having
passed the last stamp plus one interval; after failover to a leader whose clock trails log
time the first tick stamps log time plus one millisecond and the gate then stays shut until
the wall clock catches up, so no lease expires and no session lapses for the whole skew,
and a dead lock holder keeps its lock. After this ticket the idle tick is due when log time
has not advanced for one interval of the leader's own elapsed time, so ticks keep appending
and log time keeps moving at the tick interval even when the leader's clock is behind.

**Blocked by:** None (can start immediately)

**Nature:** monotonic time across leader changes, C19, I19 (Fable)

**Status:** done (DynaCache d18427fe, merged into misc/ai_gen)

- [x] `I19_idle_ticks_continue_after_failover_to_a_trailing_clock`: the leader's clock is
      ahead of the followers' by 30 s, entries bring log time to the leader's now, the leader
      is killed, the new leader's clock is advanced ten intervals with no user writes, and ten
      ticks are committed with log time strictly increasing
- [x] `C17_lease_expires_after_skewed_failover`: a lock taken with a lease shorter than the
      skew is released by a tick on the new leader without any user command
- [x] `C18_session_lapses_after_skewed_failover`: same for a session whose heartbeat stops
- [x] `I19_lease_expires_late_never_early_across_failover` and every existing CP test pass;
      log time never moves backwards (C23)
- [x] The T39 progress wording ("1 ms per entry advance") is corrected to describe the fix
- [x] Progress entry appended

A red test for this exists in the review worktree `kp-wt/review` under the cp module's test
tree (`BugHuntTtlTickTest`); reuse it if present, otherwise rewrite it from the first
criterion. Ground rules for every ticket: implement only this ticket; 200 to 600 lines
including tests; JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected
Clock; spec-named tests keep their names, constraint tests `C<n>_<description>`, invariant
tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before
green, one slice at a time, `codebase-design` vocabulary for any new interface, and a
`code-review` self-pass before the commit; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec-cp.md, the plan is docs/dynamiccache/plan.md and this ticket's
entry is docs/dynamiccache/plans/p6-review-fixes.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only DynaCache/ and,
when a measurement forces it, docs/dynamiccache/.
