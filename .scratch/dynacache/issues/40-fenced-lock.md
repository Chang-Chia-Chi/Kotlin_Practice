# 40: FencedLock

**What to build:** `FencedLockStateMachine`: TRY with a lease TTL returning a strictly
increasing token per key, reentrance by the same session with a hold count, UNLOCK checked
against session and token, RENEW by the holder only, FORCE_UNLOCK, STATE; lease expiry on log
time; the token counter is state-machine state so it survives leader change.

**Blocked by:** 39 (Log-carried time)

**Nature:** mutual exclusion and monotonic tokens, C17, I13, I14, I18, I19 (Fable)

**Status:** ready-for-agent

- [ ] CP spec 10.1 all ten tests, `lock_try_acquire_release_roundtrip` to `lock_force_unlock_overrides`
- [ ] `cp_leader_failover_preserves_state`, `I18_lock_held_across_leader_failover`
- [ ] `I19_lease_expires_late_never_early_across_failover`
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
