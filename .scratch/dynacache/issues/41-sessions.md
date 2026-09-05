# 41: Sessions and session-tied release

**What to build:** The `SessionRegistry` state machine with SESSION_CREATE, HEARTBEAT and
CLOSE; the leader checks session timeouts on every `TTL_TICK` and appends `SESSION_CLOSED`;
applying `SESSION_CLOSED` releases every lock the session holds in that same entry (permits
join in ticket 42); a lock or permit op with an unknown session is `-NOSESSION`.

**Blocked by:** 40 (FencedLock)

**Nature:** ephemeral ownership, C18 and I15 (Fable)

**Status:** ready-for-agent

- [ ] `session_create_heartbeat_close`, `session_timeout_closes`, `session_op_without_session_rejected`
- [ ] `I15_no_lock_owned_after_session_closed_index`
- [ ] `C18_release_is_one_log_entry`
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
