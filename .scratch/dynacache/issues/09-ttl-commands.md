# 09: TTL commands and active expiry

**What to build:** `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `TTL`, `PTTL`, `PERSIST`; `SET EX/PX` and
every TTL command schedule the key on its partition's wheel, a re-`EXPIRE` cancels and
reschedules, `PERSIST` cancels, the lazy check on access stays, expired keys are absent from
`KEYS`, `SCAN`, `DBSIZE` and `RANDOMKEY`, TTLs are stored as absolute instants, and the server
owns one scheduler coroutine per partition that calls `advanceTo(clock.instant())` on the
partition's executor.

**Blocked by:** 02 (Engine walking skeleton), 08 (Timer wheel)

**Nature:** expiry semantics, spec 5.4 (Opus)

**Status:** ready-for-agent

- [ ] `expire_replaces_wheel_entry`, `persist_cancels_expiry`, `expireat_absolute`, `ttl_reports_remaining_and_minus_values`
- [ ] `C7_key_readable_until_deadline_then_absent`: readable at deadline minus one ms, absent after deadline plus one tick
- [ ] `string_set_ex_expires` now runs through the wheel path
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
