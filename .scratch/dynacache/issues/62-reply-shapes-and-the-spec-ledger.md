# 62: Reply shapes and the spec ledger

**What to build:** Every remaining small gap between the code and the specs is either fixed
or recorded, so the next reader finds no unrecorded divergence. Fix: `-NOTLEADER` carries the
leader's id as its bare hint (today the hint reads `leader is node-1`, so a client taking the
first token reads `leader`); `LOCK_UNLOCK` answers per CP spec 3.1 and 6.1, with `:1` for an
accepted unlock including a reentrant decrement that still holds, and an error for a
rejection, so `:0` no longer has a meaning the spec never gave it; the dead `LongDecrBy`
variant, produced by nothing but the wire decoder, is deleted; `EXAT`, `PXAT` and `PSETEX`,
which no spec line asks for, are deleted unless a forward or the test kit depends on them
(`PEXPIREAT` stays as the forward's carrier and is recorded as such). Record, as deviations in
the progress log: `-CAPACITY` is not built; a fanned command inside a batch is refused even
when all its keys share the declared partition (a divergence from Redis for hash-tagged
keys); the reentrancy reply shape if the spec is ambiguous. Add the six missing constraint
and invariant test names (`C17_`, `C20_`, `C22_`, `I10_`, `I13_`, `I14_`) as tests that assert
the constraint, delegating to the spec-named tests that already cover the behaviour.

**Blocked by:** None (can start immediately)

**Nature:** command semantics and the deviation ledger (Opus)

**Status:** ready-for-agent

- [ ] `notleader_hint_is_the_leader_id`: the token after the kind parses as a member id
- [ ] `lock_unlock_reply_shape`: accepted unlock and accepted reentrant decrement both answer
      `:1`; a non-holder's unlock answers the spec's error
- [ ] `LongDecrBy` no longer exists; the wire decoder rejects its old tag with a clear error
- [ ] `EXAT`, `PXAT`, `PSETEX` are gone or their keeping is recorded with the reason
- [ ] `C17_`, `C20_`, `C22_`, `I10_`, `I13_`, `I14_` tests exist and pass
- [ ] The progress entry records `-CAPACITY` and the fanned-in-batch refusal as deviations
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200 to 600 lines including tests;
JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named
tests keep their names, constraint tests `C<n>_<description>`, invariant tests
`I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before green,
one slice at a time, `codebase-design` vocabulary for any new interface, and a `code-review`
self-pass before the commit; append a progress entry to docs/dynamiccache/progress.md
describing what was done and every deviation. The specs are docs/dynamiccache/design-spec.md
and design-spec-cp.md, the plan is docs/dynamiccache/plan.md and this ticket's entry is
docs/dynamiccache/plans/p6-review-fixes.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/.
