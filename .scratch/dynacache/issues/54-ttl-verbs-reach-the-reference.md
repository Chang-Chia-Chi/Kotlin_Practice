# 54: TTL verbs on cp:ref: keys reach the reference

**What to build:** `TTL`, `PTTL`, `EXPIRE`, `PEXPIRE` and `PERSIST` on a `cp:ref:` key act on
the reference, as `GET` and `SET` already do. Today the dispatcher re-targets those verbs onto
the counter's verbs for every `cp:` key, so a reference set with `EX` reports `-2` and cannot
have its TTL changed. After this ticket the compat re-target looks at the key's kind for the
TTL verbs the same way it does for `GET` and `SET`. Moving the whole namespace rule into one
place is ticket 71; this ticket fixes the visible bug only.

**Blocked by:** None (can start immediately)

**Nature:** routing, C16 (Opus)

**Status:** ready-for-agent

- [ ] `ref_ttl_via_compat_reports_reference_ttl`: `SET cp:ref:x v EX 10` then `TTL` and
      `PTTL` report the remaining lease on log time
- [ ] `ref_expire_and_persist_via_compat`: `EXPIRE` shortens, `PERSIST` removes, and a TTL
      tick past the deadline deletes the reference
- [ ] The counter path is unchanged: the same five verbs on a `cp:counter:` key pass their
      existing tests
- [ ] Progress entry appended

A red test for this exists in the review worktree `kp-wt/review` under the server module's
test tree (`BugHuntCpCompatTest`); reuse it if present, otherwise rewrite it from the first
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
