# 48: WAL logs nothing for a refused conditional ZADD

**What to build:** A warm restart preserves a sorted set exactly. Today a `ZADD NX` or
`ZADD XX` that changed nothing still answers `:0`, is logged, and is replayed on recovery
without its condition, so the score moves on restart. After this ticket the WAL's "what is
logged is what changed" rule holds for sorted sets as it already does for a refused
conditional `SET`: a conditional `ZADD` that took nothing logs nothing, and one that took is
replayed with exactly the effect it had. The progress entry for the WAL (T35) is corrected
where it claims the rule already covers every conditional command.

**Blocked by:** None (can start immediately)

**Nature:** write-ahead rule, C14 (Opus)

**Status:** ready-for-agent

- [ ] `C14_refused_conditional_zadd_replays_nothing`: `ZADD NX` on an existing member and
      `ZADD XX` on a missing member, then recover from the WAL; every score is where it was
- [ ] `C14_taken_conditional_zadd_replays_as_taken`: a conditional `ZADD` that changed one of
      two members replays with that one change only
- [ ] A `ZADD` mixing taken and refused members in one call replays with only the taken ones
- [ ] The WAL codec's comment and the T35 progress wording no longer claim the rule covers
      every conditional command by reply shape alone
- [ ] Progress entry appended

A red test for this exists in the review worktree `kp-wt/review` under the engine module's
persist test tree (`BugHuntWalTest`); reuse it if the worktree is still present, otherwise
rewrite it from the first criterion. Ground rules for every ticket: implement only this
ticket; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK; no
sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time, `codebase-design` vocabulary for any
new interface, and a `code-review` self-pass before the commit; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's
entry is docs/dynamiccache/plans/p6-review-fixes.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only DynaCache/ and,
when a measurement forces it, docs/dynamiccache/.
