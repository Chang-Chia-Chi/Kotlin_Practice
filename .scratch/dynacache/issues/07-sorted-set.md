# 07: Sorted Set commands

**What to build:** The Sorted Set type on the dual index: member to score in the ticket 05
table, order in the ticket 06 skip list, both updated together; `ZADD`, `ZREM`, `ZRANGE`,
`ZREVRANGE`, `ZRANGEBYSCORE`, `ZRANK`, `ZREVRANK`, `ZSCORE`, `ZCARD`, `ZINCRBY` with
`WITHSCORES` where Redis has it, and `ZSCAN`.

**Blocked by:** 05 (Hash table and SCAN), 06 (Skip list)

**Nature:** command semantics on a dual index (Opus)

**Status:** ready-for-agent

- [ ] `zset_ordering_invariant` (seeded ZADD and ZREM storm), `zset_rank_consistency`, `zset_score_update`
- [ ] `I3_zrange_sorted_with_lex_tiebreak`
- [ ] `zscan_returns_all_members`
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
