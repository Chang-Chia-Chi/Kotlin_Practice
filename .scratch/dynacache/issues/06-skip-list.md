# 06: Skip list

**What to build:** The ordered index behind Sorted Set: a skip list keyed by (score, member
bytes) with seeded level generation, insert, delete, score update, range by score, range by
rank, rank of member, forward and reverse traversal. A pure data structure with no engine
wiring; ticket 07 wires it.

**Blocked by:** 01 (Skeleton)

**Nature:** data-structure craft with a sequential spec (Opus)

**Status:** ready-for-agent

- [ ] `skiplist_insert_order`, `skiplist_delete_preserves_order`, `skiplist_range_query`, `skiplist_rank_correct`, `skiplist_duplicate_score_lex_order`
- [ ] `skiplist_log_n_property`: 100,000 inserts, average comparisons per search at most 2 log2 N
- [ ] Level generation takes an injected seed so the log-n test is reproducible
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
