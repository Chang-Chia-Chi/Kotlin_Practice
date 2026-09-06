# 05: Hash table with incremental rehash, SCAN family

**What to build:** A hand-built open hash table that rehashes one bucket per operation across
two tables, replaces the key map of every partition store and the field map of Hash, and
supports reverse binary iteration; `SCAN` with `COUNT` and `MATCH` whose cursor encodes the
partition plus the inner cursor, and `HSCAN`. `ZSCAN` waits for ticket 07.

**Blocked by:** 03 (String completion and Hash)

**Nature:** iteration under mutation, C15 (Fable)

**Status:** done (DynaCache 72089fd + merge 71692db, merged into misc/ai_gen)

- [x] `scan_returns_all_keys`, `scan_cursor_zero_terminates`, `scan_match_filters`, `scan_during_rehash_no_miss`, `scan_may_duplicate`, `incremental_rehash_no_block`, `hashtable_put_get_remove`
- [x] `C15_scan_completeness`: a seeded insert and delete storm during a scan; every key present throughout is returned at least once
- [x] No single operation migrates more than one bucket
- [x] Progress entry appended

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
