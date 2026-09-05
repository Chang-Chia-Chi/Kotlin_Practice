# 29: Type-specific merge rules

**What to build:** `merge(local, remote)` per type for concurrent DVVs: String last-writer by
DVV with the highest node id as tiebreak, Hash per field, List union of concurrent appends and
last-writer for pops, Sorted Set union of adds with the maximum score; the merged value carries
a DVV descending from both; replication, read repair and anti-entropy call it.

**Blocked by:** 07 (Sorted Set), 21 (Dotted Version Vectors)

**Nature:** conflict resolution semantics, spec 2.5 and 5.3 (Fable)

**Status:** ready-for-agent

- [ ] `merge_string_concurrent_tiebreak_highest_node`, `merge_hash_field_level`, `merge_list_union_of_concurrent_appends`, `merge_zset_union_max_score`
- [ ] `merge_is_commutative_associative_idempotent` with seeded triples per type
- [ ] `merge_result_dvv_descends_from_both`
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p3-fault-tolerance.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only DynaCache/ and,
when a measurement forces it, docs/dynamiccache/.
