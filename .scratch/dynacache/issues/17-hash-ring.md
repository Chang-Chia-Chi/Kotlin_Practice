# 17: Hash ring and preference lists

**What to build:** `NodeId` and a ring of SHA-256 positions (hash tag aware, the same rule as
`Key`) with at least 128 vnodes per node; `preferenceList(key, n)` walks clockwise to N
distinct physical nodes; `vnodeOf(key)` for Merkle ranges and anti-entropy. The ring decides
placement only; the engine's partition is a separate layer it never touches (CONTEXT.md, ADR
0001). The ring is a pure function of the sorted node set and the vnode count.

**Blocked by:** 01 (Skeleton)

**Nature:** deterministic structure, C3 and I5 (Opus)

**Status:** done (DynaCache 8082a2e, merged c420b51)

- [x] `ring_determinism`: three independently built rings, identical preference lists for 10,000 keys
- [x] `I5_same_inputs_same_ring`, `C3_preference_list_has_n_distinct_nodes`
- [x] `ring_load_is_even`: 100,000 keys, max over min node load below 1.25
- [x] `ring_hash_tag_places_keys_together`
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p2-distribution.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/. DynaCache/ is its own git repository; commit code
there and docs in the parent.
