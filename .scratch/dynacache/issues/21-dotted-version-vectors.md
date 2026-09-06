# 21: Dotted Version Vectors

**What to build:** `Dvv(dot, context)` with `dominates`, `isConcurrent`, `merge` (a descendant
of both) and `bump(nodeId)`; a per-node counter that on restart resumes above the highest own
counter found in local data; a compact wire encoding shared later by the transport and the
RDB codec. The type-specific merge of values is ticket 29; this ticket is the clock only.

**Blocked by:** 17 (Hash ring)

**Nature:** causal ordering, C2 and I4 (Fable)

**Status:** done (DynaCache 5997afc, merged into misc/ai_gen)

- [x] `dvv_dominance_detection`, `dvv_concurrent_detection`, `dvv_merge_preserves_causality`
- [x] `dvv_bounded_size`: 10,000 writes from 100 clients through 3 nodes, size at most 3
- [x] `dvv_no_counter_reuse`, `I4_later_write_dominates`, `C2_counter_strictly_increases`
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
measurement forces it, docs/dynamiccache/.
