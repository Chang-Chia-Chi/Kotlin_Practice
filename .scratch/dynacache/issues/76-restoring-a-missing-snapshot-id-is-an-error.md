# 76: Restoring a missing snapshot id is an error, not an empty node

**What to build:** Asking a node to restore a snapshot id it never cut answers an error and
leaves the node as it was. Today the snapshot engine's restore of a missing dump file is a
silent no-op that leaves the engine empty, so a restore request with a typo empties a node
without complaint. After this ticket a restore names its precondition: the part must exist
and be complete; otherwise the reply is an error, nothing is cleared, and the engine's data
is untouched. Found by T55, pre-existing since T32.

**Blocked by:** 55 (A snapshot-set part is a persist adapter)

**Nature:** command semantics, I12 (Opus)

**Status:** ready-for-agent

- [ ] `restore_of_a_missing_id_is_an_error_and_changes_nothing`: with data in the engine, a
      restore of an unknown id answers an error and every key still reads as before
- [ ] `restore_of_an_incomplete_part_is_an_error`: a part with a state file but a channel still
      open is refused the same way
- [ ] `chandy_lamport_restorable` and every existing restore, snapshot and P4 acceptance test
      passes; the single-node RDB restore path keeps its behaviour for a present file
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200 to 600 lines including tests;
JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named
tests keep their names, constraint tests `C<n>_<description>`, invariant tests
`I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before green,
one slice at a time, `codebase-design` vocabulary for any new interface, and a `code-review`
self-pass before the commit; append a progress entry to docs/dynamiccache/progress.md
describing what was done and every deviation. The spec is docs/dynamiccache/design-spec.md,
the plan is docs/dynamiccache/plan.md and this ticket's entry is
docs/dynamiccache/plans/p6-review-fixes.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/.
