# 75: A marker with an unusable snapshot id is dropped

**What to build:** A snapshot id that arrives on a marker from the wire can never become a
path the node did not intend. Today the marker's id reaches the part adapter unchecked and is
resolved as a directory name under the snapshot root, and an abort deletes that directory
recursively, so a crafted or corrupted id is a path traversal that ends in a recursive delete.
After this ticket the part adapter accepts only ids of a fixed safe shape (one path segment,
a bounded length, a bounded alphabet, never `.` or `..`) and refuses anything else without
throwing past the demux; the snapshot module drops a marker whose id the adapter refuses,
records nothing for it, and the node keeps running. A bare `require` is not the fix: the
inbound loop has no per-envelope catch by design, so a throw would turn a traversal into a
node kill. Found by T55, pre-existing since T36.

**Blocked by:** 55 (A snapshot-set part is a persist adapter)

**Nature:** validation at a trust boundary, C10 (Opus)

**Status:** done (DynaCache be51b192, merged into misc/ai_gen)

- [x] `snapshot_id_outside_the_safe_shape_is_refused_by_the_adapter`: `..`, a separator, an
      empty id, an over-long id and a control character are each refused without touching the
      filesystem, and an id of the safe shape is accepted
- [x] `marker_with_an_unusable_id_is_dropped_and_the_node_lives`: a marker carrying `../x`
      arrives on the in-memory transport; no directory is created or deleted, no part is
      recorded, and the next valid marker still starts a snapshot
- [x] The initiator generates ids of the safe shape; every existing Chandy-Lamport test passes
- [x] Progress entry appended

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
