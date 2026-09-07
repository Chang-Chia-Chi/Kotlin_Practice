# 70: Each CP primitive owns its snapshot bytes

**What to build:** Adding a CP primitive touches one file plus the parser. Today the composite
state machine exposes every primitive's table as a public field, its snapshot type lists one
map per primitive, and the wire codec knows every primitive's field layout, so a new primitive
is a five-file change. After this ticket each primitive encodes and decodes its own table, the
snapshot is a list of (primitive id, opaque bytes), the composite iterates its primitives for
apply, sweep, snapshot and restore, and the wire codec carries the blobs without reading them.
The snapshot encoding version is bumped; a snapshot from before this ticket is rejected with a
clear error (the project is pre-release; no migration).

**Blocked by:** 69 (CP primitives tested at the state machine, without Raft)

**Nature:** codec, snapshot format (Opus)

**Status:** done (DynaCache a0e5cdb4, merged into misc/ai_gen; snapshot format version 2, older snapshots refused)

- [x] Each primitive has a snapshot round-trip test of its own table through its own bytes
- [x] The composite's snapshot and restore are a loop over its primitives; no primitive table
      is public
- [x] `cp_snapshot_install_preserves_tokens_and_sessions` and every existing snapshot, chaos
      and failover test passes; an old-format snapshot is rejected with a named error
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200 to 600 lines including tests;
JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named
tests keep their names, constraint tests `C<n>_<description>`, invariant tests
`I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before green,
one slice at a time, `codebase-design` vocabulary for any new interface, and a `code-review`
self-pass before the commit; append a progress entry to docs/dynamiccache/progress.md
describing what was done and every deviation. The spec is docs/dynamiccache/design-spec-cp.md,
the plan is docs/dynamiccache/plan.md and this ticket's entry is
docs/dynamiccache/plans/p6-review-fixes.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/.
