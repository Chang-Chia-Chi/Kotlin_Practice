# 32: Snapshot engine

**What to build:** Per partition, on its executor, an immutable point-in-time view of the
partition (copy-on-write or a persistent structure, free choice) handed to a writer that
serializes off the executor; the view is taken between commands so no batch or script is
half inside it; save on interval and on graceful shutdown; restore at startup before the node
joins; atomic rename on completion.

**Blocked by:** 14 (MULTI, EXEC, DISCARD), 31 (RDB codec)

**Nature:** non-blocking snapshot under concurrent writes, C9 (Fable)

**Status:** done (DynaCache a683b6f, merged into misc/ai_gen)

- [x] `rdb_concurrent_writes`: writes during the save, the file is a valid point in time
- [x] `C9_snapshot_never_contains_half_a_batch`: a `MULTI/EXEC` of ten keys racing a snapshot, all ten or none
- [x] `snapshot_restore_on_startup`, `snapshot_does_not_block_reads` (a read completes while the writer is stalled by a slow injected sink)
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p4-persistence.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/.
