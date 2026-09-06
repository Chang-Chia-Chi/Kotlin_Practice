# 55: A snapshot-set part is a persist adapter

**What to build:** The cluster module stops touching the filesystem. A node's part of a
snapshot set (its state file plus one log per recorded channel) is written and read by an
adapter in the engine's persist package, next to the snapshot engine that already owns the
RDB directory handling; each channel log is an ordinary WAL, so it gets the crc and torn-tail
recovery the WAL writer and reader already provide, which the current delimited-protobuf loop
lacks after a crash mid-append. The cluster keeps the marker rules and the channel bookkeeping
and receives an append/replay pair for a part. Plan 2.2's rule that `java.nio.file` appears
only in the engine's persist package and the cp module holds again for the cluster.

**Blocked by:** 49 (A snapshot cuts state before it opens channels)

**Nature:** file format and crash recovery of the file (Opus)

**Status:** ready-for-agent

- [ ] No `java.nio.file` import remains under the cluster module's main sources
- [ ] `snapshot_part_with_torn_channel_log_replays_the_complete_prefix`: a channel log cut
      mid-record replays every complete record and stops cleanly
- [ ] `snapshot_set_deleted_as_a_whole_on_deadline` and every existing Chandy-Lamport and P4
      acceptance test pass unchanged
- [ ] Restoring a part written before this ticket is either supported or rejected with a
      clear error; the progress entry says which
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
