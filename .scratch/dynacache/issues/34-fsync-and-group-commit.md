# 34: Fsync policies and group commit

**What to build:** The `ALWAYS`, `EVERY_SECOND` (driven by the injected clock and a tick) and
`NEVER` policies; group commit where concurrent appenders enqueue, one flusher writes the
batch and fsyncs once, and every appender's durability future completes together; the fsync
call goes through a small sink interface so tests count it (the filesystem is a true
boundary).

**Blocked by:** 33 (WAL writer and reader)

**Nature:** concurrent durability protocol (Fable)

**Status:** done (DynaCache 448f668, merged into misc/ai_gen)

- [x] `wal_fsync_always_durable`: one fsync per append
- [x] `wal_fsync_every_second_batches`: fsync count far below append count
- [x] `wal_group_commit_amortizes`: 100 concurrent appenders, fsync count far below 100, every appender completes
- [x] `wal_group_commit_preserves_seq_order`
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
