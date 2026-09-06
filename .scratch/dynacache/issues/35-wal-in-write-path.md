# 35: WAL in the write path, checkpoint, recovery

**What to build:** The partition executors append every mutation to the node's WAL and await
durability per policy before applying to the engine and replying; a checkpoint after each
successful RDB save truncates entries at or below the snapshot's sequence number; startup
recovery loads the RDB and replays entries after the checkpoint; replay is idempotent
(absolute TTLs, DVV-carrying writes).

**Blocked by:** 32 (Snapshot engine), 34 (Fsync policies and group commit)

**Nature:** write-ahead ordering, C14 (Fable)

**Status:** done (DynaCache bd6ddff + merge 622de9c, merged into misc/ai_gen)

- [x] `wal_checkpoint_truncates`
- [x] `wal_full_recovery`: write, snapshot, write more, crash, restore from RDB plus WAL, every key present
- [x] `wal_replay_idempotent`, `C14_reply_only_after_durable_append` (a stalled sink delays the reply)
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
