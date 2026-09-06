# 33: WAL writer and reader

**What to build:** An append-only writer for `[crc32][length][seq][op][payload]` and a reader
that returns every complete entry, stops at the first CRC failure or torn tail and reports
where; sequence numbers strictly increasing; the `NEVER` fsync policy only (the others are
ticket 34).

**Blocked by:** 01 (Skeleton)

**Nature:** file format and crash recovery of the file (Opus)

**Status:** done (DynaCache 017d5f3, merged into misc/ai_gen)

- [x] `wal_write_read_roundtrip`
- [x] `wal_crash_recovery`: a torn last entry is skipped, all earlier entries returned
- [x] `wal_crc_detects_corruption`: a flipped byte stops the reader at that entry
- [x] `wal_seq_strictly_increasing`
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
