# 67: Versions survive restart

**What to build:** After a crash and recovery every key's version is what it was before the
crash, so a restarted node takes part in quorum reads, read repair and anti-entropy with the
same authority it had. Today versions live only in memory; the RDB has an unused version slot
per entry and the WAL entries carry none. After this ticket the versioned store of ticket 66
is what persists: the RDB entry's version slot is filled on save, a WAL entry carries the
version as an opaque trailer (the engine never interprets it), and the store rebuilds its
table from both on load. The dot counter then starts at the larger of ticket 51's persisted
ceiling and the highest own dot in the rebuilt table, so 51's guarantee is kept and the
counter is also exact.

**Blocked by:** 51 (A node's dot counter survives restart), 66 (A versioned store beside the engine)

**Nature:** durability of versions, C2 and I2 (Fable)

**Status:** ready-for-agent

- [ ] `I2_versions_survive_restart`: write under versions, snapshot, write more, crash,
      recover; every key's version equals the pre-crash version, including keys written after
      the snapshot
- [ ] `restarted_replica_answers_quorum_read_with_its_version`: a restarted replica's answer
      carries a version, and read repair neither reverts it nor pushes to it needlessly
- [ ] `dvv_no_counter_reuse` holds across restart with the counter derived from the table
- [ ] The RDB and WAL format versions are bumped; a file from before this ticket is either
      read with empty versions or rejected with a clear error, and the progress entry says which
- [ ] Every existing persistence and P4 acceptance test passes
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
