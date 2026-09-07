# 74: The live WAL never lives inside a snapshot part

**What to build:** A write acknowledged after a distributed snapshot's cut survives a restart
even when that snapshot set is later abandoned. Today a node wired with both a data directory
and a snapshot directory rotates its live WAL into the snapshot **part** at the cut (the
snapshot engine's save resolves the next log file under the part), so the WAL keeps growing
inside the part; when the node's deadline passes with a channel still open, the part is deleted
as a whole and the open log goes with it, and recovery, which reads the data directory only,
never sees those writes (C14, spec 2.8 recovery sequence). After this ticket the live WAL
always continues under the data directory; a part holds a copy or a sealed segment of the log
up to the cut, never the file the engine is still appending to; and deleting a part can never
remove a file recovery needs. Found by T55, pre-existing since T36.

**Blocked by:** 55 (A snapshot-set part is a persist adapter)

**Nature:** concurrent durability protocol, C14 and spec 2.8 (Fable)

**Status:** ready-for-agent

- [ ] `C14_writes_after_the_cut_survive_an_aborted_snapshot_set`: a node with a data directory
      and a snapshot directory cuts a part, acks writes after the cut, aborts the set at its
      deadline, restarts, and every acked write is present
- [ ] `snapshot_part_holds_the_log_up_to_the_cut_only`: the part's log content equals the WAL
      up to the cut and nothing after; the live WAL file is not under any part
- [ ] `chandy_lamport_restorable`, `I12_reads_after_restore_return_snapshot_time_values`, every
      WAL, recovery and P4 acceptance test passes
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
