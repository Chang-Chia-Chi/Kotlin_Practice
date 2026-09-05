# 36: Chandy-Lamport distributed snapshots

**What to build:** A `Marker` message; the initiator records local state (the ticket 32
snapshot) and sends markers on every outgoing channel; a receiver on its first marker records
state, sends markers, and starts recording in-flight messages on its other incoming channels;
a marker on channel C stops recording C; completion when every channel is closed; per-node
state files plus per-channel message logs form the snapshot set; abort on timeout with no
partial files left; a restore path that loads state and replays the recorded channel messages.

**Blocked by:** 22 (Replication and quorum), 32 (Snapshot engine)

**Nature:** consistent-cut protocol, C10 and I12 (Fable)

**Status:** ready-for-agent

- [ ] `chandy_lamport_consistent_cut`: traffic during the snapshot; for every recorded B with A before B, A is recorded
- [ ] `chandy_lamport_restorable`, `chandy_lamport_timeout_aborts` (a node killed mid-snapshot; no files, no state change)
- [ ] `C10_marker_on_every_channel`, `I12_reads_after_restore_return_snapshot_time_values`
- [ ] Progress entry appended

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
