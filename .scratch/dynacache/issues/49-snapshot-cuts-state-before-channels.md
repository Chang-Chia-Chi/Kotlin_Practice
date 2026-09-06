# 49: A snapshot cuts state before it opens channels

**What to build:** A write that lands while a node is taking its part of a Chandy-Lamport
snapshot is restored exactly once. Today the node starts recording its incoming channels,
then cuts and saves its state, then sends markers; an envelope applied in between is in both
the saved state and a channel log, and restore applies it twice. After this ticket the order
is: cut the state, then open the channels for recording, then send markers, so every envelope
is either in the state or on a channel, never both. The gRPC ordering gap (concurrent unary
sends to one peer are not FIFO, so a channel is not a true channel over gRPC) is recorded in
the progress entry as a known limitation, not fixed here.

**Blocked by:** None (can start immediately)

**Nature:** consistent-cut protocol, C10 and I12 (Fable)

**Status:** done (DynaCache 3886d81d, merged into misc/ai_gen)

- [x] `I12_write_during_the_cut_is_restored_once`: with the in-memory transport under
      `runTest`, a replicate envelope delivered while the initiator is suspended inside its
      state save is restored with its effect applied exactly once on every node
- [x] `C10_state_is_cut_before_any_channel_opens`: no channel log for a snapshot id contains
      an envelope whose effect is also in that node's saved state
- [x] `chandy_lamport_consistent_cut` and every existing snapshot test still pass
- [x] The progress entry names the gRPC no-FIFO limitation and what a fix would need
- [x] Progress entry appended

A red test for this exists in the review worktree `kp-wt/review` under the cluster module's
test tree (`BugHuntSnapshotTest`); reuse it if present, otherwise rewrite it from the first
criterion. Ground rules for every ticket: implement only this ticket; 200 to 600 lines
including tests; JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected
Clock; spec-named tests keep their names, constraint tests `C<n>_<description>`, invariant
tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before
green, one slice at a time, `codebase-design` vocabulary for any new interface, and a
`code-review` self-pass before the commit; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's
entry is docs/dynamiccache/plans/p6-review-fixes.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only DynaCache/ and,
when a measurement forces it, docs/dynamiccache/.
