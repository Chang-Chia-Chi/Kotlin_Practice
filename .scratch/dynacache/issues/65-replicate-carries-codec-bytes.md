# 65: A replicate carries codec bytes

**What to build:** A replica applies exactly the entry the coordinator logged. The coordinator
runs the write, passes (command, reply, now) through the single what-changed function of
ticket 63, logs the result and ships the same bytes to its replicas with the version; the
replica decodes and submits them. Replication's hand-written copies of the rules (the
refused-SET check, stripping NX/XX, turning the TTL into an instant, the replica's two
submits for a TTL'd SET) are deleted, its tokens/parse constructor functions go, and the
cluster test kit's partial command encoding is deleted. Hints store the same envelope as
before, so hinted handoff is unchanged. This is the contract step of the codec consolidation.

**Blocked by:** 64 (A forward carries codec bytes)

**Nature:** replication protocol, ADR 0003 (Fable)

**Status:** done (DynaCache 7d3525bb, merged into misc/ai_gen; finished by a second agent after the first hit the session limit)

- [x] `replica_applies_exactly_the_logged_entry`: a TTL'd conditional `SET` on the
      coordinator arrives at each replica as one entry with an absolute instant and the
      condition already decided, and the replica's WAL holds the same bytes
- [x] No tokens/parse functions remain in the cluster module's constructors; the test kit's
      command encoding class is deleted
- [x] Every existing replication, quorum, hint, read-repair, anti-entropy, convergence and
      P2/P3/P4 acceptance test passes unchanged
- [x] ADR 0003 gains one line: the command ships as the engine codec's bytes
- [x] Progress entry appended

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
