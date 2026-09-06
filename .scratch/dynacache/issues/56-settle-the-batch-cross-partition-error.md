# 56: Settle the batch cross-partition error

**What to build:** One agreed reply for a batch whose keys do not share a partition, with the
documents in agreement. Today the engine answers `-CROSSSLOT Keys in request don't hash to
the same slot`; ADR 0002 says `-CROSSSLOT` was considered and rejected, the glossary says
never "slot", and the T14 progress entry records `CROSSSLOT` as the chosen error kind. The
decision, taken in the 2026-09-06 review: keep the `CROSSSLOT` error kind (it is what client
libraries switch on, and ADR 0002 rejected it for fan-out commands, not for batches) and
reword the message in the glossary's words: `-CROSSSLOT keys of a batch must share a
partition (use a hash tag)`. ADR 0002 gains one line saying the kind is used for batches
only. The three tests pinning the old message are updated.

**Blocked by:** None (can start immediately)

**Nature:** batch semantics, C12 (Opus)

**Status:** ready-for-agent

- [ ] The engine, the server and the Lua bridge all answer the new message for a batch that
      spans partitions; the pinned tests assert the new wording and the kind
- [ ] ADR 0002's "Considered and rejected" paragraph gains the one-line clarification
- [ ] The word "slot" appears nowhere in DynaCache main sources, tests or CONTEXT.md except
      inside the `CROSSSLOT` error kind itself
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
