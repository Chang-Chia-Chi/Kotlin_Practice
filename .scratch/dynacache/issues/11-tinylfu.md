# 11: Count-Min Sketch and W-TinyLFU

**What to build:** A four-row Count-Min Sketch with periodic halving; the W-TinyLFU policy
(admission window of one percent as LRU, main space as segmented LRU with probation and
protected; on window eviction the candidate's estimated frequency against the main victim's
decides admission), selectable at engine construction next to LRU. LFU as a
frequency-counter variant of the sampling loop if it fits the budget, otherwise recorded as a
deviation.

**Blocked by:** 10 (Memory accounting and LRU eviction)

**Nature:** eviction policy, spec 2.7 (Opus)

**Status:** ready-for-agent

- [ ] `tinylfu_admits_frequent`
- [ ] `sketch_estimate_never_underestimates`, `sketch_ages_halves_counts`
- [ ] `eviction_respects_max_memory` green under `W_TINYLFU`
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p1-data-engine.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/.
