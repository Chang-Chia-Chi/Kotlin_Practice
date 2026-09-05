# 10: Memory accounting and LRU eviction

**What to build:** Every mutation maintains an approximate byte size per entry; a per-node
threshold is split evenly per partition; after any write that crosses the threshold, a bounded
eviction step runs on the partition's executor that removes expired keys first and then
sampled LRU victims (K random keys, the least recently accessed goes) until under threshold;
access times come from the injected clock; `INFO` reports used memory.

**Blocked by:** 07 (Sorted Set), 09 (TTL commands)

**Nature:** eviction semantics, spec 5.5 and I6 (Opus)

**Status:** ready-for-agent

- [ ] `eviction_respects_max_memory`, `eviction_prefers_expired`, `lru_evicts_oldest_access`, `eviction_does_not_corrupt`
- [ ] `I6_expired_evicted_before_live`
- [ ] The eviction step is bounded and never runs off the partition's executor
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
