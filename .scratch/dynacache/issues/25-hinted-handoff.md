# 25: Hinted handoff and sloppy quorum

**What to build:** A `HintStore` per node holding (target node, key, value, DVV, absolute TTL);
when a preference-list node is dead per `Membership`, the coordinator sends the write to the
next healthy node on the ring, which stores a hint and acks toward W; on a `Membership` alive
event the holder replays its hints to the target as ordinary replication writes and deletes
each on ack; replay is one bounded coroutine per node.

**Blocked by:** 22 (Replication and quorum)

**Nature:** availability protocol, C5 and I9 (Fable)

**Status:** done (DynaCache 94ec86b, merged into misc/ai_gen)

- [x] `hinted_handoff_replays`, `I9_rejoined_node_matches_reference_replica`
- [x] `C5_hint_carries_full_write`: value, DVV and TTL identical after replay
- [x] `sloppy_quorum_reaches_w_with_one_dead_node`, `hint_deleted_after_ack`
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p3-fault-tolerance.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only DynaCache/ and,
when a measurement forces it, docs/dynamiccache/.
