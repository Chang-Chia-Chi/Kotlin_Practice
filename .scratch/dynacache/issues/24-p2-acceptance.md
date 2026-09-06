# 24: P2 acceptance

**What to build:** An acceptance harness starting three server nodes in one JVM on ephemeral
RESP and gRPC ports with real gossip; Jedis writes through node 1 and reads through node 3;
forwarding is exercised by writing a key whose coordinator is not the contact; node 2 is
stopped and reads and writes still succeed; `INFO` shows membership. Gossip and quorum timing
is the acceptance tier's only real time, awaited with deadlines.

**Blocked by:** 16 (P1 acceptance), 22 (Replication and quorum), 23 (gRPC transport)

**Nature:** acceptance (Opus)

**Status:** done (DynaCache 717f5c2 + merge abd4e0f, merged into misc/ai_gen)

- [x] `P2_acceptance_three_nodes_quorum_and_minority_failure` green
- [x] Every P1 and P2 test green in the same run
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p2-distribution.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/.
