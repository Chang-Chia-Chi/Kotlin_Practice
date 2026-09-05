# 22: Replication and quorum

**What to build:** The coordinator write of spec 5.1 steps 4 to 6 and 8 (bump the DVV, apply
locally, replicate to N-1 successors with a deadline, reply after W acks counting itself,
error when W is unreachable) and the coordinator read of spec 5.2 steps 1 to 4 (R replies,
the dominating DVV wins, highest node id breaks a true tie); R + W > N validated at
construction; TTL replicated as an absolute instant; every stored value carries its DVV.
Sloppy quorum is ticket 25 and read repair ticket 26.

**Blocked by:** 19 (Request router), 20 (SWIM gossip), 21 (Dotted Version Vectors)

**Nature:** quorum protocol, C4 (Fable)

**Status:** ready-for-agent

- [ ] `write_read_quorum`, `minority_failure_available`, `majority_failure_unavailable`
- [ ] `C4_write_needs_w_distinct_acks`, `C4_read_returns_highest_dvv`
- [ ] `quorum_config_rejects_r_plus_w_not_above_n`
- [ ] Every fan-out is bounded by N and a deadline
- [ ] Progress entry appended

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
