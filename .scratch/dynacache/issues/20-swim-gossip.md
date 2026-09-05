# 20: SWIM gossip membership

**What to build:** The SWIM protocol period as a step function: ping, ping-req through K
intermediaries, suspect after the RTT bound, dead after T rounds, refutation by incarnation
number, membership piggybacked on every message; the `Membership` seam (alive, suspect, dead,
a change flow) with a scripted fake for later tickets; period, K, RTT and T configurable; one
coroutine per node in production, step-driven in tests.

**Blocked by:** 18 (Transport seam and test kit)

**Nature:** failure-detector state machine, I8 (Fable)

**Status:** ready-for-agent

- [ ] `gossip_detects_failure`, `gossip_detects_recovery`
- [ ] `I8_membership_change_reaches_all_within_log_n_rounds` on 5 and 7 nodes, seeded
- [ ] `gossip_suspect_refuted_by_incarnation`, `gossip_ping_req_masks_one_lost_link`
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
