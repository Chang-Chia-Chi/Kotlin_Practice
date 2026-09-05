# 19: Request router

**What to build:** The contact node computes the coordinator from the preference list, submits
to its own engine when it is the coordinator and otherwise sends a `Forward` carrying the
command tokens and receives a `ForwardReply` carrying the `Reply`. The router presents the
`CommandEngine` shape itself, so the server's ticket 13 pipeline submits to it unchanged;
forwarded errors pass through unchanged; a forward that misses its deadline is an error reply.

**Blocked by:** 13 (Command parser and Netty server), 18 (Transport seam and test kit)

**Nature:** routing adapter, spec 5.1 steps 1 to 3 (Opus)

**Status:** ready-for-agent

- [ ] `router_executes_locally_when_coordinator`, `router_forwards_to_coordinator`
- [ ] `router_forwarded_reply_identical_to_local`, `router_forward_timeout_is_an_error`
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
measurement forces it, docs/dynamiccache/. DynaCache/ is its own git repository; commit code
there and docs in the parent.
