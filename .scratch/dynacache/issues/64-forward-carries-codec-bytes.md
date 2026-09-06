# 64: A forward carries codec bytes

**What to build:** When the contact node is not a key's coordinator, the router forwards the
command as the engine codec's bytes and the coordinator decodes them; the RESP spelling of a
command no longer crosses the cluster seam. The router loses its two injected functions
(tokens and parse), an unparseable forward becomes impossible by construction, and the
server's RESP re-encoding of commands is deleted if nothing else uses it. This is the first
migrate step after ticket 63's expand; replicates follow in ticket 65.

**Blocked by:** 63 (The engine's command codec encodes every keyed command)

**Nature:** routing adapter, spec 5.1 steps 1 to 3 (Opus)

**Status:** ready-for-agent

- [ ] The `Forward` envelope carries codec bytes; the router's constructor takes no
      tokens/parse functions and the server's wiring and the in-process cluster stop passing
      them for forwards
- [ ] `forward_round_trips_every_keyed_variant`: every keyed command, including a conditional
      `SET` with a TTL and a multi-key read, forwards and answers as if run locally
- [ ] Every existing router, forwarding and P2 acceptance test passes unchanged
- [ ] The server's command-to-tokens encoder is deleted, or the progress entry names its
      remaining caller
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
