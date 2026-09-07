# 68: One inbound loop, and a send-only transport

**What to build:** A node reads its incoming envelopes in one place that owns the handler
order (snapshot markers first, then forwards, replication, anti-entropy, gossip), and every
module that only sends takes a send-only seam whose one promise is stated in the interface:
an unreachable peer is a drop, never a throw. Today the transport's inbound side may be read
only by the router, so the server's node wiring builds five transport views, four of them
deaf on purpose, and turns gRPC exceptions into drops to match the in-memory adapter; the
demux chain is hand-built twice, once in the node wiring and once in the in-process test
cluster; and SWIM keeps a second inbound path alive only for its own test. After this ticket
the node wiring and the in-process cluster share one inbound loop, the four wrappers and the
second SWIM path are gone, and both transport adapters satisfy the same send-only seam.

**Blocked by:** None (can start immediately). Touches the router, so if agents run in
parallel schedule it after 64.

**Nature:** adapter and wiring (Opus)

**Status:** done (DynaCache dabc5035, merged into misc/ai_gen)

- [x] `inbound_order_is_snapshots_forwards_replication_antientropy_gossip`: a test drives
      one envelope of each kind through the loop and asserts which handler saw it and in what
      order when several arrive together
- [x] `unreachable_peer_is_a_drop_on_both_adapters`: sending to a dead peer over gRPC and
      over the in-memory transport both return without throwing and without a reply
- [x] The server's node wiring builds one transport, not five views; the in-process cluster
      uses the same loop class; the SWIM test drives delivery through the loop
- [x] Every existing transport, SWIM, router, replication and acceptance test passes
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
