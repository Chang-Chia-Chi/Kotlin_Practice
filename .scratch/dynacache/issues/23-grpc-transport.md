# 23: gRPC transport adapter

**What to build:** The second adapter of the `Transport` seam: a `ClusterService` with a
`Deliver` RPC over the ticket 18 `Envelope`, `GrpcTransport` with one channel per peer, and the
gRPC server hosted in the server module next to Netty. No codec: the envelope goes on the wire
as is.

**Blocked by:** 18 (Transport seam and test kit)

**Nature:** technology adapter (Opus)

**Status:** done (DynaCache a5de4e6, merged into misc/ai_gen)

- [x] `grpc_transport_roundtrip_every_message_type` on two transports over localhost ephemeral ports, the same suite the in-memory adapter passes
- [x] `grpc_peer_down_is_a_send_error`
- [x] Generated classes appear only in the cluster module, the cp module and the server's adapters
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
measurement forces it, docs/dynamiccache/. DynaCache/ is its own git repository; commit code
there and docs in the parent.
