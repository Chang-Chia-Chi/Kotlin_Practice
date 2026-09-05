# 18: Transport seam and in-process cluster test kit

**What to build:** `cluster.proto` with the `Envelope` message (a oneof that later tickets
extend with their own messages); the generated classes are the cluster's message model, so no
codec exists anywhere (CONTEXT.md "transport"). The `Transport` seam (`send`, `inbound`) over
those classes and its first adapter, `InMemoryTransport`, with `networkPartition(sides)`,
`heal()`, `drop(rate, seed)`, `delay(range, seed)`, `kill(node)`, `restart(node)` and
deterministic delivery under `runTest`; `InProcessCluster(nodeCount, n, w, r)` wiring engine,
ring and transport per node with `drainMessages()`, `writeVia`, `readVia` and
`readAllReplicas(key)`; a recording fake `CommandEngine` for router and dispatcher tests.

**Blocked by:** 17 (Hash ring)

**Nature:** the load-bearing test kit, deterministic concurrency (Fable)

**Status:** ready-for-agent

- [ ] `transport_delivers_in_order_per_pair`, `network_partition_blocks_both_directions`, `heal_restores_delivery`
- [ ] `kill_stops_delivery_and_restart_resumes`, `drop_is_reproducible_by_seed`
- [ ] `cluster_boots_three_nodes_sharing_one_ring`
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
