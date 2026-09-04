# 16: Pressure: randomized adversary, model checking, partition matrix, soak

**What to build:** The connector is run the way MIT 6.824 runs a Raft implementation: a seeded
random adversary drives the transport for thousands of operations with every invariant checked
after each one; the two lock-guarded structures are model-checked across thread interleavings;
real network partitions are staged through Toxiproxy against a real server; and an opt-in soak
runs for hours reading the meters. Every defect found is fixed in its own commit carrying the
seed or the row that found it. Spec Sec 17 has the tier definitions and the at-least-once
property (I15) this ticket proves.

**Blocked by:** 15 (Acceptance run)

**Status:** done

- [ ] Tier A: seeded randomized fault model over `FakeSftpTransport`, ported from
      `snapshotcache`'s `RandomizedModelTest` pattern - fixed seed, per-sequence `Random(SEED + i)`,
      weighted ops filtered by precondition, pure model, invariant check after every op, failure
      rethrown with the seed and a copy-pasteable replay line, shrunk to the shortest failing prefix.
      5000 sequences x 40 ops under `runTest` virtual time; runs on every build
- [ ] Tier A invariants after every op: I1/I4 (total <= maxSize; permits never leak), I7/I12, I13,
      `openSessions == pool total` (no orphan session), breaker never counts a `Fatal` or
      `OverwriteRefused`, and I15 at-least-once with no phantom failure. End of sequence: `close()`
      within I9's bound, every entry `Closed`, `openSessions == 0`, no `.part`
- [ ] Tier A in-build leak check: one 50,000-op run on one seed asserting `openSessions`, thread
      count and post-GC heap stay within a band of their values at op 1,000
- [ ] Tier B: Lincheck `ModelCheckingOptions` on `InFlightSet` (the real candidate: I7, I8,
      capacity) and one run on `SessionRegistry` (I2); approach recorded - suspend `@Operation`s or
      an extracted non-suspending core if the coroutines-version mismatch bites. Own surefire
      execution; transitive JUnit 4 excluded
- [ ] Tier C: Toxiproxy partition matrix, one named test per row of spec Sec 17.3, topology
      `client -> LoopbackConnectProxy -> Toxiproxy -> EmbeddedSftpServer`, each asserting the
      disposition, the counter that moved and the recovery time; skips with a clear message when
      Docker is absent
- [ ] Tier D: soak gated on `-Dsftp.soak.minutes=N`, excluded by default, seeded fault schedule on
      the loopback proxy, per-minute samples of threads, post-GC heap and the `sftp_*` meters to
      `target/soak/*.csv`; asserts flat threads and heap (linear-fit slope within noise),
      `created_total` proportional to injected kills, recovery under `2 x keepAlive + max backoff`,
      every produced file delivered exactly once
- [ ] Degradation measured and recorded as observations, not assertions: acquire p50/p99 for
      concurrency 1..maxSize+2, listing memory under three concurrent 100k listings, op_seconds
      by class under each toxic
- [ ] Every defect found is fixed in its own commit with the failing seed or Tier C row in the
      message; anything that contradicts the spec is recorded and raised, not decided
- [ ] Progress entry appended, with the measurements table

Dependencies, managed in the parent pom (validated 2026-09-03): `org.jetbrains.lincheck:lincheck`
3.7 (new group; the `kotlinx` one is deprecated at ERROR), `org.testcontainers:testcontainers-toxiproxy`
2.0.5 (Testcontainers 2.x naming; brings `toxiproxy-java` transitively). No JMH - nothing here
has a hot path. Not in Tier C on purpose: cooperative cancel under `bandwidth` (T8 proved it with
`holdAfter`), `latency` p99 (Tier D's job), `slicer` (SSH framing makes it implausible).

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; this ticket's budget is 600-900 lines because it is almost entirely tests;
no Thread.sleep outside Tier D's real-time sampling; invariant tests named `I<n>_<description>`;
every new configuration knob lands in the DSL block for its area with build-time validation;
every new meter uses the names fixed in spec Sec 13; append a progress entry describing what
was done and every deviation. The spec is docs/sftpconnector/spec.md and it wins over this
ticket when they disagree, unless the progress log records a deliberate deviation.
