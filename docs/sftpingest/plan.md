# SFTP Ingest - Implementation Plan

Version: v0.1
Companion to: `docs/sftpingest/spec.md` v0.1 (Sec 1 to 17)
Purpose: break the application into phases small enough for an agent to implement in one
session each, with fixed contracts and fixed acceptance criteria, so implementations may vary
but assertions may not.

Phases here are `G0` to `G10`. The repository already has `P` phases (snapshotcache), `E`
phases (SimpleEtl) and numbered tickets 01 to 15 (the connector); the letter is what
disambiguates.

---

## 1. Ground Rules for Agent-Driven Implementation

1. **Fixed vs free.** Interfaces and value types from G0, the ledger DDL (spec 5.1), the
   transfer and delivery states (spec 4.2, 7.2), the invariants and scenario table (spec 17),
   and every test assertion are FIXED. Internal class structure, algorithms and private naming
   are free. An agent that believes a fixed contract is wrong stops and reports; it does not
   adapt.
2. **One phase, one change,** roughly 200 to 600 lines including tests. Past that, the phase
   was scoped wrong: stop and split.
3. **Tests are deliverables.** Invariant tests are named `I<n>_<description>`; scenario tests
   are named by their `S<n>` id.
4. **No scope creep.** Nothing from a later phase, even "while I'm here". A stub throwing
   `NotImplementedError` is the placeholder for a later seam.
5. **No sleeps in tests.** Time is an injected `java.time.Clock`; coroutine tests run under
   `runTest`; interleavings use the declared `Hook` points.
6. **Documents win.** When code and documents disagree, the documents win unless
   `docs/sftpingest/progress.md` records a deliberate deviation. Every phase appends an entry
   there. The three tiers of authority in the root `CLAUDE.md` apply.
7. **Boundary.** A phase modifies only `sftp-ingest/` and, when a measurement forces it,
   `docs/sftpingest/`. Nothing here touches the connector's module or documents; a need the
   connector does not meet is an appeal raised in its progress log, not a patch.

---

## 2. Architecture Overview

### 2.1 Design stance

One Maven module, `sftp-ingest`, a Quarkus application in the shape of `etl-host`. Packages
are the boundary and ArchUnit enforces them from G0. The framework-free core is
`infra.sftpingest.pipeline`; every technology is an adapter package named for it. A seam
exists only where a second implementation is real, and every seam below has one in the test
kit.

### 2.2 Package layout and rules

```
sftp-ingest/src/main/kotlin/infra/sftpingest/
  pipeline/   Transfer, TransferState, FileIdentity, Digest, LocalFile, TargetRef,
              IngestEvent, Downloader, Ledger, Target, DeliveryChannel,
              DeliveryEvent, DeliveryOutcome, DeliveryPolicy, QualityCheck, Hook,
              RouteConsumer, FilePipeline, Relay, IngestConfig + DSL, IngestMetrics
  sftp/       SftpBinding: connector SftpEvent -> IngestEvent, connector download -> Downloader
  jdbi/       JdbiLedger, LedgerSchema (the DDL text)
  s3/         S3Target (store = PUT + HEAD + prune, verify, probe)
  http/       HttpChannel, HttpChannelBuilder (the channel DSL block)
  quarkus/    Producers, IngestHost (start/stop), ReadinessCheck, AdminResource, HostConfig
```

ArchUnit sentences, all enforced from G0:

- `pipeline` depends on nothing in this module and on no technology: not the connector, not
  JDBI, not the AWS SDK, not `java.net.http`, not Quarkus. Allowed: kotlin-stdlib, coroutines,
  micrometer-core, jboss-logging, Jackson databind for the `DeliveryEvent` body tree.
- `sftp` depends on `pipeline` and the connector core only.
- `jdbi` depends on `pipeline` and JDBI only. `java.sql` and `org.jdbi` appear nowhere else.
- `s3` depends on `pipeline` and `software.amazon.awssdk` only.
- `http` depends on `pipeline`, `java.net.http` and Jackson only.
- `quarkus` may depend on everything above and is depended on by nothing.
- Logging is `org.jboss.logging.Logger` everywhere. Time is `java.time.Clock`, injected.

### 2.3 Public surface budget

Exactly five interfaces, all in `pipeline`:

| Interface | Role | Second implementation |
|---|---|---|
| `Ledger` | every state transition, spec 5.2 | `InMemoryLedger` (test kit) |
| `Target` | store, verify, probe, spec 6.1 | `InMemoryTarget` (test kit) |
| `DeliveryChannel` | deliver one event, spec 7.1 | `RecordingChannel` (test kit) |
| `QualityCheck` | Pass or Fail on a staged file, spec 8 | `QualityCheck.NONE` and a scripted one |
| `Hook` | named interleaving points, spec 4.4 | no-op and the test driver |

`IngestEvent` and `Downloader` are a sealed class and a function type, not interfaces, and
the test kit's `ScriptedSource` produces them directly. Everything else is a concrete class.

### 2.4 Do-not-build list

- A Quarkus `@Scheduled` anything. The connector's `watch` polls; the relay is a coroutine loop.
- A `SharedFlow` or `Channel` carrying ledger rows. Rows ride the cold flow of spec 7.3; a
  `SharedFlow` may only ever be the wake signal.
- A delivery claim in the ledger (IN_FLIGHT state), a retention sweep, a second replica.
- An attempt-history table, a payload column, a template engine for bodies.
- A plug into the connector's `SeenRepository`.
- Creation of the bucket or the tables at startup.
- A `RetryPolicy`, `Notifier`, `StageExecutor` or any other single-implementation interface.
- A `Source` interface, a source or target registry, or plugin discovery. The source seam is
  `IngestEvent` plus `Downloader`; a second source or target is one adapter and one DSL function.
- A second Maven module, a custom time abstraction, a logging facade.
- Streaming transfers, resume, content parsing for the notification body.

### 2.5 Concurrency rule

Everything in `pipeline` is `suspend`. Blocking calls, which are JDBI, the synchronous S3
client and `HttpClient.send`, run on one bounded view of `Dispatchers.IO` owned by the module
and sized to the route's parallelism; the connector owns its own for JSch. Per-file pipelines
run under one `SupervisorJob` scope per route, the relay under one scope per process, and no
lock is held across I/O anywhere. The relay's in-flight set is the only shared mutable state in
the module: a concurrent set touched at select and in `finally`, never elsewhere.

---

## 3. Phase Plan

```
G0 --+--> G1 --+--> G2 --> G3 --> G3b --+
     |         |                        |
     |         +--> G4 -----------------+---> G9 --> G10
     |                                  |
     +--> G5 (Oracle ledger) -----------+
     +--> G6 (S3 store) ----------------+
     +--> G7 (HTTP channel) ------------+
                                        |
     connector tickets 10 + 12 ---------+--> G8 (binding) --+
```

G0 freezes the surface. G1 to G4, with G3b, prove the whole behaviour against the test kit with no
socket, no container and no connector. G5 to G7 are the three technology adapters and run in
parallel from G0. G8 is the only phase that needs the connector to exist. G9 hosts everything
in Quarkus; G10 is the acceptance run.

---

### G0 - Skeleton, seams, DSL, boundary gates

- **Goal:** a compiling module with the entire fixed surface, so every later phase codes
  against final signatures.
- **Deliverables:** the `pipeline` value types and the five interfaces of 2.3; `IngestEvent`,
  `Downloader`, `DeliveryEvent`, `DeliveryOutcome`, `DeliveryPolicy` with spec 7.2 defaults;
  `TransferState` and delivery states; `Hook` with the seven named points of spec 4.4 and a
  no-op runner; the config DSL of spec 12.1 producing an immutable `IngestConfig`, with every
  validation rule of spec 12.1 wired; `IngestMetrics` holding the spec 13 names as constants;
  `ArchitectureTest` with the 2.2 sentences; `docs/sftpingest/progress.md` created with the
  sibling format.
- **Out of scope:** any behaviour. `FilePipeline`, `RouteConsumer` and `Relay` are shells.
- **Fixed contracts:** every signature above; I14.
- **Acceptance:** compiles; ArchUnit green; `I14_` rejects `apiCallTimeout >= drainTimeout`
  and a channel `timeout >= drainTimeout`; every other 12.1 rule has one rejecting test; a
  metric-name test asserts the spec 13 set verbatim.
- **Size:** small-medium.

### G1 - Test kit

- **Goal:** the instruments every later phase uses, no socket.
- **Deliverables:** `InMemoryLedger` with the same transaction semantics as spec 5.2, recording
  every call in order; `InMemoryTarget` returning a fresh reference per store, keeping exactly
  one copy per key and answering `verify` from it; `RecordingChannel` with scripted outcomes per call;
  `ScriptedSource` producing an `IngestEvent` flow from a script (files, poll boundaries,
  truncation, failures) and recording every ack and nack with its arguments; a scripted
  `Downloader` that materializes a file of given bytes or throws; `HookDriver` that suspends a
  pipeline at a named point and can cancel it there, which is how "crash after X" is played;
  a `ClockFixture` over `Clock.fixed` and `Clock.offset`.
- **Fixed contracts:** the fakes obey spec 5.2 and 6.2 exactly; a test of the fakes proves it.
- **Acceptance:** each fake has its own test; the hook driver demonstrably stops a sample
  coroutine at a named point and cancels it.
- **Size:** medium. Parallel with nothing; G2 and G4 depend on it.

### G2 - Per-file pipeline and entry points

- **Goal:** stages 0 to 4 of spec 4.1 against the test kit, with the entry-point rules of 4.3.
- **Deliverables:** `FilePipeline`: decide, download, quality, store,
  ledger UPLOADED, ack, ledger ACKED with deliveries; staging deletion on success and on every
  failure path; `attempts` and the FAILED flip at `maxAttempts`; REJECTED on quality Fail;
  `nack(redeliver = true)` for retryable errors and `redeliver = false` for REJECTED and
  FAILED; the four entry points of spec 4.3 including the re-ack of an ACKED or DONE file.
- **Fixed contracts:** I1, I2, I7, I9, I10, I11 (the ACKED half); spec 4.3 table.
- **Acceptance:** named tests for those invariants; S1, S10, S11, S12 against the fakes; a
  test per 4.3 row; a test that a quality Fail leaves the target untouched; `store` is called
  exactly once per successful run and `verify` exactly once per UPLOADED entry.
- **Size:** medium. The correctness phase; give it the review attention.

### G3 - Route consumer and reconciliation

- **Goal:** the collector around the pipeline, and the end-of-poll repair.
- **Deliverables:** `RouteConsumer`: collects an `IngestEvent` flow, bounds pipelines at
  `parallelism` under a `SupervisorJob`, counts `PollFailed` and `PollSkipped`, marks the route
  down on `RouteDown`; reconciliation at `PollCompleted` per spec 4.5 including the truncated
  listing rule; `stuck_files` refresh; the hook points wired into the pipeline so G3b can
  stop it anywhere.
- **Fixed contracts:** spec 4.5; the parallelism bound; a `PollFailed` never cancels a
  running pipeline.
- **Acceptance:** S14, S16; a test that `parallelism + 1` files run at most `parallelism`
  pipelines at once on the virtual clock; reconciliation marks ACKED exactly the UPLOADED
  rows older than the poll start and absent from a complete listing, and creates their
  deliveries; a `RouteDown` ends the collector with the route gauge at zero and no pipeline
  cancelled.
- **Size:** medium.

### G3b - Crash matrix replay

- **Goal:** every row of spec 4.4 survives a restart.
- **Deliverables:** the `I8_` test family: for each hook point, cancel the pipeline there, run
  a second poll from the same in-memory ledger and target, assert the end state and the
  extra-upload and extra-delivery counts the 4.4 table promises; any pipeline or consumer fix
  the replay forces, recorded in the progress entry.
- **Blocked by:** G3.
- **Fixed contracts:** I8; spec 4.4 row by row.
- **Acceptance:** `I8_` with one case per 4.4 row; S2, S3, S4, S5, S6.
- **Size:** small-medium, and pure state-machine reasoning: it deserves a session of its own
  rather than the tail of G3's.

### G4 - Relay

- **Goal:** spec 7.3 and 7.4 against the test kit.
- **Deliverables:** `Relay` as the cold flow with `buffer` and `flatMapMerge`; the in-flight
  set with add at select and remove in `finally`; the wake signal; outcome recording with
  backoff, `maxAttempts` and `giveUpAfter`; the DONE flip when every channel delivered;
  cancellation leaving rows PENDING.
- **Fixed contracts:** I3, I4, I5, I13; spec 7.2 policy table.
- **Acceptance:** named tests for those invariants; S7, S8, S9, S17 against the fakes; a test
  that the wake causes a select before the sweep interval elapses on the virtual clock; a test
  that cancellation mid-delivery leaves the row PENDING and the set empty.
- **Size:** medium. Parallel with G2 and G3.

### G5 - Oracle ledger adapter

- **Goal:** `JdbiLedger` over the spec 5.1 DDL on a real Oracle.
- **Deliverables:** `LedgerSchema.DDL` verbatim from spec 5.1; every `Ledger` method as one
  transaction; `due` with a skip-locked select bounded by `limit` and excluding the given ids;
  `unlisted` as one statement; the JDBI adapter's tests tagged `oracle` and excluded by a pom
  property like the sibling modules.
- **Fixed contracts:** spec 5.1 DDL; spec 5.2 signatures; I11 on Oracle.
- **Acceptance:** the same contract test class runs against `InMemoryLedger` and `JdbiLedger`
  and both pass; `I11_` shows a failing delivery insert rolls back the ACKED update; a unique
  identity violation on `seen` returns the existing row.
- **Size:** medium. Needs Docker.

### G6 - S3 target adapter

- **Goal:** `S3Target` over AWS SDK v2 against a versioned MinIO.
- **Deliverables:** client construction per spec 6.1 with checksums when-required, path style,
  endpoint override, timeouts; `store` as PUT with metadata, HEAD of the content length, then
  the prune of every other version by exact key, returning the version id as the reference;
  `verify` as a HEAD of key and version; `probe` as a HEAD of the bucket; tests tagged `minio`
  on Testcontainers with versioning enabled on the bucket.
- **Fixed contracts:** spec 6.1 signatures; spec 6.3; I6 on MinIO.
- **Acceptance:** the same contract test class runs against `InMemoryTarget` and `S3Target`;
  `I6_` stores three times and lists one version, and replays a crash between PUT and prune
  through a hook inside the adapter, after which the next `store` leaves one version; `verify`
  of a deleted version is false; a key with a sibling prefix is never pruned by the
  neighbour's store.
- **Size:** small-medium. Needs Docker.

### G7 - HTTP channel

- **Goal:** the declarative channel of spec 7.5 over `java.net.http`.
- **Deliverables:** the `http("name") { }` DSL block with method, URL, headers, auth from
  environment, timeout, body builder over a Jackson tree, response mapping and the reference
  pointer; `HttpChannel.deliver` mapping status and exceptions to `DeliveryOutcome`; INFO log
  per attempt with transfer id, channel, attempt, status, reference.
- **Fixed contracts:** spec 7.5 mapping rules; `CancellationException` is never converted.
- **Acceptance:** tests against a JDK `HttpServer` on loopback scripted per case: 200 with
  reference, 200 without the pointer resolving, 503, 429, 400, connection refused, a stalled
  response past the timeout; a body test proves JSON escaping of a file name containing quotes
  and backslashes.
- **Size:** small-medium. No Docker.

### G8 - Connector binding

- **Goal:** the real source: the connector's `watch` mapped onto `IngestEvent`, and its
  `download` onto `Downloader`.
- **Deliverables:** `SftpBinding`: `FileSeen` to `Seen` with the ack and nack passed through,
  `PollCompleted` to `PollCompleted(listed, truncated)`, `PollFailed` and `PollSkipped`
  counted, a terminated watch to `RouteDown`; the connector's `download` as the `Downloader`;
  route configuration handing `onAck = move(temp)` and readiness to the connector's DSL.
- **Blocked by:** G3, and connector tickets 10 (poll, ack, nack) and 12 (watch). This phase
  does not start until both connector tickets are merged.
- **Fixed contracts:** spec 9.
- **Acceptance:** against the connector testkit's embedded SSHD: one poll moves a file to
  `temp/` only after the in-memory target holds it; a file removed between listing and
  download yields no transfer beyond SEEN; a wrong password ends the flow with `RouteDown`.
- **Size:** small-medium.

### G9 - Quarkus host

- **Goal:** the running application of spec 11 and 12.
- **Deliverables:** `Producers` in the documented order; `HostConfig` mapping properties onto
  the DSL; `IngestHost` running spec 11.1 startup steps 1 to 7 and spec 11.2 shutdown in order
  from the shutdown event under `drainTimeout`; readiness per spec 11.1; `AdminResource` with
  the five endpoints of spec 12.3 under the admin role; the bounded IO dispatcher of 2.5;
  metrics bound to the host registry.
- **Blocked by:** G3b, G4, G5, G6, G7, G8.
- **Fixed contracts:** I12; spec 11 ordering; spec 12.3 endpoints.
- **Acceptance:** `I12_` measures close within `drainTimeout` with a delivery parked in a
  stalled loopback server; S15; S18; a boot with a missing table fails naming the DDL; a boot
  with a missing bucket fails; readiness is false until the startup steps pass and false again
  when a route is down; the admin re-drive endpoints change the ledger and wake the relay.
- **Size:** medium.

### G10 - Acceptance run

- **Goal:** the whole scenario table against the real adapters, and the open items re-checked.
- **Deliverables:** one suite running S1 to S18 end to end through the embedded SSHD,
  Testcontainers MinIO and Oracle, and a loopback HTTP server, each scenario named by its id;
  S13 at 5,000 files with the three measurements it names; spec 16 items re-checked, and the
  spec amended with a decision entry wherever a measurement contradicts it.
- **Blocked by:** G9.
- **Acceptance:** the suite is green; the progress entry records every deviation and every
  open item's status.
- **Size:** medium. Needs Docker.

---

## 4. Traceability

| Spec invariant | Phase |
|---|---|
| I1, I2, I7, I9, I10 | G2 |
| I6 | G6 |
| I11 | G2 (fake), G5 (Oracle) |
| I8 | G3b |
| I3, I4, I5, I13 | G4 |
| I12 | G9 |
| I14 | G0 |

| Spec scenario | Phase |
|---|---|
| S1, S10, S11, S12 | G2 |
| S14, S16 | G3 |
| S2, S3, S4, S5, S6 | G3b |
| S7, S8, S9, S17 | G4 |
| S15, S18 | G9 |
| S13 and the full table end to end | G10 |

---

## 5. Per-phase Agent Briefing

Each phase handed to an agent carries exactly this:

1. The spec sections it implements and this plan's phase entry.
2. The frozen signatures from G0.
3. The fixed assertions: invariant ids, scenario ids, the DDL, the state tables, stated as
   "assertions may not be altered".
4. The do-not-build list (2.4) and the concurrency rule (2.5).
5. The size budget and the instruction to stop and report if the budget or a frozen contract
   does not survive contact with reality, updating the document first.
