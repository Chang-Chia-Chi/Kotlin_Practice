# Shuttle - Implementation Plan

Version: v0.2
Companion to: `docs/shuttle/spec.md` v0.2 (Sec 1 to 19)
Purpose: break the application into phases small enough for an agent to implement in one
session each, with fixed contracts and fixed acceptance criteria, so implementations may vary
but assertions may not.

Phases are `G0` to `G19`. The repository already has `P` phases (snapshotcache), `E` phases
(SimpleEtl) and numbered tickets 01 to 15 (the connector); the letter disambiguates.

---

## 1. Ground Rules for Agent-Driven Implementation

1. **Fixed vs free.** The frozen surface from G0, the DDL (spec 8.1), the transfer and delivery
   states (spec 4.2, 9.1), the validation rules and their numbers (spec 13.3), the invariants
   and scenario table (spec 18), and every test assertion are FIXED. Internal structure,
   algorithms and private naming are free. An agent that believes a fixed contract is wrong
   stops and reports; it does not adapt.
2. **One phase, one change,** roughly 200 to 600 lines including tests. Past that, stop and split.
3. **Tests are deliverables.** Invariant tests are named `I<n>_<description>`; scenario tests
   by their `S<n>` id; validation tests by `rule<n>_`.
4. **No scope creep.** A stub throwing `NotImplementedError` is the placeholder for a later seam.
5. **No sleeps in tests.** Time is an injected `java.time.Clock`; coroutine tests run under
   `runTest`; interleavings use the declared `Hook` points.
6. **Documents win** unless `docs/shuttle/progress.md` records a deliberate deviation. Every
   phase appends an entry there. The root `CLAUDE.md` tiers of authority apply.
7. **Boundary.** A phase modifies only `shuttle/` and, when a measurement forces it,
   `docs/shuttle/`. Nothing here touches the connector's module or documents; a need the
   connector does not meet is an appeal in its progress log.

---

## 2. Architecture Overview

### 2.1 Design stance

One Maven module, `shuttle`, a Quarkus application in the shape of `etl-host`. Packages are the
boundary and ArchUnit enforces them from G0. The framework-free core is `infra.shuttle.core`;
every technology is an adapter package named for it. A seam exists only where a second
implementation is real, and every seam has one in the test kit.

### 2.2 Package layout and rules

```
shuttle/src/main/kotlin/infra/shuttle/
  core/     Transfer, TransferState, TransferKind, SourceIdentity, StagedObject, Payload, Digest,
            TargetRef, RouteEvent, Fetcher, StateStore, ObjectStoreTarget, DeliveryChannel,
            Processor, ProcessContext, Outcome, Provider, Hook, DeliveryEvent, DeliveryOutcome,
            DeliveryPolicy, MappingTable + MappingRenderer, built-in processors, RouteRunner,
            TransferPipeline, Notifier, RouteSupervisor, ShuttleConfig + DSL + Rules, ShuttleMetrics
  yaml/     YamlLoader: files -> ShuttleConfig through the DSL; ${VAR} resolution; rule reporting
  sftp/     SftpPollSource (connector watch -> RouteEvent, connector download -> Fetcher),
            SftpTarget (upload + rename; G17)
  s3/       S3Target (store = PUT + Content-MD5 + HEAD + ETag + prune, verify, probe), S3Fetcher
  http/     HttpChannel
  nats/     NatsChannel: subscribe trigger -> RouteEvent, publish (G15)
  jdbi/     JdbiStateStore, StateStoreSchema (the DDL text)
  quarkus/  Producers, ShuttleHost, ReadinessCheck, AdminResource, NamedBeans, ValidateCommand, TryCommand
```

ArchUnit sentences, enforced from G0:

- `core` depends on no other package of this module and on no technology: not the connector,
  JDBI, the AWS SDK, `java.net.http`, jnats, Jackson YAML or Quarkus. Allowed: kotlin-stdlib,
  coroutines, micrometer-core, jboss-logging, Jackson databind.
- `yaml` depends on `core` and Jackson YAML only.
- `sftp` depends on `core` and the connector core only. `s3` on `core` and the AWS SDK.
  `http` on `core` and `java.net.http`. `nats` on `core` and jnats. `jdbi` on `core` and JDBI;
  `java.sql` and `org.jdbi` appear nowhere else.
- `quarkus` may depend on everything above and is depended on by nothing.
- Logging is `org.jboss.logging.Logger` directly, everywhere; no context object carries a
  logger. Time is `java.time.Clock`, injected.

### 2.3 Public surface budget

Five seams, all in `core`, exactly as spec 3.4: `StateStore`, `ObjectStoreTarget`,
`DeliveryChannel`, `Processor`, `Hook`. `Provider` is a one-method interface for named beans and
`RouteEvent` a sealed class, `Fetcher` a function type. Everything else is a concrete class.

### 2.4 Do-not-build list

- A Quarkus `@Scheduled` anything; the notifier and the triggers are coroutine loops.
- A `SharedFlow` or `Channel` carrying state-store rows; a `SharedFlow` may only be the wake.
- A delivery claim in the state store, a retention sweep, a second replica.
- An attempt-history table, a payload column, an expression or template language (D30).
- A plug into the connector's `SeenRepository`.
- Creation of buckets, tables, directories or subjects at startup.
- A `Source` interface, a registry, plugin discovery, or a source-times-target matrix (D21).
- A `RetryPolicy`, `Notifier` interface, `StageExecutor`, or any other single-implementation
  interface.
- A logger in `ProcessContext` or any other context object (D34).
- A second Maven module, a custom time abstraction, a logging facade.
- Streaming transfers, resume, multiple targets per route.

### 2.5 Concurrency rule

Everything in `core` is `suspend`. Blocking calls (JDBI, the synchronous S3 client,
`HttpClient.send`, archive writing) run on one bounded view of `Dispatchers.IO` owned by the
module and sized to the sum of route parallelism; the connector owns its own for JSch.
Per-object pipelines run under one `SupervisorJob` scope per route, the notifier under one scope
per process, no lock is held across I/O anywhere, and the notifier's in-flight set is the only
shared mutable state in the module: a concurrent set touched at select and in `finally`.

---

## 3. Phase Plan

```
M1 (vendor-drop and mirror routes)

G0 --+--> G1 (yaml) -----------------------------------------------+
     +--> G2 (test kit) --+--> G4 --> G5 --> G6 --> G7              |
     +--> G3 (mapping) ---+                    |                    |
     |                    +--> G8 (notifier)   +--> G12 (sftp poll) |   <- connector tickets 10 + 12
     +--> G9 (oracle) ---------------------------------+------------+--> G13 (host) --> G14 (M1 accept)
     +--> G10 (s3) ------------------------------------+
     +--> G11 (http) ----------------------------------+

M2 (image-sets route)

G2 --> G15 (nats) ---------------+
G7, G10, G15 --> G16 (expand)    +--> G19 (M2 accept)
G12 + connector ticket 07 --> G17 (sftp target)
G8, G11 --> G18 (notification moments + callback)
G14 --> G19
```

G0 freezes the surface. G2 to G8 prove the whole behaviour against the test kit with no
socket, no container and no connector. G9 to G11 are technology adapters in parallel from G0.
G12 is the only M1 phase that needs the connector. G13 hosts everything; G14 accepts M1. M2
adds the subscription source, fan-out, the SFTP target and the remaining notification moments and the callback ack.

---

## 3a. Milestone 1

### G0 - Skeleton, frozen surface, DSL and rules, boundary gates

- **Goal:** a compiling module with the entire fixed surface, so every later phase codes
  against final signatures.
- **Deliverables:** every `core` type of 2.2 with the signatures of spec 3.4, 5, 6.1, 6.2,
  7.1, 8.2, 9.2, 9.3, 9.6; the Kotlin DSL of spec 13.2 producing an immutable `ShuttleConfig`;
  all 25 validation rules of spec 13.3 as one `Rules` object reporting every violation with its
  number; `Hook` with the seven points of spec 4.4 and a no-op runner; `ShuttleMetrics` with the
  spec 14.2 names; `ArchitectureTest` with the 2.2 sentences; `docs/shuttle/progress.md` created
  in the sibling format.
- **Out of scope:** any behaviour; `TransferPipeline`, `RouteRunner`, `Notifier`,
  `MappingRenderer` and every processor are shells.
- **Fixed contracts:** every signature above; I14.
- **Acceptance:** compiles; ArchUnit green; `I14_` proves every rule rejects a violating
  configuration by number, with a `rule<n>_` test each; defaults of spec 9.3 and 10 asserted;
  the metric-name set asserted verbatim.
- **Size:** medium; mostly declarations and rule tests.

### G1 - YAML loader and validate function

- **Goal:** the spec 13.1 file becomes a `ShuttleConfig` through the DSL, with the same rules.
- **Deliverables:** `YamlLoader` over Jackson YAML; `${VAR}` resolution from an injected
  environment map; duration and range parsing (`30s`, `1h`, `[200-299]`); every violation
  collected and reported with rule numbers; a pure `validate(files, env): Report` the host
  wraps in G13.
- **Blocked by:** G0.
- **Fixed contracts:** spec 13.1 grammar; rule 25 (secrets only by reference).
- **Acceptance:** the spec 13.1 document loads and equals the spec 13.2 DSL build for the
  vendor-drop route; S25 with five violations reports five numbers; a literal secret fails rule
  25; an unknown key is an error naming its path.
- **Size:** medium.

### G2 - Test kit

- **Goal:** the instruments every later phase uses, no socket.
- **Deliverables:** `InMemoryStateStore` with the transaction semantics of spec 8.2, recording
  every call; `InMemoryTarget` keeping exactly one copy per key and answering `verify`;
  `RecordingChannel` with scripted outcomes; `ScriptedSource` producing a `RouteEvent` flow from
  a script (objects, poll boundaries, truncation, failures, route down) and recording every ack
  and nack; a scripted `Fetcher`; `FakeProcessContext` over a temp directory; `HookDriver` that
  suspends a pipeline at a named point and can cancel it there; `ClockFixture`.
- **Blocked by:** G0.
- **Acceptance:** each fake has its own test; the hook driver demonstrably stops and cancels a
  sample coroutine at a named point.
- **Size:** medium.

### G3 - Mapping renderer and providers

- **Goal:** spec 9.6 as a pure function from a transfer row plus attributes to a JSON tree.
- **Deliverables:** `MappingTable` model; `MappingRenderer` handling `field`, `attribute`,
  `provider` with `select` and per-rendering memoization, `value`, `type`, `format`, `default`,
  `trim`, `upper`, `lower`, `required`; `check(table, declaredAttributes)` used at attribute
  freeze; the vocabulary as an enum; `Provider` resolution through an injected lookup function.
- **Blocked by:** G0.
- **Fixed contracts:** spec 9.6 row keys and vocabulary; I22; rules 16 to 19.
- **Acceptance:** `I22_`; every row key has a test; dotted paths nest; a file name containing
  quotes and backslashes is escaped; a missing required attribute reports the row; a provider
  returning an object mounts whole and `select` picks a piece.
- **Size:** medium. Parallel with G2.

### G4 - Processing chain and built-in processors

- **Goal:** spec 6 against the fake context.
- **Deliverables:** chain runner enforcing the four re-run rules of spec 6.2, attribute limits
  (rule 22), digest recomputation for new files, `SOURCE_DIGEST` versus `DIGEST`; built-ins
  `quality`, `rename`, `zip`, `unzip`, `extract` from file name, source path and content, `verifyDigest`; custom
  processor resolution through an injected lookup; attribute freeze followed by the G3 check
  against every channel the route notifies.
- **Blocked by:** G2, G3.
- **Fixed contracts:** spec 6.1 to 6.5; I15, I18.
- **Acceptance:** `I15_`, `I18_`; S20 on fakes; S21's positive half; S26; a processor that
  writes into its input is detected by the test kit; a chain of three runs in order and the
  payload cardinality after `unzip` is the entry count.
- **Size:** medium.

### G5 - Transfer pipeline, entry points, children

- **Goal:** stages 0 to 4 of spec 4.1 for one source object against the test kit.
- **Deliverables:** `TransferPipeline`: decide, fetch, process, store each object, STORED,
  ack, ACKED with `acked` deliveries or straight to DONE; children creation when the final
  payload has several objects, parent STORED on the last child, parent ack only; staging
  deletion on every path; `attempts`, FAILED, REJECTED, `nack` flags; the entry points of spec
  4.3 including `reacked`.
- **Blocked by:** G4.
- **Fixed contracts:** spec 4.1, 4.2, 4.3, 4.5; I1, I2, I7, I9, I10, I11, I16, I17.
- **Acceptance:** named tests for those invariants; S1, S10, S11, S12, S19 on fakes; a test
  per 4.3 row; `store` called exactly once per object per successful run and `verify` once per
  STORED entry.
- **Size:** medium; the correctness phase.

### G6 - Route runner, reconciliation, supervision

- **Goal:** the collector around the pipeline, the end-of-poll repair, and what the process
  does with a dead route.
- **Deliverables:** `RouteRunner` collecting a `RouteEvent` flow under a `SupervisorJob`,
  bounded at `parallelism`, counting `PollFailed` and `PollSkipped`; reconciliation per spec 4.6
  with the truncated-listing rule; `RouteSupervisor` restarting a route on `RouteDown` with
  backoff from `initial` to `max`, resetting on a successful trigger, exposing per-route up
  gauges and the readiness computation for both rules of spec 10; the stuck gauge.
- **Blocked by:** G5.
- **Fixed contracts:** spec 4.6 and 10; I19, I21.
- **Acceptance:** `I19_`, `I21_`; S14, S16, S23; `parallelism + 1` objects run at most
  `parallelism` pipelines on the virtual clock; a `PollFailed` never cancels a running pipeline;
  restart delays follow the backoff on the virtual clock.
- **Size:** medium.

### G7 - Crash matrix replay

- **Goal:** every row of spec 4.4 survives a restart.
- **Deliverables:** the `I8_` family: cancel at each hook point through the hook driver, run a
  second trigger from the same in-memory state store and target, assert the end state and the
  extra-store and extra-delivery counts; any fix the replay forces, recorded.
- **Blocked by:** G6.
- **Fixed contracts:** I8; spec 4.4 row by row.
- **Acceptance:** one `I8_` case per row; S2, S3, S4, S5, S6.
- **Size:** small-medium; pure state-machine reasoning, its own session.

### G8 - Notifier

- **Goal:** spec 9.3 to 9.5 against the test kit.
- **Deliverables:** `Notifier` as the cold flow with `buffer` and `flatMapMerge`; the in-flight
  set with add at select and remove in `finally`; the wake signal; policy outcomes with backoff,
  `maxAttempts`, `giveUpAfter`; the DONE flip when every delivery is DELIVERED; rendering
  through G3 at send time; cancellation leaving rows PENDING.
- **Blocked by:** G2, G3.
- **Fixed contracts:** I3, I4, I5, I13; spec 9.3 defaults.
- **Acceptance:** named tests; S7, S8, S9, S17, S22; the wake causes a select before the sweep
  elapses on the virtual clock; cancellation mid-delivery leaves the row PENDING and the set empty.
- **Size:** medium. Parallel with G4 to G7.

### G9 - Oracle state store

- **Goal:** `JdbiStateStore` over the spec 8.1 DDL on a real Oracle.
- **Deliverables:** `StateStoreSchema.DDL` verbatim; every seam method as one transaction;
  `due` skip-locked, bounded, excluding ids; `unlisted` as one statement; children and parent
  transitions; tests tagged `oracle`, excluded by a pom property.
- **Blocked by:** G0.
- **Fixed contracts:** spec 8.1; I11, I20 on Oracle.
- **Acceptance:** the shared contract test class passes against both the in-memory and the
  JDBI store; `I11_` and `I20_` show a failing delivery insert rolls back the transition; a
  unique-identity violation on `seen` returns the existing row.
- **Size:** medium. Needs Docker.

### G10 - S3 target and fetcher

- **Goal:** spec 7.2 against a versioned MinIO, plus the S3 `Fetcher` M2 will use.
- **Deliverables:** client per spec 7.2; `store` as PUT with `Content-MD5` when the digest is
  MD5, HEAD of content length, ETag check on single-part unencrypted objects, prune of every
  other version; `verify`; `probe`; the multipart threshold pinned; `S3Fetcher` streaming an
  object to staging with the digest; tests tagged `minio` with versioning on.
- **Blocked by:** G0.
- **Fixed contracts:** spec 7.1, 7.2; I6 on MinIO.
- **Acceptance:** the shared target contract test passes against the in-memory target and S3;
  `I6_` stores three times and lists one version, and replays a crash between PUT and prune
  through an adapter hook; a corrupted body is rejected by `Content-MD5`; `verify` of a deleted
  version is false; a sibling key is never pruned; the fetcher's digest matches the object's.
- **Size:** medium. Needs Docker.

### G11 - HTTP channel

- **Goal:** spec 13.1's `http` channel over `java.net.http`.
- **Deliverables:** `HttpChannel.deliver` mapping status, connection failure and timeout to
  outcomes per the response section; auth modes bearer, basic, header; the reference pointer;
  INFO log per attempt with transfer id, event, channel, attempt, status, reference; the body
  from the G3 renderer.
- **Blocked by:** G3.
- **Fixed contracts:** spec 9.2, 9.6; `CancellationException` never converted.
- **Acceptance:** against a loopback `HttpServer`: 200 with reference, 200 without the pointer
  resolving (WARN, null reference), 503, 429, 400, refused, stalled past the timeout; a body
  with quotes and backslashes escaped.
- **Size:** small-medium.

### G12 - SFTP poll source

- **Goal:** the real source for M1: the connector's `watch` mapped onto `RouteEvent`, its
  `download` as the `Fetcher`, the ack vocabulary of spec 5.3 for SFTP polls.
- **Deliverables:** `SftpPollSource`: `FileSeen` to `Seen` with ack and nack passed through,
  `PollCompleted` with listed identities and the truncated flag, `PollFailed` and `PollSkipped`,
  a terminated watch to `RouteDown`; `move`, `delete`, `none` mapped onto the connector's
  actions; readiness and staging handed to the connector's DSL; the `idleCutoff` pass-through.
- **Blocked by:** G6; connector tickets 10 and 12 merged.
- **Fixed contracts:** spec 5.1 to 5.3.
- **Acceptance:** against the connector testkit's embedded SSHD with the in-memory state store
  and target: one poll moves a file to `temp/` only after the target holds it; the mirror route
  deletes after store; a file removed between listing and fetch produces no transfer beyond
  SEEN; a wrong password ends with `RouteDown`.
- **Size:** small-medium.

### G13 - Quarkus host, validate and try modes, admin

- **Goal:** the running application of spec 12 and 14.
- **Deliverables:** producers in order; `ShuttleHost` running spec 12.1 startup and 12.3
  shutdown under `drainTimeout`; `NamedBeans` resolving custom processors and providers by CDI
  name; `ValidateCommand` and `TryCommand` for spec 12.2, the latter over the G4 chain and the G3
  renderer with the test kit's fake context; readiness per the configured rule; the seven admin
  endpoints of spec 14.1 under the admin role; the bounded IO dispatcher; metrics bound.
- **Blocked by:** G1, G7, G8, G9, G10, G11, G12.
- **Fixed contracts:** I12; spec 12 ordering; spec 14.1.
- **Acceptance:** `I12_`; S15, S18, S24, S25, S31 through the real host; boot fails naming the DDL
  on a missing table and the bucket on a missing bucket; readiness follows the rule; every
  admin endpoint changes what it says it changes.
- **Size:** medium.

### G14 - Milestone 1 acceptance

- **Goal:** S1 to S26 end to end on real adapters; open items re-checked.
- **Deliverables:** one suite through the embedded SSHD, Testcontainers MinIO and Oracle, and
  a loopback HTTP server; S13 at 5,000 files with its three measurements; spec 17 items 1 to 8
  and 11 re-checked and the spec amended where a measurement contradicts it.
- **Blocked by:** G13.
- **Size:** medium. Needs Docker.

---

## 3b. Milestone 2

### G15 - NATS channel: subscribe trigger and publish

- **Goal:** spec 5.1's `subscribe` and the NATS channel role.
- **Deliverables:** `NatsChannel`: a JetStream pull or push subscription mapped onto
  `RouteEvent` with `ack`, `term`, `nak`; message identity per spec 5.2; `deliver` as publish;
  the `SourceView` that `extract` reads with `from: message`; tests tagged `nats` on Testcontainers.
- **Blocked by:** G2.
- **Acceptance:** a message becomes one `Seen` with working ack and nak; a nak redelivers; a
  publish lands on the subject; a broker outage ends with `RouteDown`.
- **Size:** medium. Needs Docker.

### G16 - Expand, fetch and parent completion on fakes

- **Goal:** the image-sets route end to end against the test kit.
- **Deliverables:** `expand` processor reading paths from a metadata file or the message and
  fetching children through `ctx.fetch`; the route-level `fetch` for subscriptions; parent
  completion rules exercised with children stored in parallel; `extract` with `from: message`.
- **Blocked by:** G7, G10, G15.
- **Fixed contracts:** spec 4.5, 6.3; I16.
- **Acceptance:** S27, S28, S29 on fakes with the scripted fetcher; `I16_`.
- **Size:** medium.

### G17 - SFTP target

- **Goal:** spec 7.3.
- **Deliverables:** `SftpTarget`: upload to `<name>.part`, rename over with the connector's
  overwrite policy, `verify` by stat, `probe` through the connector; tests on the embedded SSHD.
- **Blocked by:** G12; connector ticket 07 merged.
- **Acceptance:** the shared target contract test passes; a crash between upload and rename is
  repaired by the next `store`; `I6_` on SFTP.
- **Size:** small-medium.

### G18 - Notifications and callback acks

- **Goal:** `fetched` and `stored` deliveries, and the `callback` ack action.
- **Deliverables:** outbox rows created in the FETCHED and STORED transactions; the
  `callback` ack calling a channel synchronously with the stage's retry; the ack-versus-
  notification rule enforced by rule 12.
- **Blocked by:** G8, G11.
- **Fixed contracts:** spec 9.1, 5.3; I20.
- **Acceptance:** `I20_` for all three events; S30; a `fetched` delivery exists after a crash
  right after fetch and is delivered by the notifier.
- **Size:** small-medium.

### G19 - Milestone 2 acceptance

- **Goal:** S27 to S30 end to end on NATS, MinIO, the embedded SSHD as the partner server, and
  the loopback HTTP server; open items 9 and 10 re-checked.
- **Blocked by:** G14, G15, G16, G17, G18.
- **Size:** medium. Needs Docker.

---

## 4. Traceability

| Invariant | Phase |
|---|---|
| I14 | G0 |
| I22 | G3 |
| I15, I18 | G4 |
| I1, I2, I7, I9, I10, I11, I16, I17 | G5 |
| I19, I21 | G6 |
| I8 | G7 |
| I3, I4, I5, I13 | G8 |
| I6 | G10 (S3), G17 (SFTP) |
| I11, I20 on Oracle | G9 |
| I12 | G13 |
| I20 all events | G18 |

| Scenario | Phase |
|---|---|
| S25 | G1, G13 |
| S20, S21, S26 | G4 |
| S1, S10, S11, S12, S19 | G5 |
| S14, S16, S23 | G6 |
| S2 to S6 | G7 |
| S7, S8, S9, S17, S22 | G8 |
| S15, S18, S24, S31 | G13 |
| S13 and S1 to S26 end to end | G14 |
| S27, S28, S29 | G16, G19 |
| S30 | G18, G19 |

---

## 5. Per-phase Agent Briefing

1. The spec sections it implements and this plan's phase entry.
2. The frozen signatures from G0.
3. The fixed assertions: invariant ids, scenario ids, rule numbers, the DDL, the state tables.
4. The do-not-build list (2.4) and the concurrency rule (2.5).
5. The size budget and the instruction to stop and report if the budget or a frozen contract
   does not survive contact with reality, updating the document first.
