# Shuttle - Progress Log

One entry per ticket, appended when the ticket is done. Later sessions read this to learn
what already exists and which deviations override the spec.

## Entry template

```
## <nn>: <ticket title>

**Built:** what exists now that did not before.
**Concepts named:** the domain vocabulary this ticket introduced, and where the seams went.
**Acceptance:** each checkbox from the ticket, with the test or command that proves it.
**Deviations:** every place the code differs from the spec, and why. "None" if none.
**For the next ticket:** seams left stubbed, gotchas, anything surprising.
```

A deviation recorded here overrides the spec for the code that already exists. A deviation
that is merely a shortcut is debt - say so, and say what would repay it.

---

## 01: Skeleton: frozen surface, DSL, validation rules, boundary gates

**Built:** the Maven module `shuttle` (`dynacache:shuttle`) in the parent reactor, compiling the
whole frozen surface of spec v0.4 with no behaviour. `infra.shuttle.core` holds every type of
plan 2.2 across ten files: `Transfer.kt` (ids, states, kinds, `Digest`, `SourceIdentity`,
`Transfer`, `TargetRef`), `Seams.kt` (the five seams and `Provider`), `RouteEvent.kt`,
`Processing.kt` (`StagedObject`, `Payload`, `Processor`, `ProcessContext`, `Fetcher`),
`Delivery.kt` (outbox row, `DeliveryEvent`/`Outcome`, `DeliveryPolicy`, `Backoff`, the mapping
table and `Field` vocabulary), `ShuttleConfig.kt` (the immutable model), `ShuttleDsl.kt` (spec
13.2), `Rules.kt` (the 25 rules as one `Rules.validate`), `ShuttleMetrics.kt` (spec 14.2
verbatim) and `Shells.kt` (`TransferPipeline`, `RouteRunner`, `Notifier`, `RouteSupervisor`,
`MappingRenderer` and seven built-in processors, every method throwing `NotImplementedError`).
`ArchitectureTest` enforces plan 2.2 from this ticket on. The pom imports `quarkus-bom` 3.17.5
for version management only, with kotlin-stdlib, coroutines and the Micrometer BOM stated
first as `etl-host/pom.xml` explains; `core`'s compile classpath is exactly kotlin-stdlib,
kotlinx-coroutines-core, micrometer-core, jboss-logging and jackson-databind. Tests use JUnit 5
assertions and Mockito only; no AssertJ.

**Concepts named:**

- **The five seams** are `StateStore` (spec 8.2 method for method, one transaction each),
  `ObjectStoreTarget` (`store`, `verify`, `probe`), `DeliveryChannel` (`name`, `policy`,
  `deliver`), `Processor` (`produces`, `process`) and `Hook`. `Hook` is spelled
  `suspend fun at(point: HookPoint, transfer: TransferId)` with `HookPoint` an enum of the seven
  spec 4.4 names in crash-matrix order and `Hook.None` the production no-op; the spec names the
  points but not the call shape, so this is the ticket's choice and is now frozen.
- **`ProcessContext` and `Provider`** are interfaces the spec itself declares (6.2, 9.6) and are
  not seams in the pipeline's sense; `Provider` is a `fun interface` with one method.
- **Configuration is data, behaviour is the seam.** `ProcessorSpec` (sealed) is what YAML and
  the DSL describe; `Processor` is what runs. `ProcessorSpec.Extract.produces` derives the
  attribute names from the regex's named groups, the `into` list or the JSON map keys, and that
  is what rules 17 and 22 count as "declared".
- **`Rules.validate(config, beans)`** returns `Report(violations: List<Violation(rule, message)>)`
  with every violation collected. `beans` answers what a named bean produces, or null when the
  name is unknown; tests pass a map, the host will pass CDI.
- **`DeliveryRequest(moment, channel)`** is what a transition hands the state store to create
  outbox rows; `DeliveryMoment` is `on_state`, `DeliveryState` is `notification_state`.
- **`StagedSummary`** is the row-side view of a `StagedObject` (everything but the local path),
  so the seam never sees a `Path` it must not keep.
- **`Digest`** is algorithm plus hex; its companion exposes `Digest.MD5` and friends as
  `DigestAlgorithm` constants so the DSL reads `digest = Digest.MD5` exactly as spec 13.2 writes it.
- **v0.4 knobs:** `Route.recheckFinished` (24 h), `Staging(dir, minFree = 1.gib)` replacing the
  bare path, `ProcessorSpec.Unzip(maxEntries = 10_000, maxBytes = 10.gib)`; `Int.gib` is the
  one byte-size helper.

**Acceptance:**

- *Module builds in the reactor; core depends only on the five libraries* - `mvn -B -o -q -pl
  shuttle test` green from the reactor root; `ArchitectureTest.core depends on no other package
  of the module and on no technology` holds the allow-list to exactly those packages.
- *Every type of plan 2.2 with the spec's signatures; five seams only* - compiles against the
  signatures of spec 3.4, 5, 6.1, 6.2, 7.1, 8.2, 9.2, 9.3, 9.6; `SurfaceTest.hook_points_of_spec_4_4_in_order`.
- *DSL builds an immutable configuration; `rule<n>_` for every rule; I14* -
  `RulesTest.rule1_` to `rule25_` each reject a configuration bent on one rule and report that
  number alone; `RulesTest.the_baseline_passes_every_rule` is the I14 half that proves the spec
  13.2 build passes all 25 (30 tests in the class).
- *Defaults of 9.3 and 10; metric names verbatim including the two staging meters* -
  `SurfaceTest.defaults_of_spec_9_3_and_10`, `SurfaceTest.metric_names_of_spec_14_2_verbatim`.
- *v0.4 knobs with defaults; rule 7 and 14 bounds* - `RulesTest.rule7_recheckFinished_is_not_negative`,
  `rule7_staging_minFree_is_not_negative`, `rule14_unzip_maxEntries_is_at_least_one`,
  `rule14_unzip_maxBytes_is_positive`; defaults in `SurfaceTest.defaults_of_spec_9_3_and_10`.
- *ArchUnit states every sentence of plan 2.2, including no logger in a context object* -
  `ArchitectureTest`, seven tests: core isolation, each adapter's technology, quarkus depended on
  by nothing, `java.sql`/JDBI only in `jdbi`, jboss-logging and no `Logger` in a `*Context`,
  `Clock` injected (no `Instant.now`, `Clock.system*`, `currentTimeMillis`). The adapter rules
  use `allowEmptyShould` because those packages do not exist yet; a first test asserts the core
  classes were imported so the rules cannot pass vacuously.
- *progress.md in the sibling format* - this file.

Final run: ArchitectureTest 7, RulesTest 30, SurfaceTest 3; 40 tests, 0 failures, 0 errors.

**Deviations:**

1. **JVM target 17, not the reactor's 21.** The module will depend on the connector, whose
   host runs JDK 17 (connector C2); the shuttle spec names no JDK, so the lower bound wins.
   `release` rather than `target`, so a JDK 21 API fails here and not on deployment.
2. **Size: 1,108 main lines (904 neither blank nor comment), 341 test, 81 pom, against a
   200 to 600 budget.** The frozen surface is every type the spec declares plus 25 rules with a
   test each; declarations dominate and none could be deferred without a later ticket changing
   a frozen signature. Debt: none; the count is the surface's, not padding.
3. **Rule 2 rejects `poll` on an S3 store and `subscribe` on an HTTP channel.** Spec 5.1 says
   poll on S3 "is a later adapter" and only NATS subscribes, so the rule reads "the adapter
   implements that role" as: poll needs an SFTP store, subscribe a NATS channel, target and
   fetch accept either store kind. Loosen the predicate when the S3 poll adapter lands.
4. **Rule 11's "local disk" is a filesystem-type deny-list** (`nfs`, `cifs`, `smb`, `sshfs`,
   `afs`) read from `Files.getFileStore`. ponytail: a heuristic; the upgrade path is an explicit
   allow-list once the deployment's volume type is known. Rule 11 also touches the real
   filesystem, so `RulesTest` builds its baseline on a `@TempDir`.
5. **`MappingRow.field` and `.digest` are strings, not enums.** Rules 16 and 21 must be able
   to name a wrong value that YAML produced; the renderer (ticket 04) parses them once.
6. **The `quarkus-bom` is imported by G0**, ahead of ticket 14, so Jackson, Mockito and JDBI
   versions resolve from one place for every ticket in between. No Quarkus dependency and no
   Quarkus plugin yet; `ArchitectureTest` forbids `io.quarkus..` and `jakarta..` outside `quarkus`.

**For the next ticket:**

- **02 (YAML):** build onto the DSL builders, not the data classes: every `@ShuttleDsl` builder
  is public with an `internal build()`, same module. `staging` is an object (`dir`, `minFree`);
  `recheckFinished` is a route key; `unzip` takes `maxEntries` and `maxBytes`; byte sizes like
  `1g` need a parser (there is none yet, only `Int.gib`). `Rules.validate` is the whole
  validate function once the YAML is a `ShuttleConfig`; rule 11 will need real directories in
  tests. Rule 25 is judged on `Secret.Literal`, so the loader must produce `Secret.Env` for
  `${VAR}` and `Secret.Literal` for anything else.
- **03 (test kit):** implement `StateStore`, `ObjectStoreTarget`, `DeliveryChannel`, `Hook`
  and `ProcessContext` from `Seams.kt` and `Processing.kt`; produce `RouteEvent` directly.
  `RouteEvent.Seen` is a plain class carrying two suspend lambdas, so it has no structural
  equality - compare identities. `Fetcher` is a typealias
  `suspend (path, into, algorithm) -> StagedObject`.
- **04 (renderer):** `MappingRenderer.render(table, transfer, event)` and
  `check(table, declaredAttributes): List<String>` are the shells; `Field` is the vocabulary
  enum; rules 16, 18, 19 and 21 already validate rows statically, so `check` owns rule 17 at
  attribute freeze only.
- **Shells** to fill: `TransferPipeline` (06), `RouteRunner` and `RouteSupervisor` (07),
  `Notifier` (09), `MappingRenderer` (04), the seven processors (05; `UnzipProcessor` already
  takes its `ProcessorSpec.Unzip`).
- **Gotchas:** JVM 17, so no Java 21 API. `kotlinx-coroutines-test` and `mockito-core` are on
  the test classpath already. ArchUnit's `coreAllowed` list is the one place to extend if core
  legitimately needs another package of the JDK. Tests are JUnit 5 assertions only; AssertJ is
  not on the classpath by design.

---

## 02: YAML loader and the validate function

**Built:** `infra.shuttle.yaml.YamlLoader`, one object over Jackson YAML: `load(text | texts, env)`
turns a spec 13.1 document into the `ShuttleConfig` the spec 13.2 builders produce, and
`validate(files, env, beans): Report` is the pure half of spec 12.2's validate mode. The loader
walks the parsed tree with a private `Node` that remembers every key it was asked for, so one
`done()` pass names every key nobody asked for by its dotted path. Values are handed to the
existing `@ShuttleDsl` builders, so every default lives in exactly one place and the YAML
grammar cannot drift from the DSL. `${VAR}` resolves from the injected map: standing alone in
a secret position it becomes `Secret.Env`, anywhere else it is substituted into the string, and
a missing variable is a load error naming it. `Report` gained `errors: List<String>` so load
errors and rule violations travel in one value. Test resource `spec-13-1.yaml` is the spec's
document verbatim (one quoting fix, deviation 3).

**Concepts named:**

- **Load error versus rule violation.** A load error means the document is not a configuration
  (unknown key, unparseable value, unset variable) and names a YAML path; a violation means the
  configuration breaks a numbered rule. `validate` reports load errors alone when the document
  never became a configuration, because rules judged on a half-read document would mislead.
- **`Node.one(...)`** is the reading of a kind-keyed mapping: `sftp:`/`s3:` under a store,
  `http:`/`nats:` under a channel, `poll:`/`subscribe:` under a source, the one processor name
  of a step, the one ack action of an `onAck` object. Zero or several keys is an error.
- **Parsers as `Node` methods**: `dur` (`kotlin.time.Duration.parseOrNull`, so `30s`, `15m`,
  `1h`, `24h`), `bytes` (`512m`, `1g`, `10g`, binary units), `statuses` (`[200-299]`,
  `[408, 429, 500-599]`), `word` (closed vocabularies: readiness, digest, method, `on`, `from`,
  `type`), `secret`. `free(key)` takes a bean's `custom.config` subtree whole, so its keys are
  never "unknown".
- **Several files deep-merge**, later keys winning, so a site file can complete a base file.

**Acceptance:**

- *Spec 13.1 loads, passes all 25 rules, equals the 13.2 DSL build for vendor-drop* -
  `YamlLoaderTest.the_spec_13_1_document_loads_passes_every_rule_and_equals_the_dsl_build_for_vendor_drop`
  (the resource's staging paths are pointed at a temp directory for rule 11; the DSL route adds
  `maxAttempts = 5`, `stuckAfter = 3.hours` and `recheckFinished = 24.hours`, which the YAML
  states and the 13.2 excerpt omits).
- *Rule 9 counts every role; omitted parallelism is 1* -
  `rule9_counts_every_role_and_a_route_without_parallelism_as_one` (pool 4 holds exactly the
  routes counted with 0, overflows counted with 1).
- *`${VAR}` from the injected map; literal secret fails rule 25* -
  `a_missing_environment_variable_is_a_load_error_naming_it`, `rule25_a_literal_secret_fails`,
  and every other test's `${SFTP_USER}`/`${SFTP_PASSWORD}`.
- *S25* - `S25_validate_mode_reports_five_rule_numbers_in_one_report`: rules 1, 3, 7, 12, 25
  from one file through `validate`; nothing in the package can open a connection (it imports
  `core` and Jackson only, `ArchitectureTest.each adapter depends on core and its own technology only`).
- *Unknown key names its path; durations, byte sizes, ranges parse* -
  `an_unknown_key_is_an_error_naming_its_path`, `durations_byte_sizes_and_status_ranges_parse`,
  `validate_mode_reports_load_errors_when_the_document_is_not_a_configuration`.
- *v0.4 knobs with defaults* - `v0_4_knobs_load_with_the_spec_defaults_when_omitted`.
- *Progress entry* - this entry.

Final run: ArchitectureTest 7, RulesTest 30, SurfaceTest 3, YamlLoaderTest 10; 50 tests, 0 failures.

**Deviations:**

1. **`core.Report` gained `errors: List<String>` (default empty); `ok` requires both lists
   empty.** The ticket asks for one report; a second report type in `yaml` would have made the
   host merge two. Existing `Rules` callers are unchanged.
2. **Bare `${VAR}` values are quoted before parsing.** YAML forbids `{` in a plain scalar inside
   a flow mapping, so spec 13.1's `auth: { user: ${SFTP_USER}, ... }` is not YAML as written;
   SnakeYAML rejects it. One regex quotes a `${VAR}` that stands as a whole value so the spec's
   document, and the way operators will write it, loads verbatim. A reference embedded in a
   longer flow-context value (`prefix-${VAR}`) still needs the operator's quotes.
3. **Spec 13.1 amended: `files: "/images[*].path"` is now quoted.** Same YAML rule, `[` in a
   flow mapping's plain scalar; a JSON-pointer-with-wildcard is rare enough that quoting in the
   document beats widening the pre-pass. The test resource carries the same line.
4. **Spec observation, no code change:** the 13.1 document passes rule 17 only if the
   `imageResizer` bean declares that it produces `orderNumber`. `downstream`'s body reads
   attribute `orderNumber`, and `image-sets` notifies `downstream` but only `extract` declares
   `batchId`. The test's bean lookup says so; a deployment must too, or drop that row from a
   shared channel. Worth a line in the spec when 13.1 is next touched.
5. **`validate` runs the rules only on a document that loaded clean** (see Concepts). The
   ticket's "every violation reported with its rule number in one report" holds for any document
   that is a configuration; a document that is not gets its load errors, all of them, instead.
6. **Size: 323 main lines, 228 test lines, 108 lines of verbatim spec resource.** At the top of
   the budget with the resource excluded; the resource is the spec's text, not code, and the
   main count is one loader plus the `Node` helper, no second model of the configuration.

**For the next ticket:**

- **14 (host):** `YamlLoader.validate(files, env, beans)` is the whole of validate mode's
  logic: hand it the file paths, `System.getenv()` and a CDI-backed `beans` lookup
  (`(name) -> Set<String>?`, the attributes a named `Processor` bean produces, or null when no
  bean has the name); print `report.errors` then `report.violations` and exit non-zero if
  `!report.ok`. `load` throws `YamlLoadException(errors)` for boot.
- **Everyone:** `Report(violations, errors)`; `report.ok` is the one flag. The `yaml` package
  imports only `core` and `com.fasterxml.jackson..`; the byte-size vocabulary is `k`, `m`, `g`,
  `t` (binary) with an optional `b`; durations are whatever `kotlin.time.Duration.parseOrNull`
  accepts. YAML `on:` as a key is fine under Jackson (field names are taken as text).
- **Gotcha:** the loader is strict about unknown keys everywhere except inside `custom.config`;
  a new configuration knob must be read by the loader in the same ticket that adds it to the
  DSL, or every document using it fails to load.

---

## 03: Test kit: fakes, scripted source, hook driver

**Built:** `infra.shuttle.testkit` under `shuttle/src/test/kotlin/infra/shuttle/testkit/`, the
module's own test sources: the module has no test-jar and every later ticket's tests live beside
it, so the kit is plain test code and `ArchitectureTest` (which imports main classes only) never
sees it. Seven fakes, one class each, no interface beyond the frozen seams:

- `HookDriver : Hook` - `pauseAt(point)`, `awaitArrival(point): TransferId`, `resume(point)`,
  `cancelAt(point)`, `crash(point)`. A point not paused is a no-op. A pause is one-shot: every
  coroutine reaching the point suspends on one `CompletableDeferred`; `resume` completes it,
  `crash` completes it with a `CancellationException` thrown inside the paused coroutine,
  `cancelAt` cancels the arrived jobs; all three disarm the point so the next run passes. A second
  `resume` is a no-op. `awaitArrival` on a point that is not paused throws.
- `InMemoryStateStore(clock) : StateStore` - every 8.2 method. `tx` takes the `Mutex`, records a
  `Call(method, args)` into `calls`, snapshots both tables and restores them if the method
  throws, so a transition is all or nothing. `failNextDeliveryInsert` (one-shot) makes the next
  delivery-row insert throw an `IOException` inside the transaction. Inspection: `transfers`,
  `outbox`, `transfer(id)`.
- `InMemoryTarget(location) : ObjectStoreTarget` - one copy per key, fresh `TargetRef` per
  `store` (`ref = "v<n>"`), `verify` true only for the current ref at its key, `probe` no-op,
  `calls`, `failNextStore` (one-shot, throws before writing), `bytes(key)`, `metadata(key)`, `keys`.
- `RecordingChannel(name, policy, vararg outcomes) : DeliveryChannel` - outcomes in order, the
  last repeats, default `Delivered(null)`; `events` records every `DeliveryEvent`.
- `ScriptedSource(clock)` - `seen(identity[, source])`, `pollCompleted(listed[, truncated])`,
  `pollFailed(cause)`, `pollSkipped()`, `routeDown(cause)`, chained; `events(): Flow<RouteEvent>`
  is cold and replays; each `Seen`'s `ack`/`nack` record into `acks` and `nacks` (`Nack(identity,
  redeliver)`). `ScriptedSource.identity(name, ...)` builds a poll identity with defaults.
- `ScriptedFetcher(clock) : Fetcher` - `file(path, bytes)`, `gone(path)`, `failNext`; copies the
  bytes into the requested path, digests with the requested algorithm, names the object after
  the path's last segment, `mtime` from the clock; `calls`.
- `FakeProcessContext(dir, fetcher, clock, ...) : ProcessContext, AutoCloseable` -
  `newStagedFile` allocates `<n>-<name>` in `dir` and appends to `createdFiles`; `fetch` delegates
  to the fetcher into a new staged file; `attributes` is the record of `setAttribute`;
  `snapshot(payload)` then `inputsUntouched()` detects a processor writing into an input (size
  plus MD5); `close()` deletes every created file.
- `ClockFixture(start) : Clock` - `advance(kotlin.time.Duration)`, `set(instant)`. It is the wall
  clock the module reads (`updated_at`, `next_attempt_at`, "older than"); `runTest`'s virtual
  time drives `delay` only. A test that wants both moves them together.

**Concepts named:** *transaction* in the kit is `InMemoryStateStore.tx`: lock, record, snapshot,
run, restore on throw. *Gate* is one paused hook point. *Fingerprint* is the input snapshot.
The seams stayed where ticket 01 froze them; the kit's only additions are inspection and
injection knobs on the fakes.

**Acceptance:**

- *State store: every seam method has a test; atomicity; `seen` returns the existing row; `due`
  excludes and limits; `unlisted` exact; parent STORED when the last child is* -
  `InMemoryStateStoreTest` (13): `I11_a_failing_delivery_insert_leaves_the_transfer_state_unchanged`,
  `seen_creates_a_SEEN_row_and_returns_the_existing_row_for_a_known_identity`,
  `due_orders_by_next_attempt_excludes_ids_and_honours_the_limit`,
  `unlisted_is_exactly_the_STORED_rows_older_than_the_instant_and_not_listed`,
  `children_replace_earlier_children_and_the_parent_is_STORED_when_the_last_child_is`; the rest
  cover `supersede` (I24), `acked` to DONE (I17), `delivered`/`deliveryFailed`/`redriveDelivery`,
  `failedAttempt`/`redrive`, `rejected`, a child failing its parent (I16), `stuck`, `retryLater`.
- *Target: fresh ref, one copy, verify, count* - `InMemoryTargetTest.I6_...` and
  `a_failed_store_writes_nothing_and_the_switch_is_one_shot`.
- *Channel and source* - `RecordingChannelTest.S7_...`, `ScriptedSourceTest.emits_the_scripted_flow_...`
  (all five event kinds, complete and truncated poll, every ack and nack with its flag) and
  `the_fetcher_copies_scripted_bytes_digests_them_and_can_fail_or_report_a_file_gone`.
- *Fake context* - `FakeProcessContextTest.I18_allocates_staged_files_...` and
  `I18_detects_a_processor_writing_into_an_input`.
- *Hook driver, no sleeps* - `HookDriverTest`: a coroutine observed suspended at `afterStore`
  after `advanceUntilIdle` (flag false, job active), then resumed and finished; a second cancelled
  there with the `CancellationException` caught and the code after the point never run; a third
  `crash`ed, then the disarmed point passed.
- *Progress entry* - this.

Final run: ArchitectureTest 7, RulesTest 30, SurfaceTest 3, ClockFixtureTest 1,
FakeProcessContextTest 2, HookDriverTest 3, InMemoryStateStoreTest 13, InMemoryTargetTest 2,
RecordingChannelTest 2, ScriptedSourceTest 2; 65 tests, 0 failures, 0 errors.

**Deviations:**

1. **Size: 496 lines of fakes plus 530 of tests, against 200 to 600.** Seven fakes and a store
   with eighteen methods, each with its own test class as the acceptance demands; nothing is
   padding, and the store's rollback is one generic `tx`.
2. **Identity resolution ignores `revision`.** `find`, `seen` and `unlisted` compare identities
   with `revision` normalised and return the latest revision, because a listing always carries
   revision 1 and the runner must see the row `supersede` created (spec 4.3, S12). Children are
   never found by identity. Ticket 10 should read the same way or the fakes and Oracle diverge.
3. **A child's `stored` attaches its `events` to the parent**, created only in the call that
   flips the parent STORED (D42's conditional update), so a parent gets one set of `stored` rows
   however many children. `children()` replaces the parent's existing children (4.5 re-drive).
4. **The outbox row's `attempts` is counted by the store**: `delivered`, `retryLater` and
   `deliveryFailed` each add one; `redriveDelivery` resets it to 0 and `next_attempt_at` to now.
   DONE requires every row DELIVERED, so a FAILED row keeps the transfer ACKED (D9).
5. **`failedAttempt` on a child at `maxAttempts` marks the parent FAILED** in the same
   transaction (spec 4.5, I16).
6. **Rollback is a whole-table snapshot per transaction** (ponytail: an undo log if tables grow;
   they will not in tests).

**For the next ticket:**

- **Pausing and crashing a pipeline (07, 08):** `val hook = HookDriver(); hook.pauseAt(afterStore)`;
  launch the pipeline with `hook`; `hook.awaitArrival(afterStore)` suspends until it gets there;
  assert on the store and target while it is parked; then `hook.resume`, `hook.crash` (the
  pipeline sees a `CancellationException` at the point, exactly the process dying) or
  `hook.cancelAt`. Re-arm with `pauseAt` for the next run. Never leave a point paused at the end
  of a test: `runTest` fails on the parked coroutine.
- **Making a delivery insert fail (06, 09, I11/I20):** `store.failNextDeliveryInsert = true`
  before the transition; the call throws `IOException` and the row is as it was. One-shot.
- **Crash inside `store` (06, 08):** `target.failNextStore = true`, one-shot, nothing written.
- **Reconciliation (07):** `unlisted` is judged on `updated_at < olderThan`, so advance
  `ClockFixture` between storing and `PollCompleted`; `ScriptedSource.pollCompleted` stamps
  `startedAt` from the clock at script time.
- **`ScriptedSource.events()` is cold**: each collection replays the same `Seen` instances, so
  `acks`/`nacks` accumulate across runs; compare `Seen` by `identity`.
- **`FakeProcessContext`** wants `snapshot(payload)` before the chain and `inputsUntouched()`
  after; use it in a `use {}` so `close()` deletes created files (I18). Default `transfer` and
  `source` are a one-file poll on route `drop`; pass your own for children or messages.
- **Gotcha:** `InMemoryStateStore.calls` and the fakes' lists are plain or synchronized lists
  meant to be read after `advanceUntilIdle`, not while pipelines run.

---

## 04: Mapping renderer and providers

**Built:** `MappingRenderer.kt` in `core` replaces the G0 shell: `render(table, transfer, moment,
attempt): JsonNode`, a pure suspend function from the row plus its frozen attributes to a Jackson
tree, and `MappingRenderer.check(table, declaredAttributes?, providerExists): List<Violation>`,
the spec 9.6 boot checks by rule number. `Rules` now delegates every row check to `check` (rules
15, 16, 18, 19, 21 per channel; rule 17 per route), so the pipeline's attribute-freeze check and
validate mode are one implementation. `MappingFailure(path, detail)` is the one exception: a
required row with no value, an unresolvable provider, or a value the row's `type` cannot coerce.
`MappingRendererTest`, twelve tests.

**Concepts named:**

- **Missing** is: the source has no value, a `select` points at nothing, or the text is blank after
  `trim`. `default` fills a missing value first; only then is `required` judged, so a row with a
  default is never missing and `required: false` omits the path.
- **A provider's node keeps its own JSON type.** Mounted whole it is set as-is; a selected scalar
  is set as-is too unless the row states `trim`, `upper`, `lower` or a non-string `type`, in which
  case it goes through the same text pipeline as every other value. Providers are resolved by an
  injected `(String) -> Provider?` and memoized per rendering (I22).
- **Formats** are `DateTimeFormatter`: `ISO_*` names by reflection on the class's constants, else
  `ofPattern`; instants render in UTC and default to `ISO_INSTANT`. `check` and `render` share the
  one `formatter` function, so rule 18 rejects exactly what render could not format.
- **`SOURCE_PATH`** is `sourceRef/sourceName` (no column of its own in spec 8.1). `KIND`,
  `SOURCE_KIND`, `DIGEST_ALGO`, `EVENT` render lowercase.

**Acceptance:**

- *Every row key has a test; dotted paths nest; quotes and backslashes escaped* -
  `field_rows_read_the_transfer_row_and_dotted_paths_nest`, `attribute_and_value_rows`,
  `a_provider_mounts_whole_and_select_picks_a_piece`, `type_coerces_to_number_and_boolean`,
  `format_renders_an_instant_and_defaults_to_ISO_INSTANT`,
  `default_applies_before_required_and_required_false_omits_the_path`,
  `trim_upper_and_lower_transform_the_value`, `a_name_with_quotes_and_backslashes_survives_serialisation`.
- *I22* - `I22_a_provider_selected_by_three_rows_is_invoked_once` (three paths, one invocation; S22's fakes half).
- *Missing required reports the row; `required: false` omits; default before required* -
  `a_missing_required_value_reports_the_row`, `default_applies_before_required_and_required_false_omits_the_path`.
- *check rejects undeclared attribute (17), unknown field (16), unregistered provider (15), invalid
  pointer (18), unparseable format (18) by number* - `check_rejects_each_bad_row_by_rule_number`
  (also 19 and 21), `check_without_declared_attributes_skips_rule_17`; `RulesTest` rule15_ to
  rule21_ still green through the delegation.
- *Progress entry* - this entry.

Final run: ArchitectureTest 7, MappingRendererTest 12, RulesTest 30, SurfaceTest 3; 52 tests, 0 failures.

**Deviations:**

1. **`render` takes `(table, transfer, moment, attempt)`, not the shell's `(table, transfer,
   event: DeliveryEvent?)`.** `DeliveryEvent` carries the rendered body, so it cannot be the
   renderer's input; the notifier renders, then builds the event. `attempt` defaults to 1 for try mode.
2. **`check` returns `List<Violation>` and lives on the companion**, not the shell's
   `List<String>` on the instance: the ticket asks for rejection by rule number, and `Rules` needs
   it without a provider lookup.
3. **Number `format` is not implemented.** Spec 9.6 says `format` covers "timestamps and numbers";
   rule 18 as built accepts only `DateTimeFormatter` input, and no channel in spec 13.1 formats a
   number. Debt: add a `DecimalFormat` branch behind rule 18 when a channel needs one.
4. **`digest: <algo>` on a `field: DIGEST` row renders the row's digest only when the algorithms
   match**; otherwise the value is missing. A second algorithm needs a home on the transfer row
   (spec 6.5 computes it in the same stream); that column does not exist in 8.1 and is ticket 06's
   question if a channel asks for it.
5. **Blank after `trim` counts as missing.** The spec does not say; the alternative sends `""` to a
   receiver that declared the field required.
6. **Size:** 129 main lines, 163 test, against a 200 to 600 budget: in budget.

**For the next ticket:**

- **05 (attribute freeze):** call `MappingRenderer.check(channel.body, frozenAttributes) { beans(it) != null }`
  for every channel the route notifies; a non-empty list fails the transfer before the store,
  naming the row (the message starts with `row <path>:`). Attribute *presence* is not a check
  concern: a declared-but-unset attribute surfaces at render time as `MappingFailure` unless the
  row has a `default` or `required: false`; if spec 6.4's "missing required attribute fails before
  the store" must be proven at freeze, render each notified channel once at freeze with
  `moment = ACKED` and let `MappingFailure` fail the transfer (the renderer is pure, so the dry run costs nothing).
- **09 (notifier):** `MappingRenderer(providers).render(channel.body, transfer, delivery.moment,
  delivery.attempts + 1)` at send time, then `DeliveryEvent(..., body)`. `MappingFailure` at send
  time is a `Reject`-shaped outcome (configuration, not transport).
- **12 (HTTP):** the body is the `JsonNode`; `ObjectMapper.writeValueAsBytes(node)` is the whole
  serialisation, escaping included.
- **14 (try mode):** `render(table, sampleTransfer, DeliveryMoment.ACKED)` per notified channel.


---

## 05: Processing chain and built-in processors

**Built:** `ProcessingChain.kt` and `Processors.kt` in `core`; the processor shells left `Shells.kt`
(only `ExpandProcessor` remains a shell, for ticket 16/17).

- `ProcessingChain(processors, algorithm).run(payload, ctx): ChainResult` runs the chain in order.
  `Outcome.Reject` ends it as `ChainResult.Rejected(reason)`; a processor that throws becomes
  `StageError(stage, cause)` (retryable, spec 11); `CancellationException` passes through. When
  the chain ends the attributes are frozen into an unmodifiable copy, rule 22 is judged on them
  (more than 32, a name over 64 characters, or over 1 KB is a `Rejected` naming rule 22), and every
  object whose file is not one of the inputs gets `size` and `digest` recomputed from its bytes;
  the result is `ChainResult.Done(payload, attributes)`.
- `ProcessingChain.checkMappings(attributes, tables, providerExists)`: spec 6.4 at freeze. For each
  notified channel's table, `MappingRenderer.check` (rules 15, 16, 18, 19, 21) and then every
  `attribute` row that is `required` with no `default` must find a non-blank frozen value;
  otherwise `FreezeFailure("mapping row <path>: attribute <name> is required and not set")`,
  which the pipeline maps to FAILED with no retry (spec 11). Nothing is rendered at freeze.
- `Digest.of(path, algorithm)`: streams a file through MD5, SHA-256 or SHA-1; the fetch adapters
  can share it.
- `processorFor(spec, custom): Processor` turns a `ProcessorSpec` into behaviour: `Quality`
  (rejects an empty file), `Rename` (pattern parsed once; `{name}`, `{sourceName}`, a date pattern
  of `yMdHmsS` letters from the clock in UTC, any other token an attribute), `Zip` (one archive
  named `<first>.zip` through `newStagedFile`, entries named as the objects), `Unzip` (one object
  per entry keeping the entry path as the name so S33's `a/x.csv` and `b/x.csv` differ; past
  `maxEntries` or past `maxBytes` uncompressed the read stops and the chain is rejected naming the
  limit, D41), `Extract` from `fileName`, `sourcePath` or `content` (named groups, positional
  groups named by `into`, or JSON pointers; Reject when the regex does not match or a pointer is
  absent), `VerifyDigest` (expected hex from the named attribute, case-insensitive; Reject on a
  missing attribute or a mismatch), `Custom` through the injected lookup (`IllegalArgumentException`
  for an unknown name). `Extract` from `message` and `Expand` throw `NotImplementedError`.

**Concepts named:** *chain result* (Done with frozen attributes, or Rejected) versus *stage error*
(a throw, retryable) versus *freeze failure* (a mapping that cannot be satisfied, terminal until
re-drive): the three exits ticket 06 maps to REJECTED, `failedAttempt` and FAILED. *Input* is any
path in the incoming payload; everything else in the final payload is *new* and gets its digest
from the pipeline, never from the processor.

**Acceptance:**

- *I15, I18, S20, S26* - `AttributeFreezeTest.I15_attributes_never_change_after_the_chain_ends_and_mappings_are_checked_before_the_store`,
  `ProcessingChainTest.I18_a_processor_never_modifies_an_input_and_every_created_file_is_deleted_with_staging`,
  `BuiltInProcessorsTest.S20_rename_then_zip_yields_one_archive_under_the_renamed_key_with_a_different_digest`,
  `AttributeFreezeTest.S26_missing_required_attribute_at_freeze_fails_before_the_store`; S21's
  positive half is `AttributeFreezeTest.S21_an_attribute_extracted_from_the_file_name_is_available_to_the_mapping`.
- *Every built-in except expand and message extraction; unzip one per entry; zip one archive through
  the context* - `BuiltInProcessorsTest`: `quality_...`, `rename_...`, `S20_...`, `unzip_yields_one_object_per_entry_...`,
  `extract_sets_attributes_from_the_file_name_the_source_path_and_json_content_and_rejects_a_non_match`,
  `verifyDigest_...`, `a_custom_processor_resolves_by_name_and_an_unknown_name_fails_at_construction`.
- *Unzip limits, stopping at the limit* - `unzip_rejects_past_maxEntries_without_extracting_them_all_and_past_maxBytes`
  (five entries, `maxEntries = 2`: rejected after the third is seen, at most three files created).
- *Writing into an input detected; a throw is a retryable stage error* - `I18_...` (second half, the
  kit's `inputsUntouched()` is false) and `ProcessingChainTest.a_processor_throwing_is_a_retryable_stage_error_carrying_the_cause`.
- *Rule 22 enforced; `SOURCE_DIGEST` and `DIGEST` differ after zip* - `AttributeFreezeTest.rule22_attribute_limits_are_enforced_when_the_chain_ends`
  (all three limits) and `S20_...`.
- *Mapping check at freeze; missing required attribute fails before any store* - `S26_...` (the
  in-memory target records no call) and `I15_...`.
- *Progress entry* - this.

Final run: ArchitectureTest 7, AttributeFreezeTest 4, BuiltInProcessorsTest 8, MappingRendererTest 12,
ProcessingChainTest 4, RulesTest 30, SurfaceTest 3, testkit 25, YamlLoaderTest 10; 103 tests, 0 failures.

**Deviations:**

1. **Rule 22 broken at run time is a `Rejected`, not a stage error.** Spec 11 has no row for it;
   retrying cannot help a processor that sets 33 attributes, so it is terminal until re-drive like
   any Reject.
2. **The freeze check does not render.** Spec 6.4 asks for "a missing required attribute" to fail
   before the store; the check scans `attribute` rows for a required, defaultless row whose value is
   unset or blank. Rendering at freeze would fail on `field` rows the store has not filled yet
   (`TARGET_KEY`), so 04's dry-run suggestion was not taken.
3. **`quality` has one built-in check, non-empty**, since spec 13.1 configures none; its
   constructor takes any `(StagedObject) -> String?` for a route that needs more.
4. **`rename` detects a date token by its letters** (`yMdHmsS` only); an attribute named like a date
   pattern would be misread. Spec rule 13 names `{yyyyMMdd}` alone.
5. **Size:** 225 main, 328 test; in budget.

**For the next ticket:**

- **06 (pipeline):** `ChainResult.Rejected` is REJECTED; `StageError` is `failedAttempt` (spec 11
  "processor throws"); `FreezeFailure` is FAILED with no retry ("missing required mapping input").
  After `Done`, call `ProcessingChain.checkMappings(done.attributes, route.notify.map { channels[it.channel].body }) { beans(it) != null }`
  before any store; `done.payload.objects.size` decides one row or N children; `done.attributes`
  is what `processed(id, attributes)` records. Build the chain once per route with
  `route.process.map { processorFor(it, custom) }`; `Digest.of` is there for a fetcher that has
  the file but not the digest.
- **Gotcha:** `unzip` keeps the entry path as the object's `name`; a `key: "{name}"` target then
  holds `a/x.csv` verbatim. That is what S33 needs to detect the same-key collision.
- **16/17:** `ExpandProcessor` and `Extract(from = Message)` are the two remaining shells;
  `processorFor` refuses the latter at construction, so rule 14's "message only on a subscribed
  route" is the only thing standing between a config and a `NotImplementedError` until then.
