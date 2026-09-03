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

---

## 09: Notifier: pending deliveries become channel calls

**Built:** `infra.shuttle.core.Notifier`, replacing the G0 shell: one loop per process that turns
PENDING outbox rows into channel calls. `Notifier(store, channels, bodies, renderer, config,
registry, clock, random)`; `suspend fun run()` runs until cancelled; `fun wake()` is the conflated
signal a row-creating transaction sends; `inFlightCount` reads the in-flight set. Each pass selects
at most `batch` due rows not in flight, adds their ids to the set, hands each to one of `workers`
permits, refreshes the outbox gauges, and then waits for a wake or `sweepEvery`, or re-selects at
once when the batch was full. A delivery renders its body at send time through `MappingRenderer`
with `attempts + 1`, calls the channel, and records the outcome: `Delivered` writes `delivered`
(the store flips DONE when every row is DELIVERED, I11), `Retry` writes `retryLater` at now plus
spec 9.3's backoff, or `deliveryFailed` with `gave_up` once `attempt >= maxAttempts` or the row is
older than `giveUpAfter`; `Reject` writes `deliveryFailed` as `rejected`. A channel exception is a
`Retry`; a `MappingFailure` or an unknown channel is a `Reject`; `CancellationException` passes
through untouched. One INFO line per attempt names transfer, event, channel, attempt, outcome,
status and reference. Meters: `shuttle_delivery_total{channel,event,outcome}`,
`shuttle_delivery_seconds{channel}`, `shuttle_outbox_pending{channel}`,
`shuttle_outbox_oldest_seconds{channel}`, `shuttle_notifier_inflight`.

**Concepts named:**

- **The in-flight set** is the only shared mutable state of the module (plan 2.5): a concurrent set
  of delivery ids, added at select, removed in `finally` on every exit path, exposed as a gauge.
- **`wake`** is a `Channel<Unit>(CONFLATED)`: it carries no rows (D7, plan 2.4), only the fact that
  a select is worth running now; the sweep is the guarantee.
- **The give-up rule** is judged only when the channel answered `Retry`: `attempt >= maxAttempts`
  or `now - createdAt >= giveUpAfter`. A re-driven row starts at attempt 1 again; its age is
  unchanged, so a downstream still down after a re-drive gives up on the next `Retry`.
- **Two seam methods added to `StateStore`** (see deviations): `byId(id)` and `outboxPending()`.

**Acceptance:**

- *I3, I4, I5, I13 as named tests; S7, S8, S9, S17, S22 on fakes* -
  `NotifierTest.I3_a_delivery_is_DELIVERED_only_after_the_channel_returned_Delivered`,
  `I4_a_delivery_id_is_never_inside_two_workers_at_once` (a parked delivery; every later select's
  `excluding` holds its id), `I5_the_in_flight_set_never_exceeds_batch_plus_workers_and_is_empty_when_idle`
  (7 rows, batch 2, workers 1), `I13_two_channels_on_one_event_are_delivered_independently`,
  `S7_downstream_503_twice_then_200`, `S8_downstream_400`, `S9_downstream_down_past_giveUpAfter`
  (with the re-drive delivering), `S17_two_channels_on_acked_one_always_503`,
  `S22_one_provider_selected_by_three_rows_is_invoked_once_at_send_time`.
- *A wake causes a select before the sweep interval elapses* -
  `a_wake_causes_a_select_before_the_sweep_interval_elapses` (delivered 2 s in, sweep at 30 s).
- *Backoff follows spec 9.3; `maxAttempts` and `giveUpAfter` both flip to FAILED with `gave_up`* -
  `backoff_follows_spec_9_3_with_full_jitter_below_the_ceiling_and_the_cap_at_max` (11 attempts,
  ceilings 5, 10, 20 ... 640, 900, 900), `S7` (no jitter: exactly +5 s then +10 s),
  `maxAttempts_flips_a_delivery_to_FAILED_with_gave_up`, `S9`.
- *Bodies rendered at send time; cancellation leaves the row PENDING and the set empty* - `S22`
  (the provider is invoked zero times before `run`, once at send), `I3` (the body carries the
  transfer id and `acked`), `cancellation_mid_delivery_leaves_the_row_PENDING_and_the_set_empty`.
- *Meters of spec 14.2* - the counter per outcome in S7, S8, S9 and the maxAttempts test; the
  in-flight gauge in I5 and the cancellation test; the pending and oldest gauges in S17.
- *Progress entry appended* - this entry.

**Deviations:**

1. **`StateStore` gained `byId(id: TransferId): Transfer?` and `outboxPending(): List<Delivery>`.**
   Spec 8.2 has no way to load the transfer row a delivery points to, yet spec 9.1 says the body is
   rendered from that row; and it has no way to count PENDING rows per channel, yet spec 14.2 asks
   for `shuttle_outbox_pending` and `shuttle_outbox_oldest_seconds`. Both are read-only, one
   statement each. `InMemoryStateStore` implements them; **ticket 10's `JdbiStateStore` must too**
   (a `SELECT` by primary key, and `SELECT ... WHERE notification_state = 'PENDING'`). The gauge
   refresh is a full PENDING scan per pass, marked `ponytail:`; an aggregate query is the upgrade.
2. **The loop is a select loop with a `Semaphore(workers)` and one `launch` per row under
   `coroutineScope`, not literally `flow { }.buffer(batch).flatMapMerge(workers)`.** `flattenMerge`
   keeps an internal buffer of its own, which would let the set exceed `batch + workers` and make I5
   unprovable as stated. The observable properties spec 9.4 wants hold: backpressure by suspension
   (the next select waits until every row of the batch has a permit), nothing lost, rows never on a
   `SharedFlow`, cancellation cancelling the children so `finally` empties the set.
3. **A `MappingFailure` at send time is a `Reject`** (delivery FAILED, transfer untouched): a body
   that cannot be rendered will not render on retry either; spec 11 says a missing mapping input is
   not retried. Rendering normally cannot fail here because the check ran at attribute freeze (05).
4. Size: 148 main, 294 test; in budget. No `Backoff` type added: `DeliveryPolicy.backoff` and
   `fullJitter` from G0 are what the computation reads.

**For the next ticket:**

- **06 (pipeline):** after `acked`, `stored` or `fetched` creates rows, call `notifier.wake()`;
  the sweep covers a missed wake. The notifier never needs the route.
- **10 (Oracle):** implement `byId` and `outboxPending`; keep 03's `attempts` counting on
  `delivered`, `retryLater` and `deliveryFailed`, which this ticket relies on for `attempt = attempts + 1`.
- **12 (HTTP):** `DeliveryEvent.body` is the rendered `JsonNode`; return `Retry` for anything the
  policy should back off on, `Reject` for a 4xx. A thrown exception is treated as `Retry` here.
- **14 (host):** `bodies` is `config.channels` mapped to their `MappingTable`s; run the notifier
  in one scope per process; on shutdown cancel that scope inside `drainTimeout` and every
  in-flight row stays PENDING (I12). Tests drive time with `ClockFixture.advance` plus
  `advanceTimeBy` together, since the sweep is a `delay` and `next_attempt_at` is the wall clock.

---

## 10: Oracle state store over JDBI

**Built:** `infra.shuttle.jdbi`, two classes. `StateStoreSchema.DDL` is spec 8.1 verbatim, with
`statements()` splitting it into what the driver accepts. `JdbiStateStore(jdbi, dispatcher, clock)`
implements every 8.2 method as one `inTransaction` on the injected dispatcher: the transitions
that create outbox rows insert them inside that transaction; `seen` inserts and, on
`uq_file_transfer_identity`, re-selects the winner; `due` is one `FOR UPDATE SKIP LOCKED` select
bounded by `ROWNUM` over an ordered id subquery; children are deleted and re-inserted FETCHED;
a child's `stored` touches the parent row, then runs the conditional flip and creates the parent's
rows only when it fired; `acked` moves parent and children in one statement; `delivered` flips
DONE through a `NOT EXISTS` over the outbox. Three read-side methods (`transfer`, `transfers`,
`outbox`) exist for the tests and the admin surface; they are not part of the seam. The
contract test moved: `testkit/StateStoreContract` holds every seam-level assertion of ticket 03's
`InMemoryStateStoreTest` plus `I20_` and `D42_`; `InMemoryStateStoreTest` and
`JdbiStateStoreTest` are its two subclasses. The pom adds `jdbi3-core` 3.45.4 (compile), `ojdbc11`
and Testcontainers `oracle-free` 1.20.4 (test), and `excludedGroups=oracle` as a property the
way `etl-host` does; `-DexcludedGroups=none` opts the Oracle class in.

**Concepts named:** *the contract* is `StateStoreContract`: one set of assertions, a `store`, three
read views and a `poisonedEvents()` hook that makes a delivery insert fail inside the transaction
(the in-memory store's switch; on Oracle a 65-character channel name against `VARCHAR2(64)`).
*The tail lock* is the parent-row touch in `stored` (D42, amended). *Latest revision* is how
`find`/`seen`/`unlisted` resolve an identity, as ticket 03 fixed it.

**Acceptance:**

- *DDL matches 8.1 verbatim* - `StateStoreSchemaTest.the_DDL_text_matches_spec_8_1_verbatim`
  reads `docs/shuttle/spec.md` and compares the 8.1 block to the constant.
- *The contract passes against both stores, tagged `oracle`, excluded by a pom property* -
  `InMemoryStateStoreTest` 16 and `JdbiStateStoreTest` 17 (the 15 shared tests plus two Oracle-only
  ones), `@Tag("oracle")`, `<excludedGroups>oracle</excludedGroups>`.
- *I11 and I20 on Oracle* - `I11_a_failing_delivery_insert_leaves_the_transfer_state_unchanged`
  and `I20_a_notification_row_exists_iff_its_transition_committed` in the contract, green on
  Oracle: the oversize channel fails the insert and the transition is not there afterwards.
- *Unique-identity violation on `seen` returns the existing row; `due` excludes ids, honours the
  limit, skip-locked* - `seen_creates_a_SEEN_row_and_returns_the_existing_row_for_a_known_identity`
  and `seen_returns_the_existing_row_when_the_unique_identity_constraint_fires` (proves the DDL's
  constraint refuses a duplicate); `due_orders_by_next_attempt_excludes_ids_and_honours_the_limit`;
  `due_skips_rows_another_session_holds_locked` (a second connection holds a row `FOR UPDATE`, `due`
  returns the other one, then both once released).
- *D42 on Oracle* - `D42_children_completing_concurrently_leave_exactly_one_parent_STORED_write`:
  eight children `stored` at once on `Dispatchers.IO`; the parent is STORED once with one set of
  outbox rows (the unique constraint would refuse a second).
- *JDBI and `java.sql` only in the jdbi package* - `ArchitectureTest`, now with classes to check.
- *Progress entry* - this.

Default run: 92 tests green with the Oracle class excluded; opt-in run
`-DexcludedGroups=none -Dtest=JdbiStateStoreTest`: 17 green in 62 s after a container start of
about a minute (`gvenzl/oracle-free:23-slim-faststart`, already pulled).

**Deviations:**

1. **Spec 8.1 gained two sequences** (`file_transfer_seq`, `delivery_outbox_seq`). The v0.4 DDL had
   `id NUMBER(19) NOT NULL` with nothing to generate it; the store reads `NEXTVAL`. Open item 7
   ("sequence names") is now answered in the DDL.
2. **D42 amended: the parent row is touched before the conditional flip.** A zero-row conditional
   update locks nothing, so under read committed two last children each see the other unstored and
   neither flips. The touch is a row lock for the tail of the child's transaction only, no
   `FOR UPDATE`, nothing across I/O; spec 4.5 and D42 now say so.
3. **`unlisted` is one select filtered in Kotlin**, not one statement with the listing inlined: a
   STORED-and-unacked set is small by construction, and an inlined listing of thousands of
   identities would fight Oracle's 1,000-element `IN` limit for no gain. (ponytail)
4. **`due` returns fewer than `limit` when rows are locked**: `ROWNUM` picks before `SKIP LOCKED`
   skips. Bounded is what the spec asks; the next wake or sweep collects the rest.
5. **Instants are truncated to microseconds on the way in**, Oracle `TIMESTAMP`'s precision, so what
   is read equals what was written; `unlisted` normalises the listing's mtimes the same way.
6. **The contract's I11 asserts "some failure" by default**; the in-memory subclass overrides the
   assertion to the injected `IOException`, so ticket 03's assertion is not weakened, and the
   `calls` assertion moved to an in-memory-only test.
7. **JDBI `stored` only flips a parent that is not already STORED** (`p.state <> 'STORED'`); the
   in-memory store has no such guard. Untested by the contract; noted so ticket 06 relies on neither.
8. **Size:** 373 main (299 store, 74 schema), 434 test (269 contract, 94 Oracle, 38 schema, 33
   in-memory subclass). Over 600 in total because the contract absorbed ticket 03's test bodies.

**For the next ticket:**

- **14 (host):** produce `Jdbi.create(dataSource)` from the Quarkus datasource and hand it to
  `JdbiStateStore(jdbi, ioDispatcher, clock)`; `ojdbc11` must be a runtime dependency there (it is
  test-scoped here). The startup check "table missing" can be `SELECT 1 FROM file_transfer WHERE
  ROWNUM = 0`, naming `StateStoreSchema.DDL` on failure. The Oracle class takes about two minutes
  wall clock with the container; do not fold it into the default run.
- **06 (pipeline) and 09 (notifier):** the read views `transfer/transfers/outbox` are on
  `JdbiStateStore` and `InMemoryStateStore`, not on the seam; production code must not need them.
- **Gotcha:** run the Oracle class with `-DexcludedGroups=none`; a plain `-Dtest=JdbiStateStoreTest`
  runs zero tests and reports green.

**Merge note:** ticket 09's two read-only seam methods, `byId` and `outboxPending`, were added to
`JdbiStateStore` as one `SELECT` each under the shared contract (`byId_returns_the_row_in_any_state_and_null_for_an_unknown_id`,
`outboxPending_lists_exactly_the_PENDING_rows`), green on the in-memory store and on Oracle.

---

## 11: S3 target and fetcher over the AWS SDK

**Built:** `infra.shuttle.s3`, the module's first adapter over a real technology. `S3Target(client,
bucket, io, clock, betweenPutAndHead = {})` implements `ObjectStoreTarget` per spec 7.2: `store` is
one `PutObject` carrying the metadata map as user metadata and `Content-MD5` when the map says the
digest is MD5, then a `HeadObject` of the new version that checks the content length and, on an
unencrypted object, the ETag against that MD5; the returned `TargetRef` is `("s3", bucket, key,
versionId, size)`. `verify` is a HEAD of key and version id. `probe` is a HEAD of the bucket, failing
with the bucket's name when it is missing, and a read of the lifecycle configuration, warning when
no enabled rule expires non-current versions. Nothing in the package names a delete operation.
`S3Target.client(endpoint, region, pathStyle, accessKey, secretKey, connect, socket, apiCall)` builds
the D4 client: synchronous, Apache HTTP client, endpoint override, path style, static credentials,
API-call timeout. `S3Fetcher(client, bucket, io).fetcher` is the `Fetcher` for spec 4.1 stage 1:
one GET streamed to the staging path through a `DigestInputStream`, the file deleted if the stream
fails, the `StagedObject` carrying name, size, `LastModified`, digest and content type.
`ObjectStoreTargetContract` in the test kit is the shared seam test; `InMemoryTargetTest` and the
`minio`-tagged `S3TargetTest` are its two subclasses. `Minio` is one container per JVM for every
`minio`-tagged class. The pom gains the AWS SDK BOM 2.29.51, `s3` with the Netty client excluded,
`apache-client`, `commons-logging` at runtime, Testcontainers core 1.20.4 with the MinIO module
1.21.3, and the `excludedGroups` property (`oracle,minio` by default, `-DexcludedGroups=none`
to opt in), wired into surefire the way `etl-host/pom.xml` does it.

**Concepts named:**

- **`TargetMetadata` (core):** the keys the pipeline writes into the metadata map a target
  receives: `digest`, `digest-algorithm`, and attributes under `attr-<name>`. The S3 target reads
  the first two to decide on `Content-MD5` and the ETag check; the SDK adds the `x-amz-meta-`
  prefix, so `attr-orderNumber` lands as spec 6.4's `x-amz-meta-attr-orderNumber`.
- **The adapter's own crash point:** `betweenPutAndHead` is a suspend hook the constructor takes
  and production leaves a no-op; spec 4.4 says a crash inside `store` is the adapter's contract,
  and this is where the I6 replay throws.
- **`warnings`:** the target keeps the messages it warned about, beside logging them, so a test
  can assert "warns" and "is silent" without a log appender.
- **Contract versus fake:** the shared contract asserts only what the seam promises (a fresh ref
  per store, the newest content current at the key, `verify` true for a ref that exists and false
  for one that does not). The in-memory fake additionally answers false for a superseded ref;
  S3 with versioning answers true for it, because the old version still exists. That stricter
  fake-only behaviour stayed in `InMemoryTargetTest` under its own test name.

**Acceptance:**

- *Shared contract passes against in-memory and S3 on MinIO with versioning, tagged `minio`* -
  `ObjectStoreTargetContract.I6_a_fresh_ref_per_store_and_the_newest_content_current_at_the_key`
  runs in `InMemoryTargetTest` (default suite) and `S3TargetTest` (`@Tag("minio")`, bucket
  versioning enabled in `Minio.versionedBucket`).
- *I6 on MinIO: three stores read back the newest by key; a crash between PUT and HEAD is repaired
  by the next store; no delete call is ever made* -
  `S3TargetTest.I6_three_stores_read_back_the_newest_by_key_a_crash_between_PUT_and_HEAD_is_repaired_by_the_next_store_and_nothing_is_deleted`:
  the hook throws `CancellationException` on the third store, the fourth store makes its version
  current, the listing shows four versions and no delete marker, and a Mockito spy on the client
  verifies `deleteObject` and `deleteObjects` were never called.
- *Corrupted body rejected by Content-MD5; ETag check passes single-part and is skipped with a
  WARN under encryption* - `a_corrupted_body_is_rejected_by_Content_MD5_and_leaves_no_version`
  (HTTP 400 from MinIO, no version created); the ETag pass is the silent I6 store (`warnings`
  empty); `the_ETag_check_is_skipped_with_a_WARN_when_the_HEAD_reports_encryption` stubs the HEAD
  through a spy to report AES256 and a non-MD5 ETag, and the store succeeds with one warning.
- *Verify of a version expired by hand is false; probe warns without a non-current expiry and is
  silent with one; the suite passes under a credential without delete permission; the multipart
  threshold is pinned* - `verify_of_a_version_expired_by_hand_is_false` (the test deletes the
  version through the client); `probe_warns_without_a_non_current_expiry_is_silent_with_one_and_fails_on_a_missing_bucket`;
  the delete-less credential is NOT proven, see deviation 1; the threshold is pinned by
  construction: a single `PutObject` and no multipart path, so the ceiling is S3's 5 GiB single-PUT
  limit against a 10 MB largest file (documented in the class KDoc).
- *The fetcher's digest matches the object's; the AWS SDK only in the s3 package* -
  `S3FetcherTest.the_fetcher_streams_the_object_to_staging_and_its_digest_matches_the_objects`
  (SHA-256 recomputed independently in the test), `a_missing_object_surfaces_as_the_SDKs_NoSuchKey_and_leaves_no_file`;
  `ArchitectureTest.each adapter depends on core and its own technology only` now has `s3` classes
  to check and is green.
- *Progress entry appended* - this entry.

Default run (`mvn -o -pl shuttle test`): 88 tests green, MinIO excluded. Opt-in run
(`-DexcludedGroups=none -Dtest=S3TargetTest,S3FetcherTest`): 8 tests green in about 9 s including
the container start.

**Deviations:**

1. **The delete-less credential is not exercised.** The MinIO image ships no `mc`, the admin API
   needs its own signed protocol, and a `minio/mc` container is not in the local image cache, so
   no user with a PUT/GET/HEAD-only policy was created. What stands instead is the structural
   proof: the package contains no delete call, and the spy verifies none is made across the I6
   replay. Debt; repaid by adding a `minio/mc` sidecar (or an admin-API client) to the fixture
   and running the target under the restricted user, which the acceptance run of ticket 15 can do.
2. **`Content-MD5` and the ETag check depend on the metadata map.** The seam hands the target a
   file and a map; the target sends `Content-MD5` and compares the ETag only when the map carries
   `digest-algorithm: md5` (`TargetMetadata`), and otherwise verifies size only. Ticket 06 must
   write those keys; a route on SHA-256 gets the size check, as spec 6.5 implies.
3. **Encryption is noticed per store, not at startup.** Spec 7.2 says the adapter "falls back to
   size plus metadata with a WARN at startup" if the bucket encrypts; `probe` does not read the
   bucket's encryption configuration, and the WARN is emitted on each store whose HEAD reports
   server-side encryption. Debt: one `GetBucketEncryption` in `probe` repays it.
4. **`verify` answers false on HTTP 400 as well as 404.** MinIO answers 400 to a malformed version
   id (the contract's "no-such-version" ref) and 404 to a deleted one; both mean "that version is
   not there".
5. **SDK 2.29.51 has no checksum-calculation switch.** D4's "checksums when-required" is that
   version's default behaviour; the explicit `requestChecksumCalculation(WHEN_REQUIRED)` exists
   from 2.30 and must be set if the SDK is ever raised, or MinIO PUTs start carrying CRC32
   trailers. Recorded in the client builder's KDoc.
6. **`commons-logging` is a runtime dependency.** httpclient 4.x needs it and the quarkus BOM
   leaves it off; without it the first SDK call fails with `NoClassDefFoundError`. Ticket 14 may
   replace it with Quarkus's `commons-logging-jboss-logging` bridge.
7. **`TargetMetadata` added to `core`.** Three constants so the pipeline and the adapters agree on
   key names without the pipeline importing `s3`.
8. **`InMemoryTargetTest` was reshaped**, not weakened: its I6 test split into the contract's
   seam-level half and an in-memory-only half that keeps every original assertion, including the
   superseded-ref-is-false one the S3 target cannot share.
9. **Size:** 183 main, 301 test lines; in budget.

**For the next ticket:**

- **14 (host):** produce the client with `S3Target.client(store.endpoint, store.region,
  store.pathStyle, accessKey, secretKey, timeouts.connect, timeouts.socket, timeouts.apiCall)`
  from `S3Store` and the resolved secrets; one `S3Client` per declaration, shared by the target and
  the fetcher; `probe()` at startup fails on a missing bucket and only warns on the lifecycle rule.
  The `S3Target` takes the module's bounded IO dispatcher; every SDK call runs there.
- **06 (pipeline):** build the metadata map with `TargetMetadata.DIGEST`,
  `TargetMetadata.DIGEST_ALGORITHM` (the enum name, any case) and `TargetMetadata.ATTRIBUTE_PREFIX`
  + name for attributes; spec 7.1's source mtime, source name and transfer id are further plain
  keys. A `store` that throws is a stage error; the object it may have left is a non-current
  version the next store supersedes (I6).
- **10 (Oracle) and 15:** the `excludedGroups` property already lists `oracle`; add the tag and
  nothing in the pom. The MinIO tier costs about 9 s per JVM.
- **16/17 (S3 fetch for subscriptions):** `S3Fetcher(client, bucket, io).fetcher` is ready; it
  reads `path` as the key, so the route's `fetch.path` pointer must resolve to a bare key.

---

## 12: HTTP channel

**Built:** `infra.shuttle.http.HttpChannel(config: core.HttpChannel, http: HttpClient, env: (String) -> String?)`,
the `DeliveryChannel` for a channel declared with `http:`. One request per `deliver`: method and
URL from the config, `Content-Type: application/json`, the auth header (bearer, basic, or a named
header) from secrets resolved against `env` at construction, the request timeout from the config,
and the body as `ObjectMapper.writeValueAsBytes(event.body)`, the `JsonNode` ticket 04 put on
`DeliveryEvent`. The outcome comes from the channel's `response` section: a status in `success`
is `Delivered(reference)` with the reference read by the JSON pointer `response.reference` from
the response body, or `null` with a WARN when the pointer resolves nothing or there is no
pointer; a status in `retry` is `Retry(status, body excerpt)`; any other status is `Reject`.
Connection refused, any other `IOException` and `HttpTimeoutException` are `Retry(null, cause)`.
One INFO line per attempt: transfer id, event, channel, attempt, status, reference. Tests in
`HttpChannelTest` against a loopback `com.sun.net.httpserver.HttpServer` on port 0.

**Concepts named:**

- **The send is `sendAsync` bridged into `suspendCancellableCoroutine`,** not a blocking `send`
  on a dispatcher. Cancelling the coroutine cancels the JDK request through the future at once,
  so a cancelled delivery neither blocks a thread for the remaining timeout nor comes back with
  an outcome. `CancellationException` is not an `IOException` and passes through untouched.
- **The reference is best-effort.** A downstream that answers success without the id it promised
  has still received the notification; the row is DELIVERED with a null reference and the WARN
  names the channel, transfer and pointer (spec 9.7 makes the id the receiver's obligation).
- **Secrets resolve at boot.** `Secret.Env` is looked up when the channel is constructed, so a
  missing variable fails startup with the channel and variable named rather than the first
  delivery hours later.

**Acceptance:**

- *200 with the pointer resolving: Delivered with reference; 200 without: Delivered with null and
  a WARN; 503 and 429: Retry; 400: Reject; refused and a stall past the timeout: Retry* -
  `HttpChannelTest.200 with the reference pointer resolving yields Delivered with the reference`,
  `200 without the pointer resolving yields Delivered with a null reference`,
  `a retry status yields Retry and any other status yields Reject`, `connection refused yields Retry`,
  `a stall past the timeout yields Retry`. The WARN is logged, not asserted.
- *Auth modes bearer, basic and header; CancellationException never converted* -
  `auth modes bearer basic and header set the header the server sees` (asserts the header the
  server recorded), `CancellationException propagates unchanged and produces no outcome` (a job
  cancelled while the handler stalls ends in under a second with the exception and no outcome).
  Escaping: `a body value with quotes and backslashes arrives escaped and parses back`.
- *`java.net.http` only in the http package* - `ArchitectureTest.each adapter depends on core and
  its own technology only`, now with classes in `infra.shuttle.http` to check.
- *Progress entry appended* - this entry.

**Deviations:**

1. **No IO dispatcher parameter.** Plan 2.5 puts blocking `HttpClient.send` on the module's
   bounded IO dispatcher; this channel does not block, so there is nothing to put there. The
   first version did use `withContext(io) { http.send(...) }` and the cancellation test showed
   the cost: a cancelled coroutine waited the full request timeout and then returned `Retry`.
   `sendAsync` plus `suspendCancellableCoroutine` is stdlib-and-JDK only (no
   `kotlinx-coroutines-jdk8`). Ticket 14 produces only the `HttpClient`.
2. **Retry and Reject carry the first 200 characters of the response body as `reason`.** The
   spec leaves `reason` free; an excerpt is what an operator wants in the log.
3. **A failed reference lookup on a non-JSON success body is the same null-plus-WARN**, not an
   error: the notification was received.
4. Size: 107 main, 159 test lines; in budget.

**For the next ticket:**

- **09 (notifier):** construct `HttpChannel(config, httpClient, env)` per `http:` channel and
  call `deliver(DeliveryEvent(transferId, moment, channel, attempts + 1, renderedBody))`; map
  `Delivered` to `delivered(id, reference)`, `Retry` to `retryLater`, `Reject` to
  `deliveryFailed`. Do not catch `CancellationException` around it either.
- **14 (host):** produce one `HttpClient` for the process (`HttpClient.newBuilder().connectTimeout(...)`)
  and pass `System.getenv()::get` as `env`; a missing secret variable throws
  `IllegalStateException` at construction, which is the boot failure spec 12.1 wants. No
  dispatcher is needed for this channel.
- Gotcha: `com.sun.net.httpserver.Headers` normalises header names (`X-api-key`), which is why
  the test reads them that way; the client sends what the config says.

---

## 06: Transfer pipeline, entry points and children

**Built:** `infra.shuttle.core.TransferPipeline`, replacing the G0 shell: spec 4.1 stages 0 to 4
for one source object. `TransferPipeline(route, algorithm, store, target, chain, bodies,
providerExists, wake, hook, clock, registry, staging, usableSpace)`; `suspend fun run(event:
RouteEvent.Seen, fetch: Fetcher)` (now `Unit`). One instance per route, safe to run concurrently:
every per-object fact lives in a private `Run`. Stage 0 is the table of spec 4.3 from `find`:
REJECTED and FAILED nack without redelivery and do nothing; STORED verifies the row's reference once
and skips to the ack, or re-runs on the same row when the copy is gone; ACKED and DONE re-fetch a
polled file (inside D40's window: nothing at all), re-ack it as `reacked` when the source digest is
the row's own, or `supersede` it into the next revision when it is not (`shuttle_supersedes_total`);
a redelivered message is verified and re-acked without a fetch; everything else runs from stage 1.
Before stage 1 the injected usable-space function is read (D41): below `staging.minFree` the object
is nacked with redelivery, no attempt counted, `shuttle_staging_deferred_total` incremented,
`shuttle_staging_free_bytes{store}` refreshed either way. Stage 1 fetches into
`<staging.dir>/<transfer id>/`, a directory the run creates and deletes in `finally` on every exit,
including every file the chain created through the run's own `ProcessContext`. Stage 2 runs the
chain, checks the notified channels' tables at freeze, writes `processed`, then expands the target
key for every object (`expandPattern`, rule 13's vocabulary, now shared with `rename`) and rejects
the transfer when two objects resolve to one key, naming both. Stage 3 stores one object on the
row itself or N objects as N child rows through `children`; each child's `stored` is the one seam
call that flips the parent when the last sibling lands (D42, the store's job). Stage 4 acks in the
source's order (D6): poll moves first and writes ACKED after; subscribe writes ACKED first and acks
the broker after. Every transition that can create outbox rows (`fetched`, `stored`, `acked`)
wakes the notifier when the route attaches a channel to that moment. Every error is caught at the
object boundary: `failedAttempt` on the row being driven (`maxAttempts` from the route, or 1 for a
`FreezeFailure`), `nack(redeliver = true)` below the cap and `false` at it, one WARN, the stack at
DEBUG. Meters: `shuttle_transfers_total{route,outcome}` (done, rejected, failed, reacked),
`shuttle_stage_seconds{route,stage,result}` (fetch, process, store, ack), `shuttle_children_total`,
`shuttle_supersedes_total`, `shuttle_staging_free_bytes`, `shuttle_staging_deferred_total`.

**Concepts named:**

- **The run** (`Run`) is one source object's pass through the stages; its `row` is the transfer an
  error is charged to, null until a row is being driven, so a fetch that fails while re-checking a
  finished identity, or a state store that is down before `seen`, nacks with redelivery and counts
  nothing on any row.
- **Resume** is stages 1 (ledger) to 4 over an already staged object; the full run, the STORED
  re-run and the supersede all reach it, which is what keeps the entry-point table one function.
- **`ledger(moment) { transition }`** is a transition that may create outbox rows: the route's
  `notify` entries for that moment ride the seam call, and the wake follows only when there were any.
- **`expandPattern`** (in `Processors.kt`) is rule 13's `{name}`, `{sourceName}`, date pattern and
  attribute vocabulary as one function; `RenameProcessor` calls it now instead of its own resolvers.
- **`TargetMetadata.SOURCE_MTIME`, `SOURCE_NAME`, `TRANSFER_ID`**: spec 7.1's three plain keys,
  beside ticket 11's digest and attribute keys.

**Acceptance:** all in `TransferPipelineTest` (19 tests).

- *I1, I2, I7, I9, I10, I11, I16, I17 as named tests* -
  `I1_S6_a_STORED_row_whose_copy_is_missing_is_stored_again_on_the_same_row_before_it_is_acked`,
  `I2_the_only_source_writes_are_the_ack_and_nack_actions_of_the_trigger`, I7 inside `S10_...`
  and `S11_...` (the next poll fetches and stores nothing),
  `I9_staging_holds_no_file_after_a_processor_throws_after_a_store_fails_and_after_a_freeze_failure`,
  `I10_the_ack_action_runs_only_once_the_transfer_is_STORED`,
  `I11_a_failing_ACKED_transaction_leaves_the_row_STORED_with_no_outbox_row_and_the_attempt_counted`,
  `I16_a_parent_is_acked_only_when_every_child_is_STORED_and_a_failed_child_fails_the_parent`,
  `I17_S19_a_mirror_route_with_no_notifications_goes_none_to_DONE_and_creates_no_outbox_row`.
- *S1, S10, S11, S12 both halves, S19, S33; I24* - `S1_vendor_drop_happy_path_one_file_one_channel`,
  `S10_processor_Reject_is_REJECTED_nothing_stored_and_the_object_stays_until_redrive` (with the
  re-drive re-running from fetch), `S11_fetch_fails_five_polls_in_a_row_is_FAILED_with_nack_no_redelivery`,
  `S12_same_identity_re_dropped_after_DONE_with_the_same_digest_is_verified_and_acked_again_as_reacked`,
  `I24_a_finished_identity_returning_with_a_different_digest_becomes_a_new_revision_and_the_old_row_is_untouched`
  (revision 2 stored, acked and notified; revision 1 equal to its snapshot), `I17_S19_...`,
  `S33_two_children_of_one_parent_on_one_key_reject_the_transfer_with_both_paths_in_the_reason`.
- *Every row of spec 4.3* - none: `I17_S19_...`; SEEN, FETCHED, PROCESSED:
  `a_row_parked_at_SEEN_FETCHED_or_PROCESSED_runs_fully_from_stage_1`; STORED true:
  `S3_a_STORED_row_whose_verify_is_true_skips_to_the_ack_with_no_second_store`; STORED false:
  `I1_S6_...`; ACKED/DONE: `S12_...`, `I24_...`, `D40_...` and the message half of
  `a_subscribed_message_is_written_ACKED_before_the_broker_ack_and_a_redelivery_is_reacked_without_a_fetch`;
  REJECTED, FAILED: `S10_...`, `S11_...`.
- *Store once per object per successful run, verify once per STORED entry* - `target.calls` in
  `S1_`, `S3_`, `I1_S6_`, `S12_`, `I16_` (two children, two stores).
- *D40* - `D40_a_DONE_identity_listed_again_inside_recheckFinished_is_skipped_without_a_fetch_or_a_write_and_rechecked_outside_it`
  (23 h: only `find` on the store, no fetch; 25 h: fetched and re-acked); `recheckFinished = 0s`
  rechecks every poll in `S12_` and `I24_`.
- *D41* - `D41_below_staging_minFree_the_object_is_deferred_with_redelivery_before_any_fetch_and_no_attempt_counted`
  (both meters read, the run proceeds once the space is back).
- *D42* - the pipeline calls `stored` once per child and never touches the parent; the seam
  contract and both stores are ticket 03 and 10's (`StateStoreContract.D42_...`); `I16_` proves the
  parent is STORED, acked once and DONE after the second child's `stored`.
- *Staging empty after success and every failure path, including files a processor created* -
  `stagingIsEmpty()` at the end of every scenario; `I9_` with a processor that creates a file then
  throws, a failing store, and a freeze failure.
- *Hook points* - `the_hook_points_of_spec_4_4_are_reached_in_order_on_a_polled_route` (fetch,
  process, store, ledger stored, ack, ledger acked) and the subscribed test (ledger acked before ack).
- *Progress entry appended* - this entry.

Final run: ArchitectureTest 7, AttributeFreezeTest 4, BuiltInProcessorsTest 8, MappingRendererTest 12,
NotifierTest 13, ProcessingChainTest 4, RulesTest 30, SurfaceTest 3, TransferPipelineTest 19,
HttpChannelTest 8, StateStoreSchemaTest 2, ClockFixtureTest 1, FakeProcessContextTest 2,
HookDriverTest 3, InMemoryStateStoreTest 18, InMemoryTargetTest 3, RecordingChannelTest 2,
ScriptedSourceTest 2, YamlLoaderTest 10; 151 tests, 0 failures, 0 errors (`oracle` and `minio` excluded).

**Deviations:**

1. **A parent found at STORED (or finished) cannot be verified through the seam, so it re-runs.**
   Spec 4.3 says "verify for every child" and S28 says verified children skip the store; the frozen
   `StateStore` has no way to read a parent's children (`children(id, staged)` creates and replaces
   them), and the parent row carries no reference of its own. `verified(row)` therefore answers
   false for a parent, which is the spec's "any false: full run on the same row": the chain re-runs,
   `children` replaces the rows, every child is stored again (an overwrite, D5). Debt for ticket 16
   (S28, M2): a read of a parent's children on the seam, or `children` returning the existing rows
   when the summaries match; then `verified` verifies each child and the store loop skips the true ones.
2. **A child's failure is charged to the parent, not the child.** Spec 4.5 says "a child that reaches
   `maxAttempts` fails the parent"; ticket 03's store does that through the child's `failedAttempt`.
   But a re-run of a PROCESSED parent replaces its children (deviation 1), which would reset a child's
   `attempts` on every retry and never reach the cap. A failing child store is a stage error of the
   parent's run, so the parent's `attempts` climbs and FAILED at `maxAttempts` (I16 proven that way).
   Revisit with deviation 1.
3. **A `FreezeFailure` is `failedAttempt(id, reason, maxAttempts = 1)`.** The seam has no
   `failed(id, reason)`; one attempt against a cap of one is FAILED in one transaction with the
   reason on the row, and the trigger sees `nack(redeliver = false)`. It also adds one to `attempts`.
4. **Inside D40's window the trigger is told nothing**: no ack, no nack. Spec 4.3 says skipped "with
   no fetch and no state write"; a polled file under `none` stays listed whatever we say, and the
   runner (07) owns whatever the connector needs to release the in-flight entry.
5. **A finished polled row whose copy is gone and whose digest is unchanged re-runs on the same row**
   (spec is silent): `fetched` moves the DONE row back to FETCHED and the run stores, acks and creates
   `acked` deliveries again. I10 over a duplicate notification.
6. **Children upload one after another**, not under `route.parallelism` (marked `ponytail:`); the
   runner's parallelism bounds whole pipelines and no M1 route has children. D42's concurrent flip is
   proven on the stores by ticket 10.
7. **`seen` precedes the D41 check**, so a deferred new object already has a SEEN row with
   `attempts = 0`; spec 4.1 asks for "no attempt counted", which holds.
8. **Size: 258 main, 448 test lines** against 200 to 600. The correctness phase: every row of the
   entry-point table, both halves of four scenarios and eight invariants have a test each, and none
   is padding.

**For the next ticket:**

- **07 (runner):** construct one `TransferPipeline` per route: `chain = ProcessingChain(route.process.map
  { processorFor(it, custom) }, algorithm)`, `algorithm = route.digest ?: config.digest`, `bodies` as
  the notifier's map, `wake = notifier::wake`, `staging` from the SFTP store the route fetches from,
  `usableSpace` left to its default. `run` never throws except `CancellationException`; launch it
  under the route's `SupervisorJob` scope behind `Semaphore(route.parallelism)` and count
  `shuttle_inflight` there. Reconciliation's "same function stage 4 uses" is `store.acked(id,
  requests(ACKED))` plus `wake` when the list is not empty; the pipeline does not expose it, so the
  runner writes those two lines itself (or lift `ledger` out if a third caller appears).
- **08 (crash matrix):** every point of spec 4.4 except `afterDeliverySent` is reached with the
  transfer id (child id at `afterStore` and `afterLedgerStored` of a child); `HookDriver.crash` at any
  of them leaves staging empty (the `finally` runs on the `CancellationException`) and the row as the
  matrix says. The poll order is ack, `afterAck`, ledger, `afterLedgerAcked`; subscribe is ledger,
  `afterLedgerAcked`, ack, `afterAck`. To fail one specific transaction with the one-shot
  `failNextDeliveryInsert`, arm it from a hook (see `I11_`): every transition inserts, even with no
  events.
- **13 (SFTP source):** the `Fetcher` receives `event.source.path` and the staging path
  `<dir>/<transfer id>/<source name>`; it must create the file itself and name the `StagedObject`
  after the source object. The `Seen.ack` is the whole ack action (move, delete, none, callback);
  the pipeline calls it exactly once per successful run and once more per re-ack.
- **16 (expand, S28):** `ProcessContext.fetch` throws `NotImplementedError` in the pipeline's
  context; deviation 1 and 2 are yours.

---

## 07: Route runner, reconciliation and supervision

**Built:** `infra.shuttle.core.RouteRunner` and `RouteSupervisor`, replacing the G0 shells.
`RouteRunner(route, pipeline, fetch, store, wake, clock, registry)`; `suspend fun run(events:
Flow<RouteEvent>)` collects one route's flow in order under a `supervisorScope`. Every `Seen` is one
`pipeline.run(event, fetch)` coroutine behind `Semaphore(route.parallelism)`: the permit is taken
by the collector before `launch`, so the collector suspends on the trigger while the route is full,
and the permit and `shuttle_inflight{route}` are released in `invokeOnCompletion`, exactly once
whether the pipeline ran, threw or was cancelled before it started. `PollFailed` and `PollSkipped`
are counted in `shuttle_poll_total{route,result}` with one WARN and touch no pipeline. `PollCompleted`
counts `completed`, then, when not truncated, `store.unlisted(route, startedAt, listed)` and
`store.acked(id, requests)` for each id (the route's `on: acked` requests, the same transition stage
4 writes), one INFO per row, `shuttle_reconciled_total` by the count, one `wake` when rows were
repaired and the route notifies on acked; when truncated, `shuttle_reconcile_skipped_total` and one
WARN. Every `PollCompleted` then refreshes `shuttle_stuck_transfers{route}` from `store.stuck(route,
now - stuckAfter)` when `stuckAfter` is set. A state store that throws inside that repair is logged
and left to the next poll: nothing but cancellation reaches the collector (spec 11). `RouteDown` is
the trigger's last word: collection stops there (`transformWhile`), the in-flight pipelines finish,
and `run` throws the cause. A flow that completes returns normally after the pipelines finish;
cancelling `run` cancels them.
`RouteSupervisor(runners, events: (Route) -> Flow<RouteEvent>, restartBackoff, readiness, registry)`;
`suspend fun run()` launches one child per runner under a `supervisorScope`, each looping: gauge
`shuttle_route_up{route}` to 1, `runner.run(events(route))`, gauge to 0, one WARN naming the cause
and the delay, `delay`, `shuttle_route_restarts_total{route}`, again. The delay starts at `initial`,
doubles by `Backoff.factor` and is capped at `max`; it falls back to `initial` after a run that
delivered a `Seen` or a `PollCompleted`. `fun ready()`: `AllRoutesDown` is ready while any route's
gauge is 1, `AnyRouteDown` only while every gauge is 1; both false before `run` starts.

**Concepts named:**

- **The collector** is the coroutine collecting the route's flow; the pipelines are its children.
  Backpressure to the trigger is the collector suspending on the semaphore.
- **A successful trigger** (spec 10 "resets") is a run in which the trigger delivered a `Seen` or a
  `PollCompleted`: it listed or produced something. A run of nothing but `PollFailed` then
  `RouteDown` keeps climbing the backoff. The supervisor sees this by decorating the flow with
  `onEach` before handing it to the runner; the runner knows nothing of restarts.
- **The route is down** from the moment `runner.run` returns or throws until the restart.
  Normal completion of the flow counts as down too (spec 11: "trigger terminates").
- **RouteDown ends the run by throwing its cause**, chosen over returning it so that a trigger
  that throws instead of emitting `RouteDown` takes the same path through the supervisor.

**Acceptance:**

- *I19 and I21 as named tests; S14, S16, S23* -
  `RouteRunnerTest.I19_with_parallelism_plus_one_objects_at_most_parallelism_pipelines_run_at_once`
  (the store-wide cap of I19, sessions per pool, is the connector pool's and rule 9's; the runner's
  share is the bound per route),
  `RouteSupervisorTest.I21_a_dead_route_is_restarted_with_backoff_doubling_from_initial_to_max`
  (starts at 0, 30, 90, 210, 450, 930, 1830, 2730 s on the virtual clock; 7 restarts counted),
  `S23_I21_two_routes_one_dead_the_other_keeps_completing_and_readiness_follows_the_rule`,
  `RouteRunnerTest.S14_a_truncated_listing_skips_reconciliation_and_counts_it`,
  `S16_a_poll_with_the_state_store_unavailable_completes_nothing_and_the_next_poll_completes_all`.
- *`parallelism + 1` objects, at most `parallelism` pipelines at once; a poll failure never cancels a
  running pipeline* - `I19_...` (three objects, parallelism 2, two parked at `afterFetch` while the
  gauge reads 2 and the third waits), `a_poll_failure_or_skip_is_counted_and_never_cancels_a_running_pipeline`.
- *Reconciliation marks ACKED exactly the STORED rows older than the poll start and absent from a
  complete listing, through the same function the pipeline uses* -
  `S4_a_complete_poll_acks_exactly_the_STORED_rows_older_than_its_start_and_absent_from_the_listing`
  (one unlisted old row ACKED with its `acked` delivery and one wake; a listed row and a row updated
  after the poll's start equal to their snapshots).
- *Restart delays follow the backoff and reset after a successful trigger; both readiness rules* -
  `I21_...`, `the_backoff_resets_after_a_run_that_delivered_a_PollCompleted` (starts at 0, 30, 60,
  90 s), `S23_I21_...` (true under `all-routes-down`, false under `any-route-down`).
- *Stuck gauge refreshes at every poll completion* -
  `the_stuck_gauge_is_refreshed_at_every_poll_completion` (1 with a SEEN row five minutes old against
  `stuckAfter = 3m`, 0 at the next poll once it is REJECTED).
- Also: `one_Seen_on_a_mirror_route_runs_one_pipeline_to_DONE` (the tracer bullet),
  `RouteDown_ends_the_run_with_its_cause_once_the_in_flight_pipelines_have_finished`,
  `cancelling_the_run_cancels_the_pipelines_and_releases_every_permit_and_the_gauge` (staging empty,
  the row left FETCHED, the same runner completes the next run).
- *Progress entry appended* - this entry.

Final run: ArchitectureTest 7, AttributeFreezeTest 4, BuiltInProcessorsTest 8, MappingRendererTest 12,
NotifierTest 13, ProcessingChainTest 4, RouteRunnerTest 9, RouteSupervisorTest 3, RulesTest 30,
SurfaceTest 3, TransferPipelineTest 19, HttpChannelTest 8, StateStoreSchemaTest 2, ClockFixtureTest 1,
FakeProcessContextTest 2, HookDriverTest 3, InMemoryStateStoreTest 18, InMemoryTargetTest 3,
RecordingChannelTest 2, ScriptedSourceTest 2, YamlLoaderTest 10; 163 tests, 0 failures, 0 errors
(`oracle` and `minio` excluded).

**Deviations:**

1. **The supervisor takes the runners plus a flow source, not a `(Route) -> suspend () -> Unit`
   factory.** It needs to see the events to know a trigger succeeded (spec 10's reset), and wrapping
   the flow with `onEach` is the one place that can without a callback on the runner.
2. **No `Clock` on the supervisor.** The delays are `delay`, which the virtual clock drives; a wall
   clock has nothing to read there. The ticket listed one; YAGNI.
3. **`shuttle_stuck_transfers{route}` is registered for every route** and only refreshed when
   `stuckAfter` is set, so the series exists at 0 rather than being absent for routes without a cap.
4. **A trigger flow that throws, rather than emitting `RouteDown`, cancels the in-flight pipelines**:
   the exception leaves `supervisorScope` and takes its children with it. `RouteDown` is the
   protocol for a graceful stop; a throwing flow is the process-crash path of spec 4.4 for whatever
   was in flight, and the supervisor restarts the route either way.
5. Size: 159 main, 310 test; in budget.

**For the next ticket:**

- **08 (crash matrix):** drive one route through `RouteRunner.run(ScriptedSource(clock).seen(...)
  .pollCompleted(...).events())` in `launch` under `runTest` with a `HookDriver` as the pipeline's
  hook; `advanceUntilIdle()` parks the pipelines at the paused point; `hook.crash(point)` is the
  process dying there (the runner's `invokeOnCompletion` releases the permit and the gauge); then a
  second `run` of the same flow (the `ScriptedSource` replays; identities are compared by value) is
  the next poll from the same `InMemoryStateStore` and `InMemoryTarget`. S4 is `crash(afterAck)`
  then a `PollCompleted` that does not list the file, with `clock.advance` between them so the row's
  `updated_at` is older than the poll's `startedAt`. Never leave a point paused: `resume` disarms
  the gate even after a `cancel` (see the cancellation test).
- **13 (SFTP source):** the runner collects the flow in order and suspends on `Seen` while
  `parallelism` pipelines run, so a trigger that emits from a listing loop is back-pressured by the
  route; emit `PollCompleted(startedAt, listed, truncated)` once per poll with `startedAt` read
  before the listing and `truncated = true` when the listing hit `maxFilesPerPoll`; emit
  `PollSkipped` when the previous poll is still running; emit `RouteDown(cause)` as the last event
  and then complete, never throw out of the flow (deviation 4). Inside D40's window the pipeline
  tells the trigger nothing (ticket 06 deviation 4): whatever releases the connector's in-flight
  entry for that file must be the trigger's own bookkeeping after `pipeline.run` returns, which the
  runner does not expose; the simplest is a `finally` in the `Seen`'s producing coroutine keyed on
  the ack and nack having both not fired, or an in-flight set that a listing refreshes.
- **14 (host):** one `RouteSupervisor` per process: `RouteSupervisor(runners, { route ->
  sources.getValue(route.name).events() }, config.supervision.restartBackoff,
  config.supervision.readiness, registry)`; run it in the process scope beside the notifier;
  `ready()` is the readiness probe's answer; cancel the scope on shutdown inside `drainTimeout`.
  One `RouteRunner` per route with the `TransferPipeline` built as ticket 06's note says. Gotcha:
  two routes on one store both register `shuttle_staging_free_bytes{store}` (ticket 06's gauge);
  Micrometer keeps the first and ignores the second, so only the first route's reading is
  published. Both read the same volume, so the value is right but the refresh cadence is one route's.

