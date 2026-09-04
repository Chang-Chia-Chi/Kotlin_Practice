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

## 16: NATS channel: subscribe trigger and publish

**Built:** `infra.shuttle.nats.NatsChannel(config: core.NatsChannel, connection: io.nats.client.Connection,
io: CoroutineDispatcher = Dispatchers.IO)`, one class carrying both roles a NATS channel has.

`events(route, source: Source.Subscribe): Flow<RouteEvent>` is the `subscribe` trigger of spec
5.1. It pull-subscribes to `source.subject` on a durable JetStream consumer named after the
route, fetches one message at a time with a one second wait, and turns each message into one
`RouteEvent.Seen`. The `Seen`'s `ack` applies the route's `onAck` (`term` for `AckAction.Term`,
`ack` otherwise) and its `nack(redeliver)` is `nak` when it asks for redelivery and `term` when
it does not, which is the whole `subscribe` vocabulary of spec 5.3. From the moment a `Seen` is
handed out until one of those runs, a coroutine tells the broker `inProgress` every
`inProgressEvery` (D38); a signal the broker misses is swallowed, because it costs at worst one
redelivery. Identity is spec 5.2: `SourceKind.NATS`, `sourceRef` = `channel:subject`,
`sourceName` = the publisher's `Nats-Msg-Id` header when it set one and the stream sequence
otherwise, no size and no mtime. The `SourceView` handed over is `SourceView(msg.subject,
msg.data, headers)` - core's existing type, which is already what `extract from: message` reads.
A closed connection, a failed subscribe, or a fetch that throws ends the flow with
`RouteEvent.RouteDown(cause)`; supervision restarts the route with backoff (spec 10).

`deliver(event)` is a JetStream publish of the rendered body on the channel's `subject`, with
the stream sequence the server answers as the delivery's `reference`. A broker that does not
answer is `Retry(null, cause)`. One INFO line per attempt. `CancellationException` is rethrown
before any other catch in both methods; every blocking jnats call runs on `io` through
`runInterruptible`, so cancelling the collector interrupts the call in flight.

`pom.xml`: `io.nats:jnats:2.21.2` (compile, the `nats` package only) and `nats` added to the
`excludedGroups` property, so the tier is opt-in with `-DexcludedGroups=none` like `oracle` and
`minio`. No new Testcontainers module: the broker is a `GenericContainer` on `nats:2.10-alpine`
run with `-js`, waiting for the `Server is ready` log line.

**Concepts named:**

- **The durable consumer is the route.** `PullSubscribeOptions.durable(routeName)`, with the
  route name's illegal characters replaced. The consumer is created with the server's defaults
  on first use and bound to the operator's when it already exists, so the ack wait that
  `inProgressEvery` must stay below remains the operator's - a value spec 5.1 says the process
  cannot read. No configuration knob, no stream name to state.
- **The message id, not the delivery count, is the identity.** The stream sequence and
  `Nats-Msg-Id` are both the same on a redelivery; `deliveredCount` and the reply subject are
  not. That is what makes `find(identity)` re-enter the same transfer after a nak (spec 4.3).
- **A trigger that ends is a route that restarts.** Rather than guessing which broker failures
  jnats will recover from, anything the fetch loop throws, and a connection that reports
  `CLOSED`, is one `RouteDown` and the end of the flow. Spec 10 already owns the retry policy
  for that, with backoff and a counted restart; duplicating it inside the adapter would give
  the operator two backoffs to reason about.
- **The publish subject belongs to the channel, the subscribe subject to the route.** Spec
  13.1's `nats:` block has no subject because its example channel is only ever a source. A
  channel a route notifies through has to name one, so `subject` joins the block (see
  Deviations).

**Acceptance:**

- *A message becomes one `Seen` with working ack and nak; a nak redelivers; term stops
  redelivery* - `NatsChannelTest.a publish lands on the subject, answers the stream sequence, and
  becomes one Seen`, `an acked message is not redelivered`, `a nak redelivers the message under
  the same identity`, `a nack that asks for no redelivery terms the message`, `onAck term stops
  redelivery too`. The test consumer is created with a two second ack wait, so "not redelivered"
  is five seconds of quiet, two and a half ack waits.
- *Identity per spec 5.2 is stable across a redelivery* - `a nak redelivers the message under the
  same identity` asserts the whole `SourceIdentity` is equal on the redelivery.
- *In-progress signals flow every `inProgressEvery`, and a run longer than the ack wait is not
  redelivered* - `in progress signals hold off redelivery for a run longer than the ack wait`:
  `inProgressEvery` 500 ms against a two second ack wait, held for six seconds, no redelivery.
- *A publish lands on the subject and returns the sequence as the reference; a broker outage ends
  with route down* - `a publish lands on the subject, answers the stream sequence, and becomes one
  Seen` asserts `Delivered("1")` then `Delivered("2")` and reads both back off the subject;
  `a closed connection ends the flow with RouteDown`.
- *Tests tagged `nats` on Testcontainers; jnats appears only in the nats package* - the class is
  `@Tag("nats")`; `ArchitectureTest.jnats appears nowhere outside the nats package` and
  `each adapter depends on core and its own technology only`, now with classes in
  `infra.shuttle.nats` to check.
- *Progress entry appended* - this entry.

**Cost of the nats tier:** 31.2 s of test time, 59 s wall clock for
`-DexcludedGroups=none -Dtest=NatsChannelTest` including Maven start, against an image already
in the local Docker cache. Seven tests, one container for the JVM.

**Deviations:**

1. **`subject` is new on `core.NatsChannel`** (`data class NatsChannel(name, url, credentials,
   subject)`), with the YAML key `nats: { ..., subject: files.stored }` and the DSL property
   `nats("events") { subject = "..." }`. `deliver` has to publish somewhere and nothing else in
   the model reaches it: `Notify` names only a channel, and `DeliveryEvent` carries no route. It
   is optional, so a channel used only as a `subscribe` source is unchanged. No new rule number:
   this is rule 2 ("the referenced declaration offers the role used"), whose notify check now
   reads `channel !is NatsChannel || channel.subject != null`. Tests
   `RulesTest.rule2_a_nats_channel_notified_on_states_a_subject`,
   `rule2_a_nats_channel_with_a_subject_may_be_notified_on`,
   `YamlLoaderTest.a_nats_channel_reads_its_url_credentials_and_subject`. **Spec 13.1's `nats:`
   key list and the rule 2 sentence in 13.3 need this recorded**; this ticket may only touch
   `shuttle/` and this file.
2. **No identity pointer knob.** Spec 5.2 allows "a configured pointer into the body when the
   broker's id is not stable across redeliveries". The JetStream stream sequence and
   `Nats-Msg-Id` are both stable, so nothing needed configuring; if a publisher ever proves
   otherwise, the knob lands then.
3. **No `policy` and no `body` on the nats channel config,** so `policy` is `DeliveryPolicy()`
   defaults and the notifier renders an empty body for it (the notifier takes bodies as a map it
   is given, so this is ticket 14's wiring, not a gap here). One knob per ticket.
4. **`SourceView` was already the message view.** Core declares
   `SourceView(path, body: ByteArray?, headers)` with the comment "Subscribe: the message body
   and headers", so nothing new was declared in core for ticket 17 to read.
5. Size: 166 main, 172 test lines, plus four one-line edits to core and the loader; in budget.

**For the next ticket:**

- **14 (host wiring):** produce one `io.nats.client.Connection` per `nats:` channel declaration
  (`Nats.connect(Options.builder().server(config.url)` plus
  `.authHandler(Nats.credentials(...))` when `credentials` is set) and construct
  `NatsChannel(config, connection, io)` once, using the same instance for both roles: the
  notifier's `DeliveryChannel` map and the route runtime's trigger. Close the connection on
  shutdown after the drain. `excludedGroups` is now `oracle,minio,nats`; the container tiers run
  with `-DexcludedGroups=none`. The stream and the durable consumer are the operator's to
  provision (spec 17, open item 9): the adapter creates a durable named after the route if it is
  absent, and the operator's ack wait must stay above `inProgressEvery` (rule 7 only checks the
  latter is positive; the process cannot read the former).
- **17 (extract):** `extract from: message` reads `ProcessContext.source`, which for a subscribed
  route is the `SourceView` this trigger builds: `path` is the concrete subject the message
  arrived on, `body` is the raw message bytes (parse it yourself; the adapter does not), and
  `headers` is every header flattened to its first value. `fetch.path` is a JSON pointer into
  that same body (spec 5.1). The S3 fetcher of ticket 11 expects a bare key, not a leading
  slash, so whatever the pointer yields is passed on as-is.

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

---

## 19: Notifications and callback acks

**Built:** the `callback` ack action of spec 5.3 in `TransferPipeline`, and the proof that
`fetched` and `stored` deliveries ride their own transactions (ticket 06's `ledger` already did
the wiring; this ticket adds the `I20_` tests). `TransferPipeline` gained two trailing
constructor parameters with defaults, `channels: Map<ChannelName, DeliveryChannel> = emptyMap()`
and `renderer: MappingRenderer = MappingRenderer()`; nothing existing was renamed or reordered.
When the route's source `onAck` is `AckAction.Callback(channel)`, stage 4 opens with one
synchronous call to that channel through the `DeliveryChannel` seam: the row is re-read with
`byId`, the channel's mapping table is rendered for moment `acked` with `attempt = attempts + 1`
(the same rendering the notifier does), `deliver(DeliveryEvent(...))` is called, and only
`Delivered` lets the stage go on to the source's own order of D6 (poll: move then ledger;
subscribe: ledger then broker). `Retry`, `Reject` and a thrown exception are stage errors:
`failedAttempt`, `nack(redeliver = true)` below `maxAttempts`, FAILED at it, the row staying
STORED meanwhile and verified (not stored again) on every retry. `CancellationException` passes
through. No outbox row is written for the callback itself. Rule 12 now checks that the named
channel *offers notify* (a nats channel without a `subject` is declared but does not), not only
that it exists, and rule 17 reads the callback channel's mapping table too. `Source.onAck` is a
one-line extension in `ShuttleConfig.kt` so `Rules` and the pipeline read the ack action of
either trigger kind the same way.

**Concepts named:**

- **The callback is the first act of stage 4**, whatever the source's order, so neither a ledger
  write nor a source-side ack precedes upstream's answer; a STORED row is the whole memory of a
  callback that has not succeeded yet.
- **A re-ack never repeats the callback.** ACKED is written only after the callback succeeded,
  so a finished row that comes back (S12, S32, D40) proves upstream already answered; `reack`
  runs the connector's own ack only. This is what keeps a polled `callback` route from telling
  upstream again every `recheckFinished`.
- **The callback's body is checked at attribute freeze** with the notified channels' tables,
  so a missing required input fails before the store (spec 6.4, S26), not at the ack.

**Acceptance:** all in `TransferPipelineTest` (26 tests) unless named otherwise.

- *I20 for all three events* -
  `I20_a_fetched_delivery_row_exists_iff_the_FETCHED_transition_committed` (green on arrival:
  ticket 06's wiring), `I20_a_stored_delivery_row_exists_iff_the_STORED_transition_committed`,
  `I20_an_acked_delivery_row_exists_iff_the_ACKED_transition_committed`; each arms
  `failNextDeliveryInsert` for exactly that transition (before the run for `fetched`, since
  `seen` inserts nothing; at `afterStore`; at `afterLedgerStored`), asserts no row, the previous
  state, the attempt counted and no wake, then re-runs and asserts one PENDING row of that moment
  with the transfer ACKED, not DONE.
- *S30* - `S30_a_callback_ack_answering_500_then_200_keeps_the_transfer_STORED_through_the_failure_and_ACKED_after_with_one_acked_delivery`:
  `RecordingChannel` scripted `Retry("500")` then `Delivered`; STORED, attempts 1, no outbox row,
  no source ack after the first run; ACKED with exactly one `acked` outbox row (to a second,
  notified channel) after the second; the callback saw attempts 1 and 2, both moment `acked`.
  Also `a_callback_answering_Reject_or_throwing_is_a_stage_error_and_FAILED_at_maxAttempts`
  and `a_subscribed_callback_precedes_the_ACKED_ledger_and_the_broker_ack_and_a_redelivery_does_not_call_it_again`
  (hook order `... afterLedgerStored, callback:STORED, afterLedgerAcked, afterAck`).
- *A `fetched` delivery exists after a crash right after fetch and is delivered by the notifier* -
  `a_fetched_delivery_created_before_a_crash_right_after_fetch_is_delivered_by_the_notifier`:
  `HookDriver.crash(afterFetch)`, row FETCHED with one PENDING row and staging empty; a `Notifier`
  over the same store and a `RecordingChannel` delivers it (`event: fetched` in the body) and the
  transfer stays FETCHED.
- *Rule 12 rejects a callback naming a channel without the notify role* -
  `RulesTest.rule12_a_callback_names_a_channel_offering_the_notify_role` (a nats channel with no
  subject), `rule12_a_callback_may_name_a_channel_offering_notify`, and
  `rule17_reads_the_body_of_a_callback_channel`.
- *Progress entry appended* - this entry.

Final run: ArchitectureTest 8, AttributeFreezeTest 4, BuiltInProcessorsTest 8, MappingRendererTest 12,
NotifierTest 13, ProcessingChainTest 4, RulesTest 35, SurfaceTest 3, TransferPipelineTest 26,
HttpChannelTest 8, StateStoreSchemaTest 2, ClockFixtureTest 1, FakeProcessContextTest 2,
HookDriverTest 3, InMemoryStateStoreTest 18, InMemoryTargetTest 3, RecordingChannelTest 2,
ScriptedSourceTest 2, YamlLoaderTest 11; 165 tests, 0 failures, 0 errors (`oracle`, `minio`, `nats` excluded).

**Deviations:**

1. **`Reject` from a callback is the same recoverable stage error as `Retry`.** Spec 11 has one
   row, "Callback ack fails: as recoverable", and no 4xx distinction for acks (the target's 4xx
   row takes the same path); five answers of any kind are FAILED with the last reason on the row.
2. **The callback is not repeated on a re-ack** (see Concepts). Spec 4.3 says "ack again"; the
   connector's ack is repeated, the callback is not, because ACKED proves it succeeded.
3. **The callback's stage timer is a second `ack` sample.** Spec 14.2 fixes the `stage` values
   to fetch, process, store, ack; the callback records under `ack` and the connector's ack records
   another sample, so a callback route has two `ack` samples per transfer. Adding a `callback`
   stage value would change the fixed vocabulary.
4. **A callback route whose channel is not in `channels` fails at construction**
   (`IllegalStateException` naming the route and channel), not at the first transfer: a wiring
   gap is a boot failure (spec 12.1), and rule 12 already guarantees the channel is declared.
5. **Rule 12's spelling of "offers notify" is rule 2's:** any channel but a nats channel without a
   `subject`. The predicate is one shared `offersNotify` in `Rules`.
6. Size: 38 main lines added, 203 test; in budget.

**For the next ticket:**

- **13 (SFTP source):** when the route's `onAck` is `AckAction.Callback`, the pipeline calls the
  channel itself, before `Seen.ack`; the `Seen.ack` lambda must **not** call any channel and must
  do what `none` does for a polled file: nothing to the file (it stays listed; D40 bounds the
  re-checks), release the connector's in-flight entry only. It must never move or delete under a
  callback unless a later spec adds a combined form. `Seen.nack` is unchanged. For a subscribed
  route (16) the same holds with the broker ack in place of `none`: the pipeline's order is
  callback, ledger ACKED, then `Seen.ack` acks the broker.
- **14 (host):** pass `channels = <every DeliveryChannel by name>` (the notifier's collection,
  `associateBy { it.name }`) and `renderer = <the notifier's MappingRenderer>` to every
  `TransferPipeline`, by name; the two trailing parameters default to empty and a provider-less
  renderer, and a callback route with its channel missing throws at construction. Gotcha for the
  merge with 07: `usableSpace` is no longer the last parameter, so a trailing-lambda construction
  `TransferPipeline(...) { freeBytes }` no longer compiles; pass it positionally or by name.
- **20 (M2 acceptance):** S30 on the loopback HTTP server is `HttpChannel` in the `channels` map
  of a route with `onAck: callback: <name>`; the outcome mapping (500 in `retry` is `Retry`, 200
  is `Delivered`) is ticket 12's, the stage retry is this ticket's. The `fetched` delivery after
  a crash is the notifier's ordinary sweep; nothing to wire.

---

## 13: SFTP poll source, the real trigger

**Built:** `infra.shuttle.sftp.SftpPollSource(source, config, route, poll, clock)` and the top-level
`sftpConnectorConfig(store, poll, algorithm, resolve)`, the only two things in the module that name
`sftp.connector`. `fun events(): Flow<RouteEvent>` is the connector's `watch(poll.directory,
poll.every)` read as one route's flow: `PollStarted` is where `startedAt` is read (before the
listing) and where abandoned files are given back; `FileSeen` becomes `RouteEvent.Seen(identity,
SourceView(file.path), ack = file.ack(), nack = { file.nack(reason, it) })`; the connector's
`PollCompleted(tick, seen, emitted, notReady)` becomes `RouteEvent.PollCompleted(startedAt, listed,
truncated)`; `PollFailed` and `PollSkipped` pass through; `FileGone` becomes nothing, because the
fetcher's own failure answers it. `val fetcher: Fetcher` is `FileSeen.download(into)` for the file
`event.source.path` names, looked up in the source's own path-keyed in-flight map; a null download
(gone since it was listed) is an `IOException`, which is the stage error the pipeline charges an
attempt for and nacks. The flow never throws: `catch { emit(RouteDown(it)) }` is the last event of a
watch that ended on a failure no tick could survive. `sftpConnectorConfig` maps `SftpStore`'s host,
port, auth (through a caller's `resolve`), `hostKey`, `pool`, `keepAlive`, `idleTimeout`,
`idleCutoff`, `drainTimeout`, `cancelGrace` and `staging` plus one `Source.Poll`'s `directory`,
`readiness` and `onAck`/`onNack` onto the connector's DSL, with `overlap = SKIP` (spec 5.1) and the
route's digest as the connector's staging digest, so the sum the download already computed is the
one the pipeline wants.

**Concepts named:**

- **The in-flight map** is the source's only state: remote path to the `FileSeen`, its identity, the
  tick that handed it over, and whether the fetcher has been called on it. It serves three purposes
  and that is why it is one map: it is how the fetcher finds the event for the path the pipeline
  gives it, it is what makes `listed` complete, and it is what a later poll releases from.
- **`listed` is what a STORED row may still be**, not what this poll emitted. Spec 4.6 acks every
  STORED row a complete listing did not name, and a file between its store and its move is in flight
  and for that reason *not* handed over again - so `listed` is this tick's identities plus every
  identity still in the map from an earlier tick. Without the second half, reconciliation would write
  ACKED for a file whose move had not happened.
- **Truncation is a reading of counts, not a flag.** The connector reports `seen`, `emitted` and
  `notReady`; the listing is `take(maxFilesPerPoll)` over a walk that, with `recursive = false` (the
  only shape `Source.Poll` can describe), yields exactly the files. So `truncated = seen >=
  config.polling.maxFilesPerPoll`: at the cap nothing proves the directory held no more, and saying
  `true` only skips one poll's repair, which is the safe direction.
- **D40's window is exactly the entries whose fetcher was never called.** Every other path out of
  `TransferPipeline.run` acks, nacks, or has fetched first; only a finished row that came back inside
  `recheckFinished` returns saying nothing (ticket 06 deviation 4). That is what makes the release
  precise rather than a timeout: at a `PollStarted`, every entry from an *earlier* tick that has
  still not reached the fetcher is nacked with redelivery, and the next listing hands it over again.
- **A run that ends gives back everything it held.** The connector withdraws only the files the
  *running* tick handed over, and only when that tick is cancelled; a file held from an earlier tick
  would otherwise stay in its in-flight set for the life of the process, invisible to every later
  poll and to the route's own restart. The flow's `finally` nacks the lot under `NonCancellable`.

**Acceptance:**

- *The vendor-drop route moves a file to `temp/` only after the target holds it; the mirror route
  deletes after store* -
  `SftpPollSourceTest.S1_the_vendor_drop_route_moves_the_file_to_temp_only_after_the_target_holds_it`
  (a `HookDriver` paused at `afterStore` reads the server's directory before the ack and at
  `afterLedgerAcked` after it, so D6's order is asserted on the server rather than inferred),
  `S1_the_mirror_route_deletes_the_file_after_the_target_holds_it`.
- *A file removed between listing and fetch produces no transfer beyond SEEN and no error* -
  `a_file_removed_between_the_listing_and_the_fetch_leaves_a_SEEN_row_and_no_error` (SEEN, one
  attempt, nothing stored, the run's staging directory gone; the pipeline's WARN is its report, and
  nothing reaches the collector).
- *A wrong password ends the flow with route down* -
  `a_wrong_password_ends_the_flow_with_RouteDown_as_its_last_event`
  (`RouteDown(AuthenticationFailed)` last, no `PollFailed`, and `collect` returns rather than throwing).
- *`idleCutoff` and readiness reach the connector's DSL* -
  `SftpConnectorConfigTest.the_store_and_the_poll_reach_the_connectors_config`,
  `the_readiness_checks_reach_the_connector_in_order`,
  `a_store_that_declares_no_readiness_hands_every_listed_file_over`,
  `the_ack_vocabulary_maps_onto_the_connectors_post_actions`,
  `rule12_an_ack_action_of_another_trigger_is_not_something_a_poll_can_do`,
  `sha1_has_no_name_in_the_connector_so_its_downloads_are_summed_with_sha256`.
- *Only the sftp package imports the connector* -
  `ArchitectureTest.the sftp connector appears nowhere outside the sftp package` (with the subject
  asserted, so the sentence cannot pass by having nothing to check), and the existing
  `each adapter depends on core and its own technology only`, whose `sftp` rule is now real.
- Also: `a_file_on_the_server_becomes_one_Seen_whose_identity_is_the_store_directory_name_size_and_mtime`
  (the tracer bullet; spec 5.2, mtime at the second the server reports),
  `a_poll_lists_every_identity_still_in_flight_from_an_earlier_poll` (spec 4.6's contract: one file
  fetched and never answered, and the *next* poll, which emitted nothing, still names it),
  `a_listing_that_reaches_maxFilesPerPoll_completes_truncated`,
  `a_Seen_the_route_neither_answered_nor_fetched_is_given_back_and_handed_over_again` (D40),
  `a_run_that_ends_gives_back_every_file_it_was_holding_so_the_next_run_lists_them`.
- *Progress entry appended* - this entry.

Final run: ArchitectureTest 9, AttributeFreezeTest 4, BuiltInProcessorsTest 8, MappingRendererTest 12,
NotifierTest 13, ProcessingChainTest 4, RouteRunnerTest 9, RouteSupervisorTest 3, RulesTest 32,
SurfaceTest 3, TransferPipelineTest 19, HttpChannelTest 8, StateStoreSchemaTest 2,
SftpConnectorConfigTest 6, SftpPollSourceTest 9, ClockFixtureTest 1, FakeProcessContextTest 2,
HookDriverTest 3, InMemoryStateStoreTest 18, InMemoryTargetTest 3, RecordingChannelTest 2,
ScriptedSourceTest 2, YamlLoaderTest 11; 183 tests, 0 failures, 0 errors (`oracle`, `minio` and
`nats` excluded). `SftpPollSourceTest` costs 3.4 s against the embedded SSHD, so it carries no group
tag and stays in the default tier, like the connector's own server-backed classes.

**Deviations:**

1. **The installed connector artifacts had to be rebuilt before anything could compile.**
   `dynacache:sftpconnector-core:0.1.0-SNAPSHOT` in `~/.m2` held no `sftp/connector/source` package
   at all (no `SftpSource`, `SftpEvent`, `InFlightSet`, `ReadinessCheck`, `OverlapPolicy`), and the
   `dynacache-parent` and `sftpconnector` aggregator poms there were older than the reactor's, so
   Maven called both connector poms invalid - `'dependencies.dependency.version' ... is missing` for
   `resilience4j-retry`, `-circuitbreaker`, `-timelimiter`, `lincheck`, `junit-platform-suite` and
   `testcontainers-toxiproxy` - and dropped *every* transitive dependency, which is why the embedded
   server was not on the classpath either. Fixed by reinstalling, offline and without touching a line
   of `sftpconnector/`: `mvn -o -N install`, `mvn -o -N install -f sftpconnector/pom.xml`, then
   `mvn -o -DskipTests install -pl sftpconnector/core,sftpconnector/testkit`. Anyone building this
   module from a fresh local repository has to do the same, or build the whole reactor.
2. **`org.apache.sshd:sshd-common` is pinned to 2.19.0** in `shuttle/pom.xml`'s
   `dependencyManagement`, before the Quarkus BOM import. The test kit brings `sshd-core` 2.19.0; the
   platform BOM would otherwise hold `sshd-common` at 2.12.1, which is the two halves of one server
   on two versions.
3. **`truncated` is derived, because the connector reports counts and not a flag:**
   `seen >= config.polling.maxFilesPerPoll`. It is exact for the only listing shape `Source.Poll` can
   describe (`recursive = false`, so the walk yields files and nothing else, and both the
   per-directory `maxEntries` and the `take` are that same number). Were `recursive` ever added, a
   directory entry would consume a cap slot without counting in `seen` and the reading would have to
   become "unless the walk finished", which is a connector change; until then this only ever errs
   toward `true`, which skips one poll's repair.
4. **The D40 release is `!fetchStarted` plus one tick, not a timeout.** See "Concepts named". Its
   ceiling, recorded as a `ponytail:` comment: a pipeline that sits between the state store and the
   fetcher for longer than one poll interval - a state store that has stopped answering - has its
   file released early, and its later ack is the connector's "already settled", so the move does not
   happen and the next poll drives the row again from ACKED or STORED. Nothing is lost. The clean
   upgrade is a completion callback on `RouteRunner`, which is a core change and was not made.
5. **The flow's `finally` nacks every file still held, under `NonCancellable`.** A pipeline caught
   mid-ack by a route ending has its ack ignored and its file driven again next poll; that is a
   wasted cycle, against a leak that never heals, and the leak is the worse of the two.
6. **`RouteDown` is emitted for a watch that failed, not for one that completed normally.** The
   connector ending a watch because it was stopped completes the flow, and `RouteRunner` already
   reads a completed flow as the route being down (progress 07); fabricating a cause for it would put
   an invented exception in the supervisor's log.
7. **No `maxFilesPerPoll` on `SftpStore`.** Spec 13.1 lists it "if present" and it is not; the
   connector's own default of 1000 stands and truncation is read off
   `config.polling.maxFilesPerPoll`, so a knob added later flows through with no change here. No YAML
   grammar, DSL or rule was added for it, per "prefer using what exists".
8. **One connector per polled route, not one per store.** `PollingConfig.onAck` is per connector
   while `Source.Poll.onAck` is per route, so two routes polling one store with different ack actions
   cannot share one `SftpConnectorConfig`. `sftpConnectorConfig` therefore takes one store and one
   poll. Ticket 14 has to decide; see below.
9. **The wrong-password test builds `SftpSource` from a raw pool and client** rather than through
   `SftpConnector.start`, whose start-up probe refuses the password before a watch exists. What is
   under test is a watch that ends on a rejection, which is what the connector's own
   `WatchAgainstServerTest` does for the same reason.
10. Size: 292 main, 488 test, 780 total against a 200-600 target. About two fifths is KDoc and
    per-assertion reasoning; the code is one class, one function and five private helpers, and there
    is one test per checkbox plus the four that hold the `listed`, truncation and release contracts.

**For the next ticket:**

- **14 (host):** build one connector per *polled route* with
  `sftpConnectorConfig(store, poll, route.digest ?: config.digest) { secret -> ... }`, resolving
  `Secret.Env` from the environment and `Secret.Literal` to itself, then
  `SftpConnector.start(config, meterRegistry = registry, clock = clock)`; `start` runs the probe, so
  a bad password, a missing watched directory or an action target that cannot be written fails the
  deployment rather than the first file an hour later. Deviation 8 is yours to settle: sharing one
  connector per store means one `onAck` for every route on it (rule 12 could refuse the mix at boot),
  while one per route means one pool and one `maxConcurrentTransfers` each, and rule 9's per-store
  session budget has to be divided between them.
  `SftpPollSource(connector.source, config, RouteName(route.name), poll, clock)` gives the runner
  `source.events()` and `source.fetcher`. A route with `fetch.store` on SFTP (the subscribe shape)
  needs a `Fetcher` that is *not* this one: `SftpPollSource.fetcher` only knows files its own poll
  handed over, and a fetch by path with no `FileSeen` behind it is
  `client.download(client.stat(path)!!, into)` - a few lines in this package, not a change here.
  Shut down by cancelling the supervisor's scope and then `connector.close()` inside `drainTimeout`;
  closing first would cut the sessions the in-flight acks still need.
- **15 (M1 acceptance):** `SftpPollSourceTest` is the SFTP half and runs in the default tier at about
  3.4 s for nine tests, embedded SSHD and all; there is nothing to opt into and no container. S1 is
  its two named tests. If the acceptance suite wants the whole M1 chain against a server it can reuse
  that class's wiring: `EmbeddedSftpServer.start`, `sftpConnectorConfig`, `SftpConnector.start`,
  `RouteRunner` over `SftpPollSource.events()`, `HookDriver` for the pauses, and
  `FileReadiness.SizeStable(checks = 1, interval = 1.milliseconds)` so a file is ready the moment it
  is listed instead of a minute later.
- **18 (SFTP target):** the poll uses `SftpClient.list`, `stat` (through the readiness checks),
  `download`, `rename` (the `move` ack) and `delete` (the `delete` ack). Left untouched and waiting
  for you: `upload(local, remote, overwrite)`, which is `ObjectStoreTarget.store`;
  `mkdir(remote, parents)` for a target directory that does not exist; `exists` for `verify`; and
  `withSession` for anything that needs two calls on one session. `Overwrite.REFUSE` is the
  connector's default and the one an upload should keep, with the write-then-rename shape
  `SftpClient.upload`'s KDoc describes, since a target directory someone else is watching must never
  see a partial file.


---

## 08: Crash matrix replay

**Built:** `CrashMatrixTest` in `shuttle/src/test/kotlin/infra/shuttle/core/`: spec 4.4 replayed row by row on the
fakes through the public collector. Each row is one test: `RouteRunner.run` over a `ScriptedSource` flow with a
`HookDriver` paused at the row's point, `hook.crash(point)` as the process dying there, then the row's "next
trigger" (a replay of the same cold flow, a `PollCompleted` that no longer lists the file with the clock advanced,
or the redelivery of the same message) from the same `InMemoryStateStore` and `InMemoryTarget`, then a `Notifier`
over the same store, and one `assertConverged`: DONE, the store count across both runs, exactly one `acked` outbox
row per channel in DELIVERED, the number of channel calls, the bytes at the key, and `verify` true for the row's
reference. One production change: `Notifier` gained a trailing defaulted `hook: Hook = Hook.None` and calls
`hook.at(HookPoint.afterDeliverySent, transferId)` after the channel answers and before the outcome is recorded,
so the last row of the matrix is reachable through the same driver as the other seven. Nothing else in the
pipeline, the runner or the seams needed changing: every row converged on first run.

**Concepts named:**

- **The replay** is a crash and its next trigger in one test; *converged* is `assertConverged`, the matrix's
  invariant sentence as one function every row ends with.
- **The next trigger** is decided by what the crash left on the source: a file still there is the same flow
  replayed (`ScriptedSource.events()` is cold); a moved file is a `PollCompleted` without it; an unacked message
  is the same `Seen` again.
- **A process death in the notifier** is cancelling the notifier's job while a delivery is parked at
  `afterDeliverySent`, then a fresh `Notifier` over the same store. `HookDriver.crash` alone would cancel only the
  delivery coroutine and the same loop would re-select the row, which is a retry, not a restart.

**Acceptance:** all in `CrashMatrixTest` (9 tests).

- *I8 as one named test per spec 4.4 row, each asserting end state, store count and delivery count* -
  after fetch: `I8_after_fetch_the_next_poll_runs_fully_with_no_extra_store_and_no_extra_delivery` (FETCHED,
  0 stores, then 1 store, 1 delivery, fetched twice); after process:
  `I8_after_process_the_next_poll_runs_fully_with_no_extra_store_and_no_extra_delivery` (PROCESSED, same counts);
  after store before ledger: `I8_S2_after_store_before_ledger_the_next_poll_stores_again_one_extra_store_and_no_extra_delivery`
  (PROCESSED with one copy, then 2 stores, the row on `v2`, 1 delivery); after ledger STORED:
  `I8_S3_after_ledger_STORED_the_next_poll_verifies_and_acks_with_no_second_store_and_no_extra_delivery`
  (STORED, not moved; then `store, verify` only, one ack, 1 delivery); poll move before ledger:
  `I8_S4_poll_move_before_ledger_is_repaired_by_reconciliation_on_the_next_poll_with_a_delayed_delivery`
  (STORED and moved with no outbox row; the next poll's reconciliation writes ACKED and the delivery,
  `shuttle_reconciled_total` 1, the pipeline fetched, stored, verified and acked nothing); subscribe ledger ACKED
  before broker ack: `I8_subscribe_ledger_ACKED_before_broker_ack_is_repaired_by_the_redelivery_reacked_with_no_new_deliveries`
  (ACKED with one PENDING row and no broker ack; the redelivery verifies, acks, `reacked` 1, no fetch, the outbox
  unchanged: I23, S32 on fakes); after ledger ACKED:
  `I8_after_ledger_ACKED_the_notifier_delivers_and_the_next_poll_does_nothing` (ACKED, PENDING, moved; an empty
  next poll reconciles nothing, the notifier delivers, 1 store); delivery sent before ledger:
  `I8_S5_delivery_sent_before_ledger_is_delivered_again_two_calls_one_transfer_id_and_the_row_DELIVERED_once`
  (the row PENDING with one channel call and an empty in-flight set after the death; the restarted notifier calls
  again, two events with one transfer id both at attempt 1, the row DELIVERED with `attempts` 1, DONE).
- *S2, S3, S4, S5, S6 by id* - `I8_S2_...`, `I8_S3_...` (the pipeline-level S3 is
  `TransferPipelineTest.S3_a_STORED_row_whose_verify_is_true_skips_to_the_ack_with_no_second_store`),
  `I8_S4_...` (the reconciliation-only S4 is `RouteRunnerTest.S4_...`), `I8_S5_...`,
  `S6_copy_missing_at_STORED_runs_fully_on_the_same_row_and_reaches_DONE` (the crash left STORED, the copy was
  overwritten from outside; `store, store, verify, store`, the same row id, fetched again, DONE; the
  pipeline-level S6 is `TransferPipelineTest.I1_S6_...`).
- *A crash after the move and before ACKED is repaired by reconciliation on the second poll, not by the
  pipeline; the subscribe row runs against the test kit's message source and is repaired by the redelivery's
  re-ack* - `I8_S4_...` and `I8_subscribe_...`.
- *Every deviation the replay forced is in the progress entry* - below.
- *Progress entry appended* - this entry.

Final run: ArchitectureTest 9, AttributeFreezeTest 4, BuiltInProcessorsTest 8, CrashMatrixTest 9,
MappingRendererTest 12, NotifierTest 13, ProcessingChainTest 4, RouteRunnerTest 9, RouteSupervisorTest 3,
RulesTest 35, SurfaceTest 3, TransferPipelineTest 26, HttpChannelTest 8, StateStoreSchemaTest 2,
SftpConnectorConfigTest 6, SftpPollSourceTest 9, ClockFixtureTest 1, FakeProcessContextTest 2, HookDriverTest 3,
InMemoryStateStoreTest 18, InMemoryTargetTest 3, RecordingChannelTest 2, ScriptedSourceTest 2, YamlLoaderTest 11;
202 tests, 0 failures, 0 errors (`oracle`, `minio`, `nats` excluded).

**Deviations:**

1. **`Notifier` gained `hook: Hook = Hook.None` as its last constructor parameter** and one
   `hook.at(afterDeliverySent, transferId)` between the channel's answer and the outcome record. Spec 4.4 names
   the point; ticket 09 built the notifier without it. The production runner passes nothing and the point is a
   no-op (`Hook.None`). Nothing was renamed or reordered.
2. **No production fix in the pipeline, the runner or the seams was forced.** Every row converged as tickets 06
   and 07 left them.
3. **S6's "copy missing" is a copy overwritten from outside.** `InMemoryTarget` never deletes (I6's `store` never
   deletes, and the fake has no delete knob); an outside `store` on the key makes the row's reference
   non-current, which is the same `verify = false` the pipeline reacts to.
4. **The `I8_S5_` death is `job.cancel()` on the notifier, not `HookDriver.crash`** (see Concepts): the crash
   primitive kills one coroutine, and the notifier's loop would legitimately retry the row inside the same
   process, which is a different claim from the matrix's.
5. Size: 254 test lines, 2 production lines; in budget.

**For the next ticket:**

- **14 (host):** `Notifier`'s new parameter has a default; construct it as before. Nothing in the matrix needs a
  host-side hook: `Hook.None` for both the pipeline and the notifier in production.
- **15 (M1 acceptance):** the matrix on fakes proves the state machine: which state each crash leaves, what the
  next trigger does, and that at most one extra store and one extra channel call follow. What the real adapters
  must re-prove, per row: "after store before ledger" and S2's S3 tier, that a crash *inside* `S3Target.store`
  between PUT and HEAD leaves one current copy (I6, ticket 11's contract, not replayed here since the fake store
  is atomic); "poll move before ledger", that the SFTP move is visible to the next listing so `unlisted`
  finds the row (ticket 13's source with the embedded SSHD, and `JdbiStateStore.unlisted` on `updated_at`);
  "delivery sent before ledger", that `HttpChannel` returning `Delivered` before the process dies makes the
  loopback server see two requests with one transfer id; the subscribe row is M2 (NATS redelivery after a
  process death, tickets 16 and 20). Recipe: the same nine tests with the adapters swapped in under the `oracle`,
  `minio` and `nats` tags, since every test drives only `RouteRunner.run`, the `HookDriver` and the seams.
- **17 (expand):** no row has child coverage yet. `afterStore` and `afterLedgerStored` are reached with a child
  id (ticket 06), so a crash between two children leaves the parent PROCESSED with some children STORED; ticket
  06's deviation 1 (a parent re-runs the chain and replaces its children) means the "half the children stored"
  row (S28) currently stores every child again, one extra store per child rather than per transfer. Add
  `I8_` rows for a crash after the first child's store and after its ledger once the seam can read a parent's
  children.

---

## 18: SFTP target, upload to `.part` and rename over the key

**Built:** `infra.shuttle.sftp.SftpTarget(client, directory, io)`, the second `ObjectStoreTarget` over
a real technology and spec 7.3 in one class. `store` makes the folders the *key* names (once per
folder), uploads the staged file to `<directory>/<key>.part` with `Overwrite.REPLACE`, renames it
over `<directory>/<key>` with `Overwrite.REPLACE` and the staged size as `expectedSize`, then stats
the key and checks the size that landed; the ref is `("sftp", directory, key, mtime, size)`. `verify`
is that stat again: true while the file at the key is the size and mtime the ref named. `probe` is a
stat of the target directory, refusing with the path when there is nothing there and when what is
there is a file. `SftpTargetTest` is `ObjectStoreTargetContract`'s third subclass, against the
connector's `EmbeddedSftpServer`, in the default tier at 1.1 s for five tests. No production code
outside the `sftp` package changed, and no core file was touched.

**Concepts named:**

- **The partial name is this adapter's own.** `<key>.part` is written by nothing else, which is what
  makes it safe to take back: whatever is at that name is a store of this key that died before its
  rename. That single fact is what turns I6 from a repair procedure into `Overwrite.REPLACE`.
- **The ref is the key's mtime.** SFTP has no version id, and `location` plus `key` already name the
  path, so a ref whose `ref` field carried the path would be inert - `verify(ref.copy(ref = ...))`
  could not tell a stale ref from a live one, which the shared contract requires. The mtime the
  server reported for the file the rename put there is the one per-write identity the protocol
  offers, so that is what the ref carries and what `verify` compares alongside the size.
- **`io` is about whose clock, not how many threads.** The connector already runs its blocking work
  on `Dispatchers.IO.limitedParallelism(pool.maxSize)`; what the injected dispatcher decides is
  whether the connector's own timeouts and retry backoffs see real time. A caller on a scheduler
  that skips time - `runTest`, which is how the shared contract is written - makes every request
  time out before the socket can answer. That was found by running the contract, not by reasoning.
- **The target directory is the partner's; the key's folders are ours.** `mkdir(parents = true)` is
  called only for the folders a key names *below* the target directory, so a missing target
  directory stays a start-up failure (spec 12.1) instead of being quietly created by the first file.

**Acceptance:**

- *The shared target contract passes against the SFTP target on the embedded SSHD* -
  `SftpTargetTest` extends `ObjectStoreTargetContract`, so
  `I6_a_fresh_ref_per_store_and_the_newest_content_current_at_the_key` runs against a real server.
- *`I6` on SFTP: a crash between upload and rename is repaired by the next store* -
  `SftpTargetTest.I6_a_store_that_died_between_the_upload_and_the_rename_is_repaired_by_the_next_store`
  (a `.part` holding another version is put on the server through the connector's own client, which
  is exactly what a process killed between the two calls leaves; the next store lands its content at
  the key and the folder afterwards holds one file and no partial).
- *Verify of a removed file is false; probe fails on a missing directory* -
  `verify_is_false_for_a_copy_that_has_been_taken_away_or_written_over` (false for a different file
  under the same name as well as for no file at all),
  `probe_passes_on_the_target_directory_and_fails_naming_a_path_that_is_not_a_directory`.
- Also: `a_key_with_no_folder_in_it_lands_in_the_target_directory` (the ordinary `key` pattern, and
  the branch that keeps `mkdir` off the partner's own folder).
- *Progress entry appended* - this entry.

Final run: ArchitectureTest 9, AttributeFreezeTest 4, BuiltInProcessorsTest 8, MappingRendererTest 12,
NotifierTest 13, ProcessingChainTest 4, RouteRunnerTest 9, RouteSupervisorTest 3, RulesTest 35,
SurfaceTest 3, TransferPipelineTest 26, HttpChannelTest 8, StateStoreSchemaTest 2,
SftpConnectorConfigTest 6, SftpPollSourceTest 9, SftpTargetTest 5, ClockFixtureTest 1,
FakeProcessContextTest 2, HookDriverTest 3, InMemoryStateStoreTest 18, InMemoryTargetTest 3,
RecordingChannelTest 2, ScriptedSourceTest 2, YamlLoaderTest 11; 198 tests, 0 failures, 0 errors
(`oracle`, `minio` and `nats` excluded). `SftpTargetTest` costs 1.1 s against the embedded SSHD -
one server and one connector per test method - so it carries no group tag.

**Deviations:**

1. **Both operations replace, where progress 13 expected the upload to keep `Overwrite.REFUSE`.**
   Refusing on the partial name makes I6 unrepairable: a store killed between its upload and its
   rename leaves `<key>.part` on the server, and every later store of that key would refuse until a
   person logged in and deleted it. The name is this adapter's own and nothing else may write it, so
   there is nothing to protect. The rename replaces because spec 7.1 makes the key a pure function
   of the object's name, so a retry aims at the name its own earlier attempt took. The partial file
   that progress 13 was protecting against - a watcher of the target directory seeing half a file
   under the name it waits for - is still prevented, by the `.part` name itself.
2. **`verify` compares the mtime as well as the size**, where spec 7.3 says only "a stat comparing
   size". The addition is what makes `TargetRef.ref` mean anything on this protocol; see "Concepts
   named". It errs toward false: a partner that rewrites the file under the same name gets a false,
   which is the safe direction for a check that exists to ask "is what I stored still there".
3. **`probe` is a stat of the directory, not the connector's `StartupProbe`.** Spec 7.3 says the
   connector's startup probe, and the ticket allowed this: `StartupProbe` is `internal` to the
   connector, is driven by `polling.directories` and `actionTargets`, and is run by
   `SftpConnector.start` before a client exists - there is no way to point it at a target directory.
   A target-only connector has no polling directories, so its start-up probe checks nothing, and
   this stat with its own message is what a start-up gets instead. Repaying it means a public
   "check this directory" on the connector, which is a connector change and out of scope here.
4. **The metadata map is dropped.** SFTP has nowhere to put it and nothing reads it back, so it is a
   DEBUG line naming the count and no sidecar file the partner never asked for. Spec 7.1 leaves this
   to the adapter.
5. **Two concurrent stores of the same key would share the partial name.** The pipeline runs one
   transfer per file and a retry is sequential, so nothing in this deployment does that; if two
   routes ever wrote one key on one server they would already be fighting over the key itself.
6. Size: 109 main, 144 test, 253 total against a 200-600 target. One class, three seam methods and
   two private helpers; the file is over half KDoc, because the two `Overwrite.REPLACE` decisions
   are the whole ticket and had to be written down where they are made.

**For the next ticket:**

- **14 (host):** construct it as `SftpTarget(connector.client, target.directory, io)` where `io` is
  the module's bounded IO dispatcher, and call `probe()` at start-up (spec 12.1) - `SftpConnector`'s
  own probe checks nothing for a target-only connector, so nothing else will tell a deployment that
  the partner's folder is missing. **One connector per store is right for targets** and it is the
  opposite of the poll's constraint (deviation 8 of progress 13): a target connector needs no
  `polling` block at all - no directories, no `onAck` - so its config is just endpoint, auth, host
  key and pool, and every route targeting one store can share one. Build that config with the
  connector's own `sftpConnector(name) { ... }` DSL rather than `sftpConnectorConfig`, which needs a
  `Source.Poll`; `SftpTargetTest.start` is the four-line shape. Note that `SftpStore` is the store
  declaration for both directions, so a store that is polled by one route and targeted by another
  gets two connectors and two pools, which rule 9's per-store session budget has to account for.
  The target directory itself comes from the route's `Target` declaration, not from the store.
- **20 (M2 acceptance, the partner-server half):** `SftpTargetTest`'s wiring is the whole setup -
  `EmbeddedSftpServer.start(root, user, password)`, `sftpConnector { ... }`, `SftpConnector.start`,
  `SftpTarget(connector.client, "/landing", Dispatchers.IO)` - and a delivered file can be read
  straight off the server's root directory on local disk. Two things to assert that only an
  end-to-end run can: that the partner never sees the final name holding a partial file (list the
  directory while a store is in flight, or pause at `HookPoint.afterStore`), and that a route whose
  source is one SFTP store and whose target is another moves bytes between two servers without the
  two connectors sharing anything. `runTest` cannot drive any of it - see "Concepts named" on `io`;
  use `runBlocking` with `withTimeout`.

---

## 17: Expand, fetch and parent completion on fakes

**Built:** spec 13.1's image-sets route end to end against the test kit: a message names a metadata file, the
route fetches it through `fetch.store` at the path read from the message, `expand` fans it out, the children are
stored in parallel under the route's parallelism, the parent is STORED with the last child (D42), the message is
acked once and downstream told once. Four production changes, all in `core` and `jdbi`:

- `ExpandProcessor` replaces the last G0 shell (`Shells.kt` deleted). `format: json` reads the current payload
  object, `format: message` the subscription message body; `files` is a JSON pointer with one optional `[*]`
  (`/images[*].path`, `/paths[*]`, or `/paths` for an array of strings, `.path` after the star reading as
  `/path`); every listed path is fetched through `ctx.fetch(from, path)` into a file the run owns, and the
  payload becomes one object per child, named by the fetcher after the path's last segment. Nothing listed, or
  a pointer landing on something other than a string, is a Reject. `ExtractProcessor` accepts `from: message`
  (the body as the regex or JSON subject; a message without a body is a Reject) and `processorFor` no longer
  refuses it. Rule 14 gained `expand.format` in `{json, message}`, `message` only on a subscribed route, and
  `files` a pointer on both sides of the star (`expandPointer`, shared with the processor).
- `TransferPipeline`: stage 1 of a subscribed transfer fetches the path `fetch.path` points at in the message
  body (`sourcePath()`; a body without it is a stage error, retried to FAILED). `Context.fetch(store, path)` runs
  the run's own fetcher when `store` is the route's fetch or poll store and otherwise looks `store` up in a new
  trailing constructor parameter `fetchers: Map<String, Fetcher> = emptyMap()`. Children are stored in parallel:
  one `async` per child under `supervisorScope`, each upload under the pipeline's `Semaphore(route.parallelism)`
  (one per route, shared by every run, single objects included, so uploads per route never exceed the number
  rule 9 budgets); every child runs to its end whatever a sibling did and the first failure becomes the run's
  once all have finished. A STORED child whose `verify` is true skips its store (S28).
- `StateStore.childrenOf(id)`, one read, added to the seam: a parent's rows in id order. `verified(parent)`
  verifies every child's reference; `childRows` keeps the existing rows when the chain yields the same children
  (same names and digests, none FAILED) and calls `children(...)` to replace them otherwise.
- A child's failure is the child's attempt: `storeChild` calls `failedAttempt(child.id, ..., maxAttempts)` and
  throws `ChildFailed(terminal)`; the store fails the parent when the child reaches the cap (both stores already
  did), `failed()` charges nothing to the parent for a `ChildFailed` and nacks with redelivery unless terminal.

**Concepts named:** *kept children*: the rows a re-run reuses because the chain reproduced them, which is what
lets a child's `attempts` climb across redeliveries and a STORED child skip its store; *replaced children*: the
`children(...)` call, now reserved for a first run, a changed listing and a re-drive after a failed child.
*The route's upload budget*: the pipeline-level semaphore, distinct from the runner's pipeline permits (fetch
side); a parent run holds one runner permit while its children queue on upload permits, and nothing waits the
other way, so there is no cycle. *A death versus a crash*: `hook.crash` kills the coroutine parked at a point,
which for a child is one sibling of several; a process death is the runner's job cancelled
(`CrashMatrixTest.dieAt`), taking every upload with it.

**Acceptance:**

- *S27, S28, S29, S32 on fakes with the scripted fetcher; I16 and I23 as named tests* -
  `TransferPipelineTest.S27_image_sets_happy_path_a_message_expands_into_children_stored_in_parallel_acked_once_with_fetched_and_acked_delivered_once_each`
  (parallelism 2, `batchId` from the message, one `fetched` and one `acked` row on the parent, three fetches),
  `TransferPipelineTest.S28_half_the_children_stored_the_redelivery_verifies_them_stores_the_rest_and_acks_the_message_once`
  (`store 1.png, verify 2.png` on the redelivery, the same child rows, the attempt on the child),
  `TransferPipelineTest.S29_one_child_failing_five_times_fails_the_parent_the_message_is_not_acked_and_a_redrive_replaces_the_children_and_reruns_the_chain`,
  `CrashMatrixTest.I23_S32_a_parent_redelivered_after_ledger_ACKED_is_reacked_with_every_child_verified_and_no_new_outbox_rows`
  (the outbox equal before and after, no fetch, `verify` per child, `reacked` 1), and
  `TransferPipelineTest.I16_a_parent_is_acked_only_when_every_child_is_STORED_and_a_failed_child_fails_the_parent`
  (its second half now asserts the child's five attempts, the parent's zero, the parent's `lastError` naming
  the child, and the sibling's row kept). The single-object subscribe row `I8_subscribe_...` of ticket 08 stays.
- *Expand from a metadata file and from the message; extract from message* -
  `BuiltInProcessorsTest.expand_fetches_one_child_per_path_listed_in_a_json_metadata_file_or_in_the_message_and_rejects_an_absent_or_empty_list`,
  `BuiltInProcessorsTest.extract_from_message_sets_attributes_from_the_message_body_by_regex_or_json_and_rejects_a_message_without_one`,
  `RulesTest.rule14_expand_format_is_json_or_message_with_message_only_on_a_subscribed_route_and_files_a_pointer`.
- *A child failing five times fails the parent and the message is not acked; a re-drive replaces its children* -
  `S29_...` (four nacks with redelivery then one without, `failed` counted once, the chain re-run five times, a
  FAILED row does nothing, new child ids after the re-drive).
- *Ticket 08's two child crash rows* -
  `CrashMatrixTest.I8_S28_after_the_first_childs_store_before_its_ledger_the_redelivery_stores_it_again_and_the_rest_once`
  (three stores in all, one extra) and
  `CrashMatrixTest.I8_S28_after_the_first_childs_ledger_the_redelivery_verifies_it_and_stores_only_the_rest`
  (`store, verify, store`, the stored child's row kept); both end in `assertConvergedSet`.
- *Seam addition with a contract test on both stores* -
  `StateStoreContract.childrenOf_lists_a_parents_children_in_id_order_and_nothing_for_a_row_without_children`,
  green in `InMemoryStateStoreTest` and on Oracle (below).
- *Progress entry appended* - this entry.

Final run: ArchitectureTest 9, AttributeFreezeTest 4, BuiltInProcessorsTest 10, CrashMatrixTest 12,
MappingRendererTest 12, NotifierTest 13, ProcessingChainTest 4, RouteRunnerTest 9, RouteSupervisorTest 3,
RulesTest 36, SurfaceTest 3, TransferPipelineTest 29, HttpChannelTest 8, StateStoreSchemaTest 2,
SftpConnectorConfigTest 6, SftpPollSourceTest 9, ClockFixtureTest 1, FakeProcessContextTest 2, HookDriverTest 3,
InMemoryStateStoreTest 19, InMemoryTargetTest 3, RecordingChannelTest 2, ScriptedSourceTest 2, YamlLoaderTest 11;
212 tests, 0 failures, 0 errors (`oracle`, `minio`, `nats` excluded). Oracle tier
(`-DexcludedGroups=none -Dtest=JdbiStateStoreTest`): JdbiStateStoreTest 20, 0 failures, 0 errors, in 50 s
(the 18 shared contract tests including `childrenOf_...` plus the two Oracle-only ones).

**Deviations:**

1. **06's deviation 1 repaid by a seam read, not by `children` returning existing rows.** `childrenOf` is one
   `SELECT`; `children(id, staged)` keeps its replace-everything meaning, and the pipeline decides between the
   two from what the chain yielded. Spec 4.5's "a re-drive re-runs the chain and replaces its children" holds
   when any child is FAILED (the S29 re-drive) or the listing changed; a re-drive of a parent that failed for a
   reason of its own (a callback, say) with every child STORED keeps and verifies them, one `verify` each and no
   store, which is spec 4.3's STORED row applied to children and stricter than a blanket replace.
2. **06's deviation 2 repaid: a child's failure is the child's attempt.** The parent's `attempts` stays at zero
   through child failures; it is FAILED by the store's `failedAttempt` on the child at `maxAttempts`, with
   `lastError` naming the child. `shuttle_transfers_total{outcome=failed}` counts once, at that moment.
3. **Siblings finish when one child fails** (`supervisorScope`), so a transient failure on child 3 of 8 costs one
   re-upload on the redelivery, not six. A cancellation still takes every child at once.
4. **The upload permit covers the child's STORED ledger write and hook**, not only `target.store`: under
   parallelism 1 the second child cannot start before the first's row is written, which is what makes the two
   crash rows deterministic and keeps "at most `parallelism` uploads" true of the ledger's view as well.
5. **`format: lines` is not implemented.** Ticket 17 mentions a line list; the grammar has `format`, `files`,
   `from` and no knob a line list could use, and spec 13.1 shows JSON only. One `when` branch when a route needs it.
6. **`expand` has no cardinality cap** beyond what the metadata lists; spec 6.3 states none (unzip's D41 limits
   are its own). Two listed paths with one name collide on the key and S33 rejects the transfer.
7. **A single-object re-run of a former parent leaves stale child rows.** If a re-drive's chain yields one
   object where it once yielded N, the run stores on the parent row and the old children stay FETCHED/STORED;
   `verified` uses the row's own reference then, so nothing misbehaves, but the rows are debris. Clearing them
   would cost `childrenOf` on every single-object run; not paid.
8. **A message whose body lacks `fetch.path` is a stage error**, retried to FAILED at `maxAttempts`, not a
   Reject: spec 11 has no row for a malformed message and the run has no staged object to reject with yet.
9. **Size:** `git diff --stat` 381 insertions, 54 deletions across 12 files (about 115 net main, 215 net test);
   in budget.

**For the next ticket:**

- **14 (host):** a subscribed route's `RouteRunner` fetcher is the `fetch.store`'s: `S3Fetcher(client, bucket,
  io).fetcher` for an S3 store, the same client as the store's target if it has one. Pass `fetchers` to
  `TransferPipeline` only when a route's `expand.from` names a store other than its `fetch.store` (rule 14
  guarantees the name exists; the pipeline throws `IllegalStateException` at the first `ctx.fetch` otherwise).
  `fetch.path` is a JSON pointer into the raw message body and must resolve to a bare S3 key (ticket 11's
  fetcher takes `path` as the key). The NATS `SourceView` of ticket 16 is what `extract from: message` and
  `expand format: message` read; the runner's parallelism still bounds pipelines, and the pipeline's own
  semaphore bounds uploads, so a route's pool arithmetic under rule 9 is unchanged.
- **20 (M2 acceptance):** S27 to S29 and S32 on fakes are `TransferPipelineTest` and `CrashMatrixTest` above; the
  real-adapter re-proof is NATS redelivery after a process death (I23) and the parallel child uploads on the
  SFTP target under `parallelism` 2 with D42 on Oracle (`StateStoreContract.D42_...` already runs there).
  `CrashMatrixTest.dieAt` is the model for a death with children in flight: cancel the runner's job, not the
  hook.

---

## 14: Quarkus host, validate and try modes, admin, bounded shutdown

**Built:** `infra.shuttle.quarkus`, the composition root and the only package that imports Quarkus, Jakarta,
the connector, JDBI and jnats beside their own adapters (spec 3.2). `ShuttleHost(config, env, beans, store,
reads, registry, clock, targets, s3Client, natsConnection, httpClient, hook, io)` is a plain class with no
Quarkus in it: `start()` is spec 12.1 steps 2 to 7 in order (one read per state-store table through the seam,
`byId(0)` and `outboxPending()`, failing with the whole of `StateStoreSchema.DDL` in the message; every channel
constructed, so an `http` secret missing from the environment ends startup; one `S3Client` per S3 store shared by
target and fetcher and `probe()` on every route's target, a missing bucket named; every SFTP staging directory
emptied (D17); every `custom` and `provider` resolved through `NamedBeans` while the chains are built; then the
notifier launched, then the supervisor), `close()` is spec 12.3 (readiness false, routes cancelled and joined,
notifier cancelled and joined, S3 clients closed, NATS connections closed, all under
`withTimeoutOrNull(drainTimeout)`, a warning when it overran), `ready()` is the supervisor's rule gated by
started-and-not-shutting-down, and the seven operations of spec 14.1 are `routes()`, `transfers(route, state,
limit)` with children folded under their parents, `deliveries(id)`, `redrive(id)`, `ack(id)` (STORED to ACKED
with the route's `acked` requests plus `wake`), `redriveDelivery(id)` (plus `wake`), `restart(route)`, the three
writes answering `Outcome.DONE | NOT_FOUND | WRONG_STATE`. `ShuttleHost.load(files, env, beans)` is step 1:
`YamlLoader.load` then `Rules.validate`, throwing with every rule number listed. `NamedBeans(lookup)` resolves a
CDI name to a `Processor` or `Provider` and answers `produces(name)` for rules 15 and 17;
`StoreReads(transfers, outbox)` is the admin's read side over the two whole-table views both stores offer off
the seam. `ShuttleLifecycle` (`@Singleton`) builds and starts the host on `StartupEvent` unless `shuttle.mode`
says otherwise, reading the YAML paths from `shuttle.config`, the JDBI store from the Quarkus datasource the
YAML names (`AgroalDataSourceUtil.dataSourceInstance(name)`), and swapping in any `StateStore`, `StoreReads` or
`@Named("<store>") ObjectStoreTarget` bean that exists; `onStop` closes the host. `ShuttleClock` produces the
one `Clock`; `ShuttleReadiness` is the `@Readiness` check at `/q/health/ready`; `AdminResource` is the seven
endpoints under `@RolesAllowed("shuttle-admin")` at class level, 503 before the host is up, 404 and 409 from
the outcomes. `ValidateCommand(files, env, beans, out)` prints `rule <n>: ...` per violation and exits 1;
`TryCommand(files, env, beans, out, clock, route, fileName, sourcePath, content, message)` validates, runs the
route's chain over the sample in a temp directory through its own `ProcessContext`, prints
`step <i> <name>: attributes {...}` (and the objects when they changed), rule 17 judged again against the
attributes actually set, `key: ...` per object, and `body <channel> (<moment>):` with the rendered JSON per
notified channel; exit 0 only when clean. `ShuttleMain` (`@QuarkusMain`, a static `main`) puts the first
argument into `shuttle.mode` and runs `ShuttleApp : QuarkusApplication`, which dispatches `validate`, `try` or
`waitForExit`. `RouteSupervisor.restart(route)` is the one core addition: each run and each wait is a
cancellable phase, an operator restart cancels the current one and the backoff is `initial` again.

**Concepts named:**

- **The composition root is a plain class with the framework beside it, not inside it.** `ShuttleHost` takes
  its clients as factories and its test doubles as parameters, so spec 12's orders, I12, S15 and S18 are proven
  in plain JUnit against the embedded SSHD and a loopback server in 15 s; the one `@QuarkusTest` proves only
  what Quarkus adds: the role check, the health path, the scrape, JSON over HTTP.
- **A connector's life is a route's run.** One connector per polled route, started inside the route's event
  flow at every supervised start and closed in its `finally` under `NonCancellable`, so the connector's own
  probe refusing a password is one `RouteDown` for one route (S18, S23) and shutdown's "cancel the collectors"
  is what drains each connector under its own bound (12.3).
- **Rule 9's arithmetic is the pool size.** Each route's connector gets `parallelism + 1` sessions and a
  bulkhead of `parallelism`, so the routes on one store together never exceed the store's `maxSize`, which is
  the account's cap.
- **The step-2 round trip goes through the seam.** `byId(0)` and `outboxPending()` are one read per table on
  any store; a table that is not there surfaces as the adapter's exception, and the host names the DDL. No
  SQL lives outside the `jdbi` package.
- **`${VAR}` is the environment plus what MicroProfile Config knows under an upper-case name**, so a secret may
  arrive as an environment variable, through a mounted properties file, or as a test's config override; the
  YAML still holds only references (rule 25).
- **A restart is a phase cut short.** The supervisor runs each route's run and each wait as an `async` under a
  `supervisorScope`, keyed by route; `restart` cancels that job with the route marked, and the loop reads the
  mark as "start again now with `initial`" rather than as its own cancellation.

**Acceptance:**

- [x] I12: `ShuttleHostTest.I12_close_returns_within_drainTimeout_with_a_delivery_parked_and_PENDING_rows_stay_PENDING`
  (a loopback `HttpServer` that never answers; the notifier's request is parked; `close()` measured under the
  5 s `drainTimeout`; the row still PENDING; readiness false first).
- [x] S15: `S15_shutdown_during_store_leaves_the_row_PROCESSED_and_staging_is_empty_at_the_next_start`
  (`HookDriver` paused at `afterProcess`; the row stays PROCESSED; a stray file in staging is gone after the
  next `start()`; the row then finishes). S18:
  `S18_a_wrong_password_leaves_the_route_down_and_restarted_with_backoff_and_the_process_alive` (restarts
  counted, the server saw each attempt, readiness false, `close()` clean). S24:
  `S24_rule_9_ends_startup_naming_the_rule` and `ValidateCommandTest.S24_rule_9_is_reported_in_validate_mode`.
  S25: `ValidateCommandTest.S25_five_violations_print_five_rule_numbers_and_exit_non_zero` (rules 1, 3, 7, 12,
  25; exit 1; the command holds no client, so nothing can be opened). S31:
  `TryCommandTest.S31_prints_the_attributes_per_step_the_key_and_one_body_per_notified_channel` and
  `S31_a_mapping_naming_an_attribute_the_regex_does_not_produce_is_reported_by_rule_17`;
  `a_sample_name_the_regex_does_not_match_is_the_extract_step_rejecting_it`.
- [x] Missing table: `a_boot_with_a_missing_table_fails_naming_the_DDL` (a real `JdbiStateStore` over an
  in-memory H2 with no tables; the message carries `StateStoreSchema.DDL` and the `CREATE TABLE`). Missing
  bucket: `a_boot_with_a_missing_bucket_fails_naming_the_bucket` (a Mockito `S3Client` whose `headBucket`
  throws `NoSuchBucketException`; nothing is put). Readiness:
  `readiness_follows_the_configured_rule_with_one_route_up_and_one_down` (two stores on one server, one with a
  wrong password; `all-routes-down` ready, `any-route-down` not) and
  `ShuttleQuarkusTest.readiness_at_the_conventional_path_is_UP_once_the_route_is_up_and_the_meters_are_in_the_scrape`.
- [x] Admin: `the_admin_operations_change_exactly_what_spec_14_1_says` (every operation against the host, the
  manual ack on a row paused at `afterLedgerStored`, the delivery re-drive on a row parked in the stalled
  server, the restart counted and the route back up) and
  `ShuttleQuarkusTest.every_admin_endpoint_answers_under_the_role_and_changes_what_it_says`,
  `an_anonymous_caller_is_refused_on_every_endpoint` (401), `a_caller_without_the_admin_role_is_refused` (403).
  `RouteSupervisorTest.restart_cancels_the_current_run_and_a_restart_during_the_wait_cuts_it_short_and_resets_the_backoff`.
- [x] Bounded IO: `ShuttleHost.ioDispatcher(config)` is `Dispatchers.IO.limitedParallelism(sum of
  parallelism)`, handed to `JdbiStateStore`, `S3Target` and `NatsChannel`; metrics: `shuttle_route_up` found
  on the injected `MeterRegistry` and in `/q/metrics` (the Quarkus test).
- [x] Progress entry: this one. Suite after this ticket: 224 tests, 0 failures, 0 errors, 78 s wall clock
  (`ShuttleHostTest` 15 s, `ShuttleQuarkusTest` 9 s for one boot, `ArchitectureTest` 10 s, the rest under 3 s).

**Deviations:**

1. **Spec 12.1 step 6 is inside step 7.** Connectors are started by the route's own run, not before the
   notifier: `SftpConnector.start` runs the connector's probe, so a rejected password at boot would otherwise
   end the deployment, while S18 and S23 want one route down and the process alive. A store or channel probe
   still ends startup (step 3).
2. **Ticket 13's deviation 8, settled:** one connector per polled route, because the connector's polling
   configuration carries one `onAck` and one directory. The pool is sized to the route's share of rule 9:
   `maxSize = parallelism + 1`, `maxConcurrentTransfers = parallelism`, `minIdle` capped, through
   `SftpConnectorConfig.copy` on what `sftpConnectorConfig` built. Two routes on one server therefore register
   the connector's endpoint-keyed pool gauges twice; Micrometer keeps the first (the connector's own known
   limitation, T14 of its progress log). Debt: a connector-side `name` tag on those gauges.
3. **`${VAR}` reads MicroProfile Config too** (`environment()`), not the process environment alone.
4. **ArchitectureTest amended** for the composition root: `infra.shuttle.quarkus` is exempt from the "jdbi
   only", "jnats only", "connector only" and "no `Clock.systemUTC`" rules, per spec 3.2's "everything above,
   Quarkus"; the "quarkus is depended on by nothing" rule now has a subject and no `allowEmptyShould`.
5. **The Quarkus datasource is named.** `shuttleStateStore.oracle.datasource: shuttle` is
   `quarkus.datasource.shuttle.*` in `application.properties` (`db-kind=oracle`, URL, user and password from
   `SHUTTLE_DB_*`); with the URL unset Quarkus deactivates it, which is what the test relies on.
6. **Command modes** are a static `@QuarkusMain` main setting `shuttle.mode` before `Quarkus.run(ShuttleApp)`;
   picocli is not in the local repository. `ShuttleLifecycle` stays quiet unless the mode is `serve`.
7. **The missing-table boot is proven on H2** (a real JDBC database, `com.h2database:h2` test-scoped), not on
   Oracle; the Oracle tier is ticket 15's.
8. **Subscribe routes are half wired:** events come from `NatsChannel.events`, the fetcher throws
   `NotImplementedError` naming ticket 17 because `Fetch` carries no bucket for `S3Fetcher`. The SFTP target
   throws `NotImplementedError` naming ticket 18 in `ShuttleHost.targetFor`. A route fetching from S3 has no
   staging directory yet (`stagingFor`), also 17's.
9. **The admin's reads are whole-table views** (`JdbiStateStore.transfers()`/`outbox()`, the test kit's
   lists) filtered in memory, marked `ponytail:`; a `WHERE` on the store's view is the upgrade.
10. **Pom:** quarkus-arc, rest, rest-jackson, security, elytron-security-properties-file, smallrye-health,
    micrometer-registry-prometheus, agroal, jdbc-oracle (compile; ojdbc11 rides in, the explicit test-scoped
    ojdbc11 dropped); test: quarkus-junit5, quarkus-test-security, rest-assured, h2. `quarkus-maven-plugin`
    (build, generate-code, generate-code-tests), kotlin all-open for `ApplicationScoped`, `Singleton` and
    `Path`, surefire's `java.util.logging.manager` and `maven.home`. **`micrometer-core` pinned to 1.14.2**
    in `dependencyManagement`: the reactor parent's explicit 1.17.1 beat the child's BOM import, and Quarkus's
    registry binding then failed with `NoSuchMethodError: WarnThenDebugLogger.isEnabled()` against the
    1.14.2 commons; the module had been running a split Micrometer since ticket 01 without anything noticing.
11. **`TransferPipeline`, `RouteRunner`, `Notifier` untouched**; the host's own constructor takes `hook`,
    `io`, `targets`, `s3Client`, `natsConnection`, `httpClient` with production defaults.
12. **`try` mode on a sample the regex does not match** prints the extract step's `REJECT` and exits 1: the
    processor rejects before any attribute is set, so the runtime rule 17 check never sees it.
13. **Size:** ShuttleHost 390 lines, Commands 190, Lifecycle 120, AdminResource 80, tests 640; over the
    guideline because this is the host and every adapter is wired here once. Deep: one class owns both
    orders and the admin operations, and nothing wraps an adapter.
14. **The embedded users block has no default password**, unlike etl-host's: `shuttle-admin` expands
    `${SHUTTLE_ADMIN_PASSWORD}` with no fallback, so a deployment that forgets the variable refuses to boot
    (`SRCFG00011: Could not expand value SHUTTLE_ADMIN_PASSWORD`, raised while SmallRye builds the elytron
    realm's mapping) instead of starting with a credential that is public in this repository; the test tree
    supplies its own throwaway value in `shuttle/src/test/resources/application.properties`, which no test
    authenticates with because every security assertion here carries `@TestSecurity`.

**For the next ticket:**

- **15 (M1 acceptance):** boot the host the way `ShuttleHostTest` does: `ShuttleHost.load(files, env,
  NamedBeans.none)` on a YAML whose SFTP store points at `EmbeddedSftpServer` and whose S3 store points at
  the MinIO container (`S3Target.client` is the default `s3Client` factory: drop the `targets` override and
  the real target is built and probed), then `ShuttleHost(config, env::get, beans, JdbiStateStore(jdbi, io,
  clock), StoreReads(store::transfers, store::outbox), registry, clock, io = io)` with `io =
  ShuttleHost.ioDispatcher(config)` and `jdbi = Jdbi.create(<the Oracle container's URL>)`; `start()`,
  drop files, `close()`. Through Quarkus instead: `ShuttleQuarkusTest`'s `HostResource` plus test-tree
  producers; leave the `StateStore` producer out and set `quarkus.datasource.shuttle.jdbc.url` (and user,
  password) in the resource's overrides to reach the real datasource path, and leave the `@Named("minio")`
  target producer out for the real S3 client. Every `${VAR}` a test YAML needs goes into the resource's
  override map (`environment()` reads them from config). Rule 3 bites at test scale: with `drainTimeout: 5s`
  the store needs `drainTimeout: 1s, cancelGrace: 500ms` and a channel `timeout` under 5 s.
- **17 (expand):** `ShuttleHost.fetcherFor` is the seam for a subscribed route's fetcher and `stagingFor`
  for its staging directory; both throw naming you. `S3Fetcher(client, bucket, io).fetcher` wants a bucket
  that `Fetch(store, path)` does not carry: either a `bucket` on `fetch` (a YAML key, a DSL knob, a rule) or
  the pointer yielding `bucket/key`. The S3 client for a store is `s3ClientFor(store)` inside the host, one
  per declaration, closed at shutdown after the drain.
- **18 (SFTP target):** `ShuttleHost.targetFor(route)` throws `NotImplementedError` for an `SftpStore`
  target; replace that branch with your adapter. Sessions: a route that targets an SFTP store needs its own
  connector (or a client over one), and rule 9 already counts its `parallelism` against the store, so size
  its pool `parallelism` the way `share(route)` does for a poll. Close it in `close()` after the routes have
  joined, beside the S3 clients.
- **20 (M2 acceptance):** a subscribe route's events are already wired from `NatsChannel.events(RouteName,
  Source.Subscribe)` with one connection per `nats:` channel from `natsConnectionFor` (credentials file
  through `Nats.credentials`); the connection is closed after the drain. The callback ack is already passed
  through `channels` to every pipeline.
- **Gotchas:** `RouteSupervisor.restart` marks the route and cancels its phase; a mark left behind when no
  phase was running is consumed by the next cancellation, which then loops once more and exits at the next
  phase, so a stale mark cannot keep a cancelled supervisor alive. `ShuttleHost.close()` may be called after
  a failed `start()` (every field is null-checked). `ready()` is false until `start()` has returned, so a
  probe during boot is DOWN. A `@Singleton` Kotlin bean with a `private set` needs `final var` under
  all-open. The JDK `HttpServer` prints "Executor has been shut down" on `stop(0)` with a parked handler;
  noise, not a failure.

**Addendum (pre-20 wiring):** deviation 8 is closed - the three seams ticket 14 left for 17 and 18 are
filled, proven by `ShuttleHostM2WiringTest` (4 tests, plain JUnit over the embedded SSHD, `runBlocking`).
`targetFor` builds `SftpTarget(connectorFor(store).client, target.directory, io)`, and `connectorFor` is
**one connector per SFTP store used as a target or as a subscribed route's `fetch.store`**, opened at step 3
so the connector's own start-up and the target's `probe()` both end a bad deployment, shared by every route
on the store (a target connector needs no `polling` block, so `sftpConnectorConfig`'s `poll` is now nullable
and the block is emitted only for a poll), and closed in `close()` after the routes and the notifier have
joined, beside the S3 clients. Rule 9: `share(route)` became `sized(sessions, transfers)`; a polled route's
connector still takes `parallelism + 1` places and a bulkhead of `parallelism`, and a store's target/fetch
connector takes the sum of `parallelism` over the routes that target or fetch from it - together exactly
rule 9's per-store budget. `fetcherFor` decides on the subscribed route's `fetch.store`: an `S3Store` gives
`S3Fetcher(s3ClientFor(store), fetch.bucket, io).fetcher`, an `SftpStore` gives `sftpFetcher(client)`, which
is `stat` then `download(entry, into)` - the `StagedObject` shaping and the connector-digest reuse now live
in one private `staged(...)` in the `sftp` package, which `SftpPollSource.fetch` calls too. `fetcherFor` and
`stagingFor` are `internal`: a subscribed route's fetcher cannot be reached through a running host without a
broker, and that is the seam under test. `fetchers` on `TransferPipeline` is still empty - it is only for a
divergent `expand.from`, which no wired route has yet.

**Staging decision:** the fetch store's declared `Staging` when it is an SFTP store (rule 11 has checked
it), otherwise `<java.io.tmpdir>/shuttle-staging/<store name>`, created on demand and emptied at boot with
the declared ones (D17). No new YAML key and no new rule: a bucket has no local disk to name, the directory
is one per store as rule 11 wants, and nothing an operator would plausibly set differently is hidden - if
that turns out false, `staging` on an `s3` store is the upgrade, with rule 11 extended to it.

**One config addition, and it is a deviation:** `fetch.bucket` (`Fetch(store, path, bucket)`, the YAML key,
the DSL parameter). `S3Fetcher` needs a bucket, an S3 store declaration is an endpoint rather than a bucket,
and progress 11, 16 and 17 all settled that `fetch.path` yields a **bare key** - so the bucket had to be
configuration. It is nullable and unvalidated: a missing one ends startup at step 7 naming the route and the
knob, rather than at validate time. **Ticket 20 should extend rule 6** ("a `subscribe` source has a `fetch`
with a store and a path") to "and a bucket when that store is S3", with the `rule6_` test, and add
`bucket:` to spec 13.1's image-sets `fetch` block. Suite after this change: 243 tests, 0 failures, 0 errors.

---

---

## 15: Milestone 1 acceptance - S1 to S26 on real adapters

**Built:** `infra.shuttle.acceptance.M1AcceptanceTest`, one `@TestInstance(PER_CLASS)` suite tagged `acceptance`
(with the load method tagged `load`; both added to the pom's `excludedGroups`, so the default run stays fast).
One fixture starts once per class and is shared by every scenario: a Testcontainers Oracle behind an Agroal
`AgroalDataSource` (the same datasource path the host uses in production, built by hand here) with the 8.1 DDL
applied once; one MinIO container (versioning on, a fresh bucket per scenario) reached through the real
`S3Target` the host builds from the YAML; the connector's `EmbeddedSftpServer`; and one loopback JDK
`HttpServer` that records each request and answers per a swappable `respond` function. Every scenario writes
spec 13.1's vendor-drop and mirror YAML at test scale to a temp file, calls the real `ShuttleHost.load` then
`ShuttleHost(... JdbiStateStore over the Oracle container ...)`, `start()`s it, drops files on the embedded
server, and observes only through the containers, the server's directories, the loopback server and the host's
admin read operations. A crash is `host.close()` while a pipeline is parked at a `HookDriver` point; the process
restart is a second `ShuttleHost` over the same containers. No `Thread.sleep`; every wait is a `withTimeout`
plus a `delay` poll; the module's wall clock is a `ClockFixture` advanced by hand only where a scenario needs
time to pass (reconciliation's "older than", delivery backoff, `giveUpAfter`, `recheckFinished`).

**Concepts named:**

- **The fixture is the milestone, not the scenario.** Oracle and MinIO cost about a minute to start, so they are
  started once and every scenario resets only what it owns: both ledger tables emptied, the inbox and outbound
  directories recreated, a fresh versioned bucket, the loopback server's recording cleared, a fresh `HookDriver`.
- **A crash is a closed host; recovery is the next host.** `crash(host, point)` awaits the pipeline at a hook
  point, closes the host under its own `drainTimeout`, and swaps in a fresh `HookDriver` so the recovery host
  (built over the same containers) runs to the end. This is the crash matrix (progress 08) replayed on real
  adapters: S2 (store, before ledger), S3 (ledger STORED), S4 (move, before ledger ACKED -> reconciliation),
  S5 (delivery sent, before ledger), S6 (copy missing at STORED).
- **The poll interval has to exceed one pipeline's Seen-to-fetch latency.** The connector's D40 sweep nacks any
  handed-over file whose pipeline has not reached the fetcher by the *next* `PollStarted` (ticket 13 deviation 4).
  A cold Oracle pool makes the first `find`/`seen` take ~2 s, so the fixture (a) warms the pool and Oracle's
  shared SQL area once in `@BeforeAll` and (b) polls every 5 s. Below that margin the abandon races the pipeline
  into a double-store and a double-ack; this is a property of test timing, not of the code.
- **A frozen clock is the safe default; time is advanced per scenario.** With `updated_at == poll.startedAt`,
  reconciliation's strict "older than" never fires, so it cannot race a live pipeline into a duplicate `acked`.
  S4 advances the clock once before the recovery host; S7/S9/S17 advance it in the background (`withClockTicking`)
  so delivery retries become due; S12 sets `recheckFinished: 0s`.

**Acceptance:** all in `M1AcceptanceTest` unless noted. Suite: 22 `acceptance` tests + 1 `load` test.

- [x] One suite covers S1 to S26 end to end, each named by id.
- S1 `S1_vendor_drop_happy_path_one_file_one_channel` (I1, I2, I3, I10, I11, I15, I20): file on the SSHD ->
  extract+rename+zip -> object in MinIO under `vendor/20260101-123-order.csv.zip` with the archive's own MD5 and
  the attributes in its metadata -> row DONE in Oracle -> file in `temp/` -> one delivery at the loopback server
  carrying the reference `r-1`, `fileId`, `orderNumber`, `event=acked` and the location.
- S2 `S2_crash_after_store_before_ledger_stores_again_leaving_one_current_and_one_non_current_version` (I6, I8):
  one version before the crash, two after recovery, the current one the row's ref, no delete marker. The crash
  *inside* `store` between PUT and HEAD is the S3 adapter's own contract, proven on MinIO in
  `S3TargetTest.I6_three_stores_read_back_the_newest_by_key_a_crash_between_PUT_and_HEAD_is_repaired_by_the_next_store`.
- S3 `S3_crash_after_ledger_STORED_verifies_and_acks_with_no_second_store` (I8): one version, moved on recovery.
- S4 `S4_crash_after_the_move_before_ledger_ACKED_is_repaired_by_reconciliation` (I8): `shuttle_reconciled_total`
  >= 1, nothing re-stored, the delivery delivered.
- S5 `S5_crash_after_delivery_sent_before_ledger_delivers_again_two_calls_one_row_DELIVERED_once` (I8): two
  loopback calls with one transfer id, the row DELIVERED once.
- S6 `S6_copy_missing_at_STORED_is_stored_again_on_the_same_row_and_reaches_DONE` (I1): the version deleted by
  hand, verify false, a fresh version on the same row.
- S7 `S7_downstream_503_twice_then_200_delivers_at_the_third_attempt`; S8 `S8_downstream_400_fails_the_delivery_and_leaves_the_transfer_ACKED` (D9);
  S9 `S9_downstream_down_past_giveUpAfter_is_FAILED_and_a_redrive_delivers_it` (gave_up then an admin re-drive);
  S17 `S17_two_channels_on_acked_one_always_503_the_other_delivers` (I13: the good channel DELIVERED, the transfer stays ACKED, the bad one still PENDING).
- S10 `S10_processor_Reject_is_REJECTED_nothing_stored_and_the_object_stays`; S12 `S12_same_identity_re_dropped_after_DONE_is_reacked_with_no_store_and_no_delivery` (S12, I24 same-digest half);
  S16 `S16_state_store_unavailable_for_one_poll_then_completes`; S19 `S19_mirror_route_with_no_notifications_goes_to_DONE_and_creates_no_outbox_row` (I17);
  S20 `S20_rename_then_zip_stores_one_archive_under_the_renamed_key_with_a_different_digest`;
  S21 `S21_an_extracted_attribute_reaches_the_body_and_an_undeclared_one_fails_rule_17`;
  S22 `S22_one_provider_selected_by_three_rows_is_invoked_once_and_fills_three_paths` (I22);
  S26 `S26_missing_required_attribute_at_freeze_fails_before_the_store`.
- S18 `S18_a_wrong_password_leaves_the_route_down_and_restarted_with_backoff_the_process_alive` (I21);
  S23 `S23_two_routes_one_dead_the_other_keeps_completing_and_readiness_stays_true` (I21, all-routes-down);
  S24 `S24_pool_arithmetic_exceeded_is_rejected_by_rule_9` (I14); S25 `S25_validate_mode_on_a_file_with_five_violations_lists_five_rule_numbers_and_exits_non_zero` (I14).
  These are also proven host-level in `ShuttleHostTest` (S15, S18, S24) and `ValidateCommandTest` (S25) on the
  embedded SSHD; here they run over the real Oracle and MinIO too.
- S14 (truncated listing skips reconciliation): proven at the adapter level in
  `SftpPollSourceTest.a_listing_that_reaches_maxFilesPerPoll_completes_truncated` (a real server, `maxFilesPerPoll`
  tuned down) and at the runner level in `RouteRunnerTest.S14`. There is no `maxFilesPerPoll` knob on `SftpStore`
  (ticket 13 deviation 7), so the host cannot force truncation without ~1000 files; it is not re-run here.
- S15 (shutdown during store/delivery): proven host-level in `ShuttleHostTest.S15_...` and `...I12_...` against the
  embedded SSHD and a loopback server; every crash scenario here also closes a host cleanly within `drainTimeout`.

- [x] S13 at scale: `S13_a_batch_of_files_all_reach_DONE_with_in_flight_bounded_and_staging_bounded` (`@Tag("load")`).

  **S13 measurements.** The full scale (5,000 files x 10 MB = ~50 GB) will not fit on this disk, so it was scaled
  DOWN in size and count to **200 files x 64 KiB (~12.8 MB total)** at `parallelism: 4` with a 60 s poll interval
  (one tick lists the whole batch and drains it before the next tick, so the in-flight bound - not the poll - is
  the backpressure). Measured, all asserted green: **all 200 reach DONE** with one current MinIO version each;
  **in-flight never above `parallelism`** (the `shuttle_inflight` gauge sampled through the drain stays <= 4);
  **staging bounded** (at most `parallelism` run directories under the staging dir at any instant); **no skipped
  poll** (`shuttle_poll_total{result=skipped}` is 0). Wall clock ~30 s of drain (45 s including container start).
  Extrapolation (open item, not a pass): the run is I/O-bound on the MinIO PUT per object; at the full 10 MB
  object size and 5,000 count the invariants (in-flight <= parallelism, staging bounded, all DONE) are unchanged,
  but the wall clock and disk footprint were not exercised here and would need a machine with ~50 GB free and a
  longer budget.

- [x] Spec Sec 17 items 1 to 8 and 11 re-checked:

  1. **MinIO version / SSE.** The fixture runs `minio/minio:RELEASE.2024-10-02`, versioning on, no server-side
     encryption. The ETag-equals-MD5 rule holds on the single-part unencrypted objects (S1 and S20 read the
     object's own MD5 back out of its metadata and match it; `S3TargetTest` proves the ETag check itself and the
     encrypted-bucket WARN fallback). **Closed** for the acceptance environment.
  2. **Downstream tolerates repeated calls per id+event and returns a per-call reference.** Exercised: S5 makes
     two calls with one transfer id and the row ends DELIVERED once (the receiver's dedup obligation), and every
     delivery stores the per-call `requestId` the server returned (S1: `r-1`). **Closed** at the seam; the real
     downstream's idempotency remains the integrator's to confirm against their endpoint.
  3. **Uploader's write convention on the vendor SFTP server.** M1's target is S3 only (spec 18), so this is not
     exercised here. **Open**, owned by ticket 18 / M2 (the SFTP target's `.part`-then-rename), re-checked at M2
     acceptance (ticket 20).
  4. **Temp folder ownership on the vendor server.** The move-to-`temp/` ack works against the embedded server
     (S1, S3 read the file out of `drop/temp/`), so the mechanism is proven; ownership on the real vendor server
     is infra's. **Open** (real server), infra.
  5. **Lifecycle rule expiring non-current versions.** `probe` warns when it is missing and is silent with it
     (proven in `S3TargetTest`); the acceptance buckets carry no lifecycle rule, so every boot logs the D5 WARN,
     which is the intended behaviour (the process works, only the bucket grows). The rule existing before the
     first deployment is **open**, owned by infra/DBA.
  6. **Top-of-hour alignment.** Not implemented by design (D12: the connector's `watch` polls on a fixed
     interval, no Quarkus scheduler). **Open / won't-fix** unless a requirement forces it.
  7. **Oracle schema and sequence names.** **Closed.** The full 8.1 DDL, including `file_transfer_seq` and
     `delivery_outbox_seq` (ticket 10), applies cleanly to the real `gvenzl/oracle-free` container and the whole
     `StateStore` seam runs against it across all 26 scenarios; the state store shares the Agroal datasource the
     host resolves by name.
  8. **Pod termination grace 90 s.** The manifest is infra's; M1 proves the process side, that `close()` returns
     within `drainTimeout` under load and at every crash point (I12 in `ShuttleHostTest`, and every crash
     scenario here closes cleanly). **Open** (manifest value), infra.
  11. **Connector D21's five-session cap vs infra's 20 per account.** Appeal, recorded here rather than in the
     connector's log: infra now grants 20 sessions per account, so the connector's D21 "five-session cap" is a
     floor, not the ceiling. Shuttle sizes each polled route's pool at `parallelism + 1` against the store's
     `maxSize` (rule 9), so a single account hosting the vendor-drop (parallelism 4) and mirror (1) routes needs
     ~7 sessions, comfortably inside 20; the connector's own log should raise its recorded cap to 20 to match.

- [x] Every behaviour that differs from the spec is a recorded deviation with a decision entry: D43 below.
- [x] Progress entry appended: this one.

**Deviations:**

1. **D43 (new decision entry, spec 16; S20 row and the 8.1 `stored_name` comment amended in place).** The transfer
   row's `stored_name`, `digest` and `stored_mtime` are the *fetched source* object's, written at the FETCHED
   transition and never updated by the chain, so after rename+zip the ledger row - and therefore the notification's
   `STORED_NAME`/`DIGEST` - carry the source name and digest, not the stored object's. The target object itself is
   correct: the pipeline writes the processed object's `source-name` and `digest` into its S3 metadata, so
   downstream can read the true stored name and digest off the object (S1 and S20 assert exactly this against
   MinIO). This contradicts S20's "STORED_NAME differs from SOURCE_NAME; SOURCE_DIGEST and DIGEST differ". The
   clean fix threads the processed `StagedSummary` into the `stored` seam method, whose signature is frozen for
   this ticket, so it is deferred to a follow-up; recorded, not fixed here. No production code was changed for it.
2. **The mirror route targets S3, not the SFTP `partner`.** Spec 18's M1 plan says "S3 only" (the SFTP target is
   M2, ticket 18), so the mirror route here writes to a `mirror/` key prefix of the same bucket rather than to
   `partner`. A test-fixture choice consistent with the M1 plan, not a spec deviation.
3. **No production change.** Every scenario passed on the code as tickets 06 to 14 left it; the only spec edit is
   D43's, which records a gap rather than changing behaviour. The concurrently-edited `targetFor`/`fetcherFor`/
   `stagingFor` were not touched.
4. **Test-harness scaffolding (not spec):** a 5 s poll interval and a one-shot pool/SQL warmup to clear the D40
   cold-start race; a `ClockFixture` advanced by hand or by a background ticker where a scenario needs elapsed
   time; per-store staging subdirectories (rule 11 forbids two SFTP stores sharing one staging dir); an Agroal
   datasource built by hand to reach `JdbiStateStore` the way the host's `ShuttleLifecycle` does in production.

**Size:** `M1AcceptanceTest` is 771 lines for 23 tests (26 scenario ids plus the load method, several ids folded
into one adapter-level assertion and five referencing the host tests). Over the 200-600 guideline, as the ticket
allows for an acceptance suite of this breadth; one shared fixture, each scenario kept to its assertion.

**Counts (surefire):** default run 239 tests, 0 failures, 0 errors, ~86 s wall clock (per class: M1AcceptanceTest
excluded by the `acceptance` tag; ShuttleHostTest 14.1 s, ShuttleQuarkusTest 13.0 s, ArchitectureTest 10.7 s,
SftpPollSourceTest 2.5 s, SftpTargetTest 1.2 s, the rest sub-second). `acceptance` group: 22 tests, 0 failures,
0 errors, 59.8 s wall clock (incl. Oracle + MinIO start). `load` group: 1 test, 0 failures, 0 errors, 45.2 s.

**For the next ticket:**

- **20 (M2 acceptance):** reuse this fixture wholesale. The seams are the same: `boot(text)` / `bootR(routes,
  channels, beans)` build a real host, `crash(host, point)` + a fresh host is the process restart, `withClockTicking`
  and `clock.advance` supply elapsed time, and the loopback `HttpServer` with its swappable `respond` is the HTTP
  channel. Two things change for M2. (a) **NATS:** add a Testcontainers NATS beside Oracle and MinIO in
  `@BeforeAll`, publish `images.ready` messages onto it, and leave the `@Named` producers out so the host builds
  the real `NatsChannel`; the subscribe trigger, `fetch` from S3 and the callback ack (S27-S32) then run through
  `ShuttleHost.eventsFor`/`fetcherFor` once ticket 17 wires `S3Fetcher` and `stagingFor` for a subscribed route.
  (b) **The SFTP target as the partner server:** declare a second SFTP store (its own staging dir, per rule 11)
  pointing at the same `EmbeddedSftpServer`, and point the M2 routes' target at it; ticket 18's `targetFor`
  branch must be built for `SftpStore`. The crash matrix rows for M2 (S28 half the children stored, S32 the
  subscribe redelivery after ledger ACKED) are the `dieAt`/`crashAt` shapes from `CrashMatrixTest`, replayed by
  closing and reopening a host over the containers, exactly as S2-S6 do here.
- **The D43 follow-up:** if a downstream consumer needs the *stored* name/digest in the notification rather than
  the source's, thread the processed `StagedSummary` into `StateStore.stored` (a frozen-seam change) and update
  both stores; ticket 15's S1/S20 already assert the object metadata, so a fixed ledger would let the row assert
  it too.

---

## 20: Milestone 2 acceptance - S27 to S30 and S32 on real adapters

**Built:** `infra.shuttle.acceptance.M2AcceptanceTest`, one `@TestInstance(PER_CLASS)` suite tagged `acceptance`, over
the fixture ticket 15 built, now extracted into `AcceptanceFixture` (abstract, same package) so both milestones share
it without a copy: the Testcontainers Oracle behind Agroal with the 8.1 DDL, MinIO (a fresh bucket per scenario), the
connector's `EmbeddedSftpServer`, the loopback JDK `HttpServer` with its swappable `respond` and `release` latch, the
`ClockFixture`, `boot`/`bootR`/`load`, `crash(host, point)`, `withClockTicking`, `await`/`awaitState`, `yaml`/`sftpStore`/
`downstream`; `M1AcceptanceTest` keeps its scenarios and its own route builders, unchanged in behaviour (23 tests green
after the move). M2 adds NATS JetStream from `NatsChannelTest`'s `NatsBroker` (the `nats:2.10-alpine` container with
`-js`), one stream and one durable pull consumer named after the route per scenario with a two second ack wait, spec
13.1's image-sets route at test scale (`subscribe` on NATS, `fetch` from MinIO by pointer, `extract from: message`,
`expand` of a JSON metadata file, a pass-through `custom: imageResizer` bean, the SFTP target on the embedded SSHD as
`partner`, `fetched` to `upstream-receipt` and `acked` to `downstream`), booted through `ShuttleHost.load` and
`ShuttleHost(...)` exactly as M1 boots. Observation is only through the ledger's read views (`transfers`, `outbox`,
`childrenOf`), the partner's directory on local disk, the loopback server's received requests, and the consumer's
state at the broker (`JetStreamManagement.getConsumerInfo`: delivered sequence, ack pending, ack floor).

Three production changes, none to a core seam, each forced by a measurement:

- **Rule 6 extended** (ticket 14's addendum): a `subscribe` route whose `fetch.store` is an S3 store states a `bucket`.
  One line in `Rules.route`, `RulesTest.rule6_a_subscribe_source_fetching_from_an_S3_store_states_a_bucket` (rejected
  without, accepted with, and an SFTP fetch store needs none), `bucket: images` in spec 13.1 and in the test tree's copy.
- **The outbox insert is idempotent on (transfer, `on_state`, channel)** in `JdbiStateStore` (`INSERT ... SELECT ...
  FROM dual WHERE NOT EXISTS`) and in the test kit's `InMemoryStateStore`, with
  `StateStoreContract.I20_a_transition_run_again_after_a_crash_keeps_its_existing_notification_row` green on both (D44).
- **The NATS client runs on the unbounded `Dispatchers.IO`** in `ShuttleHost.channelFor`, not on the module's bounded
  view (D45).

**Concepts named:**

- **The broker's ack state is a seam.** "Acked once", "redelivered", "not acked" and "termed" are all readable off the
  consumer without touching the adapter: `delivered.consumerSequence` counts deliveries including redeliveries,
  `numAckPending` is what the broker still owns, and `numRedelivered` is redelivered *and still unacked*, so it is 0 the
  moment the redelivery is acked (which cost one wrong assertion).
- **The message id is the identity; the duplicate window is the re-drive's gate.** Both the stream sequence and
  `Nats-Msg-Id` come back unchanged on a redelivery, so a redelivery re-enters the same row (S28, S32). A re-drive of a
  subscribed transfer has no trigger of its own, because the adapter terms the message when the transfer is FAILED: the
  upstream must publish again under the same `Nats-Msg-Id`, and JetStream drops that republish as a duplicate inside
  the stream's duplicate window (S29 found this the hard way: five attempts took 350 ms, the republish fell inside a
  500 ms window and was never stored).
- **A row exists after the transition.** Spec 4.4's "next trigger does a full run" repeats FETCHED on the same row; on
  Oracle the 8.1 index refused the second `fetched` row and walked the parent to FAILED in five naks, while the fake
  had been quietly appending a second row. Neither was the seam's meaning; the DDL was.
- **The trigger's long-poll is not IO work.** `subscription.fetch(1, 1 s)` on a view of one thread held the route's
  whole IO budget: every ledger write and every fetch waited behind it and each attempt took five seconds. Off the view
  the same run takes under two.
- **"Half stored" is a permit, not an order.** Under `parallelism: 1` the upload permit (ticket 17's deviation 4)
  guarantees exactly one child STORED at a crash, but which child wins the permit is scheduling; the assertion says
  "one STORED, and it is the one on the partner".

**Acceptance:** all in `M2AcceptanceTest` unless named otherwise. Suite: 5 `acceptance` tests, 0 failures, 58 s.

- [x] One suite covers S27 to S30 end to end, each named by id; S32 too.
- S27 `S27_image_sets_happy_path_children_stored_on_the_partner_message_acked_once_fetched_and_acked_delivered_once_each`
  (I10, I16, I20, D28, D43): three children stored in parallel on the partner with no `.part` left, the parent DONE
  with `batchId` from the message, delivered once, ack pending 0, ack floor 1; two loopback requests, `/api/received`
  with `SOURCE_PATH` `events:<subject>/1` and the metadata file's digest, `/api/files` with the parent id, `kind`
  `message`, `event` `acked`, `batchId`, D43's source name (`b-1.json`) and digest, and no `location` (the rows are
  `required: false`); the partner saw at most the route's `parallelism` in sessions.
- S28 `S28_crash_with_half_the_children_stored_the_redelivery_verifies_them_stores_the_rest_and_acks_once` (I8, I16):
  crash at `afterLedgerStored` under `parallelism: 1`, parent PROCESSED, one child STORED and one FETCHED, one copy on
  the partner, the message still the broker's; the recovery host's redelivery keeps the child rows (same ids), the
  stored child's `TargetRef` and the partner file's mtime are unchanged (verified by size and mtime, not stored again),
  the rest stored, delivered twice and acked once, `fetched` and `acked` delivered once each.
- S29 `S29_one_child_failing_five_times_fails_the_parent_the_message_is_not_acked_and_a_redrive_reruns_the_chain` (I16):
  a folder on the partner where `2.png` must land makes every rename over it `SSH_FX_PERMISSION_DENIED`; five
  deliveries later the child has 5 attempts, the parent 0 and FAILED, the sibling STORED, `failed` counted once,
  nothing told downstream, the message termed (ack pending 0). The operator removes the folder, `redrive` answers DONE
  and the row is SEEN; the upstream republishes under the same `Nats-Msg-Id`, retried until the publish ack is not a
  duplicate; the chain re-runs, the children are replaced (new ids, spec 4.5), both files land, downstream is told once
  and upstream's `fetched` is not repeated (its row existed, D44).
- S30 `S30_a_callback_ack_answering_500_then_200_keeps_the_transfer_STORED_through_the_failure_and_ACKED_after_with_one_acked_delivery`:
  `onAck: { callback: upstream-ack }` on the subscribed route; the first callback is held on the loopback latch while
  the row is seen STORED with 0 attempts, no `acked` row and the message unacked at the broker; released with 500 it is
  one failed attempt and a nak; the redelivery verifies the children, calls again (200), then ledger ACKED, broker ack,
  one `acked` delivery; both callback bodies carry `event: acked`.
- S32 `S32_crash_after_ledger_ACKED_before_the_broker_ack_the_redelivery_reacks_with_children_verified_and_no_new_outbox_rows`
  (I23): downstream answers 503 until the crash so the `acked` row is PENDING under the frozen clock; crash at
  `afterLedgerAcked`, parent ACKED, ack pending 1; the recovery host's redelivery is `reacked` once, the outbox holds
  exactly the rows the ledger wrote before the crash, every child's file on the partner keeps its mtime, delivered twice,
  acked on the redelivery, then DONE under the ticking clock.
- The state store half of S28: `StateStoreContract.I20_a_transition_run_again_after_a_crash_keeps_its_existing_notification_row`
  in `InMemoryStateStoreTest` (default tier) and `JdbiStateStoreTest` (Oracle tier, 21 tests, 0 failures).
- [x] Spec Sec 17 items 9 and 10 re-checked, each closed for what the fixture can show and left open with what is
  missing, in the spec itself (item 9: the real consumer's ack wait, `MaxDeliver` at least `maxAttempts`, the duplicate
  window a re-drive must fall outside; item 10: whether the real partner advertises `posix-rename@openssh.com`, and its
  session cap against `partner.pool.maxSize`).
- [x] Every behaviour that differs from the spec is a recorded deviation with a decision entry: D44 to D47 below.
- [x] Progress entry appended: this one.

**Final run counts (surefire).** Default tier (`mvn -B -o -q -pl shuttle test`): 30 classes, 245 tests, 0 failures,
0 errors, about 90 s wall clock; per class: ArchitectureTest 9, AttributeFreezeTest 4, BuiltInProcessorsTest 10,
CrashMatrixTest 12, MappingRendererTest 12, NotifierTest 13, ProcessingChainTest 4, RouteRunnerTest 9,
RouteSupervisorTest 4, RulesTest 37, SurfaceTest 3, TransferPipelineTest 29, HttpChannelTest 8, StateStoreSchemaTest 2,
ShuttleHostM2WiringTest 4, ShuttleHostTest 9, ShuttleQuarkusTest 4, TryCommandTest 4, ValidateCommandTest 4,
SftpConnectorConfigTest 6, SftpPollSourceTest 9, SftpTargetTest 5, ClockFixtureTest 1, FakeProcessContextTest 2,
HookDriverTest 3, InMemoryStateStoreTest 20, InMemoryTargetTest 3, RecordingChannelTest 2, ScriptedSourceTest 2,
YamlLoaderTest 11. `-DexcludedGroups=none`: M1AcceptanceTest 23 tests, 0 failures, 64 s (S13 `load` included);
M2AcceptanceTest 5 tests, 0 failures, 58 s; JdbiStateStoreTest 21 tests, 0 failures, 46 s.

**Deviations:**

1. **D44, a production change in `jdbi` and the test kit.** The outbox insert skips a row that already exists for the
   transfer, moment and channel. Spec 9.1 amended; the fake had been creating a second `fetched` row on a re-run, so
   `CrashMatrixTest`'s after-fetch and after-process rows had never had a `fetched` notification to show it. A re-driven
   transfer therefore does not tell upstream `fetched` again (S29 asserts it); if a consumer wants a "re-driven" moment,
   that is a new `on:` value, not a second row.
2. **D45, a host change.** `NatsChannel` is built on `Dispatchers.IO` instead of the bounded `io`, for the trigger's
   long-poll; its `deliver` (a short publish) rides along. Spec 3.3 amended. The bounded view still carries JDBI, S3,
   the HTTP channel and archive writing.
3. **D46, a re-drive of a subscribed transfer needs a republish** under the same `Nats-Msg-Id`, outside the stream's
   duplicate window; the adapter's `term` at FAILED (ticket 16) is kept, because a `nak` would redeliver a FAILED row for
   ever. Spec 5.3 amended. Without a publisher-set id the republish is a new identity and the re-driven row stays SEEN.
4. **D47, spec 13.1 corrected in three places:** `fetch.path` is `/metadata/path` (the written `/metadata.path` names a
   key called `metadata.path` and every message failed to FAILED in five immediate naks); `fetch` states `bucket`
   (rule 6); the `downstream` rows for `TARGET_SIZE`, `TARGET_LOCATION`, `TARGET_KEY` and `SOURCE_MTIME` are
   `required: false`, because a message parent has none of them and the renderer's `MappingFailure` rejected its
   `acked` notification outright. `shuttle/src/test/resources/spec-13-1.yaml` mirrors the block, so
   `YamlLoaderTest.the_spec_13_1_document_...` keeps proving the reference configuration passes its own rules.
5. **Spec 7.3 amended for ticket 18's deviations 2 and 3** (`verify` compares size and mtime; `probe` is a stat of the
   directory), confirmed here: S28 and S32 keep every partner file's mtime across the verify.
6. **"Message not acked" in S29 is "termed".** The broker cannot tell an ack from a term in `ConsumerInfo`; the
   assertion is five deliveries, ack pending 0, and the operator-visible FAILED row with the reason.
7. **Test-scale choices, not spec:** ack wait 2 s with `inProgressEvery: 500ms` (D38 at test scale, the operator's rule
   "below the ack wait" kept); the stream's duplicate window 500 ms so S29's republish is reachable inside a test; the
   stream in memory; `parallelism: 1` for S28 and S29 so "half stored" and "one child fails" are deterministic
   (S27 keeps spec 13.1's 2 and exercises the parallel uploads and D42 on Oracle); the callback channel's timeout 4 s
   so S30's held request outlives the assertions; S32's 503-until-the-crash so the `acked` row is deterministically
   PENDING under the frozen clock; `upstream-receipt`'s `${UPSTREAM_KEY}` supplied by overriding the fixture's `env`.
8. **The fixture extraction.** `M1AcceptanceTest` lost 279 lines to `AcceptanceFixture`; `versions`/`head`, the route
   builders and the M1 constants stayed with it; `BODY` moved to the fixture because `downstream()` defaults to it. No
   scenario changed.
9. **A refused rename leaves `<key>.part` on the partner** until the next store of that key takes it back (S29's five
   failures left `2.png.part` beside the folder); a FAILED transfer nobody re-drives leaves it for ever. The adapter's
   own name, harmless to a watcher of the final name (ticket 18), but an operator will see it.
10. **Size:** `M2AcceptanceTest` 394 lines for 5 scenarios, `AcceptanceFixture` 302 (moved, not new), production 8
    lines across three files, `RulesTest` +13, `StateStoreContract` +20, spec +37/-14. Over the 200-600 guideline as
    the ticket allows for the acceptance suite; one shared fixture, each scenario its own assertions.

**Open items 9 and 10:** in spec 17, each with what was measured and what remains the operator's or infra's.

**For the next ticket / open:**

- **The D43 follow-up** stands as ticket 15 left it.
- **`MaxDeliver` on the real consumer.** If the operator sets it below a route's `maxAttempts`, the broker stops
  redelivering before the transfer is FAILED and the row stays STORED or FETCHED with no trigger; the process cannot
  read it (spec 5.1). A boot-time consumer info read would make this a startup failure; not built.
- **The real partner's `posix-rename`.** Without the extension the connector's REPLACE is a delete then a rename and
  the key holds nothing in between; spec 7.3's "exactly one copy" still holds, a watcher of the final name may see it
  absent for a moment.
- **A `.part` after a refused rename** (deviation 9): a sweep or an ops note.
- **Running M2 alone costs about 60 s** of container start (Oracle dominates) plus 58 s of scenarios; with M1 in the
  same JVM MinIO and NATS are shared and Oracle is started once per class.

---

## 23: Fix: a finished identity is re-fetched at most once per `recheckFinished`

**Built:** the re-ack path now writes one thing: the row's `updated_at`. `StateStore.reacked(id)` is a one-column
touch in both adapters (`UPDATE file_transfer SET updated_at = :now WHERE id = :id`; the fake's `update(id) { this }`),
called from `TransferPipeline.reack` after the source's ack action and before the `reacked` count. D40's window is
measured from `updated_at`, so before this a DONE file that stayed under `onAck: none` was skipped for one window and
then downloaded and digested on every poll for the rest of its life (review finding Spec 2). Now each re-check restarts
the window from itself: 23 h skip, 25 h fetch, 26 h skip.

**Concepts named:** `reacked` is now a ledger transition as well as an outcome: the state store's name for "acked
again, nothing else changed". It creates no outbox row, moves no state, and leaves `acked_at` and `completed_at` as
the first ack wrote them; the only visible effect is the window restarting.

**Acceptance:**

- [x] Three polls at 23 h, 25 h, 26 h on the fakes; red before the fix (the third poll fetched):
  `TransferPipelineTest.SPEC2_a_finished_identity_is_refetched_at_most_once_per_recheckFinished`. `D40_` and `S12_`
  now assert `done.copy(updatedAt = clock.instant())` where they asserted the untouched row (S12 advances the clock a
  minute first so the assertion is not vacuous).
- [x] Both stores under the contract: `StateStoreContract.reacked_advances_updated_at_and_changes_nothing_else` on a
  DONE row and on an ACKED row with a PENDING delivery; `InMemoryStateStoreTest` 21/21, `JdbiStateStoreTest` 22/22 on
  the Oracle container (`-DexcludedGroups=none -Dtest=JdbiStateStoreTest`, 115 s).
- [x] `shuttle_transfers_total{outcome=reacked}` is unchanged in meaning: still one increment per re-ack in `reack`;
  SPEC2 asserts exactly 1.0 across the three polls, `CrashMatrixTest` 12/12 and the subscribe re-ack tests still see 1.0.
- [x] Progress entry appended.

Default tier: every class green except `ShuttleHostTest`'s readiness assertions and `ShuttleQuarkusTest`'s boot, both
environmental: `ShuttleQuarkusTest` died on port 8081 already bound by a sibling agent's tier and passed 4/4 alone;
`ShuttleHostTest`'s `ready()` assertions fail on the base commit as well with this change stashed (timing against the
supervisor's backoff), so they are not this ticket's.

**Deviations:**

1. **A seam method, not an idempotent `acked`.** The ticket preferred making `acked` idempotent-and-touching. On
   paper: `acked` writes `acked_at = now`, its `finishWhenAllDelivered` writes `completed_at = now`, and a DONE row
   with a FAILED delivery would drop back to ACKED, so idempotence needs three guards (`CASE`/`COALESCE`) in both
   adapters for a transition whose name would then lie. `reacked(id)` is one line per adapter and says what it does.
   Spec 8.2's listing is not edited, following the precedent of `byId` and `outboxPending` (ticket 09) and
   `childrenOf` (ticket 17): the seam is four methods ahead of the listing; a doc-only tidy when someone next
   touches 8.2.
2. **The touch happens on a message re-ack too.** D40 only needs it for polled rows; `reack` is one path for both
   and the write is harmless for a redelivered message (spec 4.4 S32 says "outbox rows unchanged", which holds).
3. **Order: source ack, then the touch.** A crash between them leaves `updated_at` old and the next poll re-checks
   once more, which is the safe direction (one extra download, never a missed check). The touch is not under
   `ledger(...)` because it creates no deliveries and wakes nobody.
4. **Test infrastructure, not spec:** `JdbiStateStoreTest`'s container gets `withStartupTimeout(4 min)`. The
   faststart image needed 94 s to say `DATABASE IS READY TO USE!` on this workstation today and the module's
   `LogMessageWaitStrategy` default is 60 s; three runs timed out before the change. `withStartupTimeoutSeconds` is
   the JDBC-side field `OracleContainer` ignores. `AcceptanceFixture`'s container still has the 60 s default; out of
   this ticket, same one-liner if M1/M2 hit it.

**Size:** production 15 lines across `Seams.kt`, `JdbiStateStore.kt`, `TransferPipeline.kt` (7 of them the `reack`
doc comment); tests +58 across the pipeline test, the contract, the fake and the Oracle test's timeout.

**For the next ticket:** `AcceptanceFixture`'s Oracle startup timeout (deviation 4); the spec 8.2 listing lag
(deviation 1).
