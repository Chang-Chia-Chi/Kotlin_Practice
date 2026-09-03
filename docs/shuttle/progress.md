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
