# 40: Standards hygiene from the four-axis review

**What to build:** The small, independent breaches the standards axis found, each fixed where it lives, no
behaviour change except where a swallowed cancellation is concerned. Review findings Standards 3, 6, 7, 8,
9, 10 and the RAII note.

**Blocked by:** None (can start immediately); must NOT touch `TransferPipeline.kt`, `ShuttleHost.kt`,
`RouteRunner.kt`, `Notifier.kt`, `ProcessingChain.kt`, `Processors.kt`, `Commands.kt`, `Rules.kt`'s
rule 7/14 bodies or `ShuttleConfig.kt` beyond `Delivery.kt`, which other tickets of this round own

**Nature:** hygiene

**Status:** ready-for-agent

- [ ] `ShuttleQuarkusTest` drops `org.hamcrest.Matchers` (banned library): RestAssured assertions become `.extract().path(...)` plus JUnit `assertEquals`
- [ ] `NatsChannel`'s `runCatching { runInterruptible(io) { message.inProgress() } }` no longer swallows `CancellationException`: rethrow it, catch only what the broker can throw; a test on the channel (fake connection or the existing `nats`-tagged class) proves a cancelled in-progress loop ends promptly
- [ ] Dead or half-wired knobs: `DeliveryPolicy.fullJitter` and `.timeout` either reach the YAML loader with a rule and are read by the delivery path, or are deleted from the DSL and the data class; `ProcessorSpec.Custom.config` reaches the bean it names (spec 6.2) or the decision to drop it is recorded; `S3Target.clock` is removed if unused
- [ ] Parse-don't-validate: `MappingRow.field` becomes the `Field` type end to end (no name round trip); `Expand.format` becomes the enum beside `ExtractFrom`; YAML and DSL keep their spelling
- [ ] `MappingRendererTest` and `ProcessingChainTest` use `runTest`, not `runBlocking`
- [ ] `ArchitectureTest`: the five adapter-package rules (yaml, s3, http, nats, jdbi) get a subject check like core, quarkus and sftp, so a rename cannot pass silently; `allowEmptyShould(true)` is gone where a subject exists
- [ ] Progress entry appended listing each item as done or deliberately skipped with a reason

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
