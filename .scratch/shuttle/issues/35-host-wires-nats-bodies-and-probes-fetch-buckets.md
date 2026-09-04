# 35: The host renders NATS notifications and probes every bucket a route reads

**What to build:** Two gaps in the composition root for subscribed routes. (a) A notification or a
callback delivered over a `nats` channel is published with an empty body: the host builds the notifier's
`bodies` map from HTTP channels only, so the NATS channel's mapping table (spec 9.6, the `body:` rows
ticket 16 loads) is never rendered. Ticket 16 handed this to ticket 14; it was not wired. (b) Startup
step 3 (spec 12.1: `probe()` each declared store) probes only route targets; a subscribe route's
`fetch.bucket` is never probed, so a missing bucket surfaces at the first message instead of ending
startup. Review findings Spec 6 and Spec 8.

**Blocked by:** None (can start immediately)

**Nature:** composition-root wiring

**Status:** done

- [x] `ShuttleHostTest` (or `ShuttleHostM2WiringTest`): a route notifying a `nats` channel with a body table delivers a rendered JSON body, not `{}`; proven through the host with the test kit's recording of what the channel was handed (a real broker is not needed if `NatsChannel` can be observed at its `deliver` input; otherwise `M2AcceptanceTest` gains the assertion under the `acceptance` tag); red before the fix
- [x] A subscribe route's `fetch.bucket` is probed at step 3 with the same HEAD the target probe uses; a missing fetch bucket ends startup naming the bucket (test beside `a_boot_with_a_missing_bucket_fails_naming_the_bucket`, with a Mockito `S3Client` whose `headBucket` throws for the fetch bucket only)
- [x] The `bodies` map is built once from every channel that carries a mapping table, whatever its kind; no per-kind branch remains in the host for it
- [x] Progress entry appended, with the S30/M2 acceptance implication stated (a callback over NATS now carries a body)

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
