# 44: A 5xx retries by default, the notifier batch is bounded, two dead knobs go

**What to build:** Three small correctness and hygiene items in the delivery path. (a) An `http` channel
declared without a `response:` block treats a 503 as a permanent Reject, because the default retry set is
empty; spec 9.3 leaves the default unstated. After this ticket the default classifies every 5xx and 429 as
Retry and every other 4xx as Reject, and a channel's `response:` block overrides it as today (decision
D54). (b) `notifier.batch` is unbounded and `batch + workers` above 1000 hits Oracle's IN-list limit in
`due`; rule 7 gains a ceiling with the reason. (c) `DeliveryPolicy.fullJitter` is deleted as a knob (spec
9.3 states full jitter as behaviour) and `S3Target.clock` is deleted as unused; every caller adjusts. Review
findings Standards 6 and 8 (remainder), the bug hunt's third and fourth unconfirmed items.

**Blocked by:** None (can start immediately)

**Nature:** defaults and deletions

**Status:** ready-for-agent

- [ ] `HttpChannelTest`: a channel with no `response:` block answers `Retry` to 503 and 429 and `Reject` to 400; red before the fix; spec 9.3 states the default and D54 records it
- [ ] `RulesTest`: `rule7_notifier_batch_plus_workers_stays_under_the_IN_list_limit` (or the ceiling you choose, with the Oracle limit named in the message)
- [ ] `fullJitter` gone from `DeliveryPolicy`, the DSL and the YAML grammar (a document still naming it is an unknown-key load error, as the loader treats every unknown key); the notifier always jitters
- [ ] `S3Target` loses its `clock` parameter; the host's construction follows
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
