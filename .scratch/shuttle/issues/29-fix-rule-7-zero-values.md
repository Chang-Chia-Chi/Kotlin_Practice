# 29: Fix: rule 7 refuses the zero values that hang or loop the process

**What to build:** Validate mode and startup refuse `notifier.workers: 0` (a zero-permit semaphore parks
the notifier for ever), `notifier.sweepEvery: 0s` (a hot select loop) and `poll.every: 0s` (the
connector throws and the route goes down at every start), each with rule 7's number, as they refuse
`inProgressEvery: 0s` today. Review finding B8.

**Blocked by:** None (can start immediately)

**Nature:** validation rule coverage

**Status:** done

- [x] `RulesTest`: `rule7_` cases for each of the three, reporting rule 7 with the route or the notifier named; `violations` is empty for them today
- [x] Any other duration or count in `ShuttleConfig` whose zero would hang, loop or divide (`batch`, `parallelism`, `maxAttempts`, `restartBackoff` bounds, `checks`) is walked and either already covered by a rule (name it in the progress entry) or added under rule 7
- [x] Spec 13.3 rule 7's sentence lists what it now covers
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>`; every new configuration knob lands in the YAML
grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in spec Sec 14.2; append
a progress entry to docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement forces it,
docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
