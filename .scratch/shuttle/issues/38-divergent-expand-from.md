# 38: An `expand.from` that names another store works, or is refused at validate time

**What to build:** Rule 14 accepts any declared store as `expand.from`, but the host never fills the
pipeline's `fetchers` map, so a route whose `expand.from` is not its `fetch.store` fails every transfer at
run time with "no fetcher for store" (ticket 17's hand-off note, never picked up). A polled route whose
`expand.from` names its own polled SFTP store can never work either: that source's fetcher only knows
files its poll handed over. After this ticket: an `expand.from` naming an S3 store other than the route's
fetch store gets a fetcher from the host (one S3 client per declaration, the bucket from a new
`expand.bucket` knob mirroring `fetch.bucket`, or the same rule-6 shape); an `expand.from` naming an SFTP
store that is not the route's `fetch.store` is refused by rule 14 at validate time with a reason, because
no by-path fetch exists there yet. Review findings Spec 9 and the bug hunt's first unconfirmed item.

**Blocked by:** 35 (host wires NATS bodies and probes fetch buckets), because both change `ShuttleHost`

**Nature:** host wiring plus one rule sharpening

**Status:** done

- [x] `RulesTest`: `rule14_` cases: `expand.from` naming an S3 store other than `fetch.store` without a bucket is a violation; naming an SFTP store other than `fetch.store` is a violation with a reason saying why; naming the fetch store itself passes as today
- [x] `ShuttleHostM2WiringTest` (or `ShuttleHostTest`): a route with `fetch.store` on one S3 store and `expand.from` on another gets a `fetchers` map with the second store's fetcher, proven through the host's `internal` seam the way the wiring test already proves `fetcherFor`; red before the fix
- [x] The new knob (if `expand.bucket` is chosen) lands in YAML, DSL, rule 14's text and spec 13.1's expand block; if the bucket is instead read from the named store's declaration, record why and add nothing
- [x] Step 3 probes the expand bucket like the fetch bucket (ticket 35's probe, extended)
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
