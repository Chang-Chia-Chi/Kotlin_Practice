# 46: One place renders and delivers a moment

**What to build:** "Read the row, render `bodies[channel]`, call `channel.deliver`, classify the outcome"
is written twice: in the notifier's worker and in the pipeline's callback ack. The pipeline carries
`bodies`, `channels`, `renderer` and `providerExists` only for that callback and the attribute-freeze check.
After this ticket a concrete deliverer in `core` holds the channels, the mapping tables and the renderer,
and answers two questions: "deliver this transfer's moment on this channel" and "can this route's tables
render these attributes". The notifier keeps its loop, policy and in-flight set; the pipeline holds the
deliverer alone; the host builds one deliverer per process. Constructor parameter counts on the pipeline
go down by at least three. Plan 2.4 forbids a `Notifier` interface; the deliverer is a class. Review
finding Architecture C3.

**Blocked by:** 45 (notifications carry the stored name and digest), because both change what the notifier renders

**Nature:** deepening; two callers, one implementation

**Status:** ready-for-agent

- [ ] A `DelivererTest` on the fakes proves render-and-deliver for one moment on one channel (rendered body, outcome classification for Delivered, Retry, Reject, an exception) and the attribute-freeze check; red against a class that does not exist
- [ ] `Notifier` and `TransferPipeline` call the deliverer; the pipeline loses `bodies`, `channels`, `renderer`, `providerExists` (or whichever of those only the deliverer needs); every existing test keeps its id and passes, in particular S30 on fakes and the MDC tests of ticket 33 (the deliverer wraps its call in the MDC context the notifier used to)
- [ ] `ShuttleHost` builds one deliverer; ticket 35's `bodies` map moves into it
- [ ] Both acceptance classes run green
- [ ] Progress entry appended, naming the parameters removed; decision D56 if a behaviour changed, none if pure structure

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
