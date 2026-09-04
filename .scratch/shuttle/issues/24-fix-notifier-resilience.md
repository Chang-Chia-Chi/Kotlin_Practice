# 24: Fix: the notifier survives a state-store failure and releases its in-flight set on cancel

**What to build:** A state-store exception during a sweep or a delivery (spec 11: "state store
unavailable, any transition, as recoverable") is logged and the notifier sweeps again after
`sweepEvery`; today one `IOException` from `due`, `outboxPending`, `delivered`, `retryLater` or
`deliveryFailed` ends `Notifier.run` for the life of the process while readiness stays UP, and the
host launches it once with no supervision. Separately, a cancellation while a batch waits for a
permit leaves the unlaunched ids in the in-flight set (spec 9.5). Review findings B2 and B9.

**Blocked by:** None (can start immediately)

**Nature:** concurrency, error handling, RAII on the in-flight set

**Status:** done

- [x] `NotifierTest`: a store whose `due` throws once makes the loop log and continue; the next sweep after `sweepEvery` delivers the row; `run` has not returned; red before the fix
- [x] The same for an exception thrown by a transition after a delivery (`delivered` or `retryLater`): the row stays PENDING and is delivered on a later sweep, never twice
- [x] `CancellationException` is never caught or converted
- [x] Cancelling the notifier while a batch waits on the permit semaphore leaves the in-flight set empty; red before the fix
- [x] Whether the host should also supervise the notifier (restart with backoff like a route) is decided and recorded; if yes, it is done here with a test, if no, the reason is in the progress entry
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>`; every new configuration knob lands in the YAML
grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in spec Sec 14.2; append
a progress entry to docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement forces it,
docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
