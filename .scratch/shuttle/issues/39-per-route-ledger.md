# 39: One per-route ledger owns each transition and its deliveries

**What to build:** "Write ACKED, create the route's acked delivery rows, wake the notifier" is assembled
in three modules today: the pipeline, the reconciler in the runner, and the operator ack in the host, each
from `route.notify`, `DeliveryRequest` and a `wake` lambda. Ticket 06 wrote "lift `ledger` out if a third
caller appears"; the operator ack was that caller. After this ticket a concrete per-route ledger over
`StateStore` holds the route's notify list and the wake, and offers the fetched, stored and acked
transitions (plus the re-ack touch of ticket 23); the pipeline, the reconciler and the operator ack call it
and hold neither `wake` nor request-building. The `StateStore` seam is unchanged; the ledger is a class in
`core`, not an interface (plan 2.4). Review finding Architecture C2 (Strong), Standards duplication.

**Blocked by:** 33, 34, 35, 36, 38 (every other ticket that edits the pipeline, the runner or the host), because this one touches all three

**Nature:** deepening; deletion test says the three copies reappear if any one is removed

**Status:** done

- [x] A `LedgerTest` on the fakes proves each transition writes the row, creates exactly the route's rows for that moment, and wakes once; the three `wakes++` counters in `TransferPipelineTest`, `RouteRunnerTest` and `ShuttleHostTest` collapse to assertions on the ledger's behaviour or are deleted
- [x] `TransferPipeline` loses `wake` and the request-building; `RouteRunner`'s reconciliation and `ShuttleHost.ack` call the ledger; constructor parameter counts go down, not up
- [x] Every existing `I<n>_`, `S<n>_`, `B<n>_` and `SPEC<n>_` test stays green with its id; the crash matrix is unchanged
- [x] The M1 and M2 acceptance classes are run once (`-DexcludedGroups=none`) and stay green
- [x] Progress entry appended, naming the constructor parameters removed

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
