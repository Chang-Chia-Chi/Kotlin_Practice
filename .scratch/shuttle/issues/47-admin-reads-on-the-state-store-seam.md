# 47: The admin's reads live on the state store, and Oracle does the filtering

**What to build:** `StoreReads` is a second head of the same adapter, wired separately beside the
`StateStore` bean, and the host answers the admin endpoints by loading whole tables and filtering them in
memory (a recorded `ponytail:` ceiling). At the spec's own load (thousands of files per poll) every admin
call reads the whole ledger. After this ticket the three admin reads join the `StateStore` seam: transfers
by route, state and limit with children folded under their parents; a transfer's deliveries; one delivery
by id. Oracle answers them with `WHERE` and `FETCH FIRST`, the in-memory store answers them the same way
under the contract, `StoreReads` and the host's in-memory filtering are deleted, and `ShuttleLifecycle`
wires one bean. This reopens plan 2.3's frozen surface and spec 8.2's "deliberately lacks" sentence; the
owner has said yes, because the seam has already grown `byId`, `outboxPending` and `childrenOf` for
readers. Record it as decision D57. Review finding Architecture C5.

**Blocked by:** 45 (notifications carry the stored name and digest), because both change the `StateStore` seam and both adapters

**Nature:** seam growth by three reads; deletion of a shallow second head

**Status:** ready-for-agent

- [ ] `StateStoreContract` gains three cases (transfers by route/state/limit with children folded; deliveries of a transfer; one delivery by id), green on the in-memory store and on Oracle (`-DexcludedGroups=none -Dtest=JdbiStateStoreTest`); red on both before the methods exist
- [ ] `StoreReads` and the host's whole-table filtering are deleted; `ShuttleHost`'s admin operations call the seam; `ShuttleLifecycle` no longer needs a second bean; `ShuttleHostTest.the_admin_operations_change_exactly_what_spec_14_1_says` and `ShuttleQuarkusTest`'s endpoint tests stay green with their ids
- [ ] The whole-table views `transfers()`/`outbox()` remain only where the test kit and tests need them (progress 10 says production must not need them); if production no longer calls them, they leave the adapters and stay on the fakes
- [ ] Spec 8.2 lists the three reads; plan 2.3's frozen-surface sentence gains the exception; D57 recorded
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
