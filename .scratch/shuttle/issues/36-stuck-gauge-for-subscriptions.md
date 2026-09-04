# 36: The stuck-transfer gauge is refreshed for subscribed routes and has a default

**What to build:** Spec 11 says `shuttle_stuck_transfers{route}` is refreshed at every `PollCompleted`
and every `sweepEvery` for subscriptions, with `stuckAfter` defaulting to three intervals. Today only the
runner's poll completion refreshes it, so a subscribed route's gauge stays at 0 for ever, and a route that
states no `stuckAfter` gets no gauge at all because the knob is nullable with no default. Review finding
Spec 5.

**Blocked by:** None (can start immediately)

**Nature:** metrics wiring; one default

**Status:** ready-for-agent

- [ ] `RouteRunnerTest` (or `RouteSupervisorTest`): a subscribed route on the virtual clock refreshes `shuttle_stuck_transfers` every `sweepEvery` without any poll event; red before the fix
- [ ] `stuckAfter` defaults per spec 11 (three poll intervals for a polled route; three `inProgressEvery`, or the value spec 11 names, for a subscribed one); the default lives in one place (the data class or the loader, matching the module's convention, progress 02) and rule 7 still refuses zero
- [ ] The refresh for subscriptions is scheduled where the route's lifetime already is (the runner or supervisor), not a new scope; it stops with the route
- [ ] Spec 11's sentence and 13.1's `stuckAfter` comment state the default; the YAML grammar and DSL need no new key
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
