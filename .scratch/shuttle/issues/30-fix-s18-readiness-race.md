# 30: Fix: the supervisor's restart count and the route gauge agree at every instant

**What to build:** `ShuttleHostTest.S18_a_wrong_password_leaves_the_route_down_and_restarted_with_backoff_and_the_process_alive`
passes deterministically whatever the machine load. Today it fails under load with
"all-routes-down with the only route down ==> expected: <false> but was: <true>": the test awaits
`restarts >= n` and then reads readiness, but `RouteSupervisor` increments the restart counter after
the backoff wait, at the moment the route's gauge is already 1 again, so a reader that sees the
counter can see the route up. Either the counter moves to the instant the route goes down (spec 10:
"each restart logged and counted"), or `ready()` is observed through a signal that cannot race it.
Found by ticket 24's subagent as pre-existing on `misc/ai_gen`; also observed by the orchestrator
twice on 2026-09-04 with four parallel Maven builds. `ShuttleQuarkusTest`'s port 8081 collision under
parallel worktree builds is a separate nuisance: give the test profile a random port
(`quarkus.http.test-port=0`) in the same ticket.

**Blocked by:** None (can start immediately)

**Nature:** ordering between a counter, a gauge and a readiness probe; test determinism

**Status:** done

- [x] `RouteSupervisorTest`: a test on the virtual clock that observes the restart counter and the gauge at every transition and asserts they never disagree about whether the route is down; red before the fix
- [x] `ShuttleHostTest.S18_...` passes ten times in a row under a concurrent CPU load (run it in a loop while another Maven build runs, or with `-Dsurefire.rerunFailingTestsCount=0` and a stress thread inside the test class only if no other way exists); the loop is a one-off check, not a committed test
- [x] `ShuttleQuarkusTest` binds a random port in the test profile and no longer collides with a parallel worktree build
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>`; every new configuration knob lands in the YAML
grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in spec Sec 14.2; append
a progress entry to docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement forces it,
docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
