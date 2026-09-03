# 07: Route runner, reconciliation and supervision

**What to build:** A route's event flow is collected end to end: bounded parallel pipelines under a supervisor,
poll failures and skips counted, a complete poll repairing any transfer moved but never
recorded as acked, a truncated poll skipping that repair, and a dead route restarted by the
process with capped backoff while per-route health and the readiness rule are computed.

**Blocked by:** 06 (Transfer pipeline)

**Nature:** coroutine structure work

**Status:** done

- [x] `I19` and `I21` as named tests; S14, S16, S23
- [x] With `parallelism + 1` objects at most `parallelism` pipelines run at once on the virtual clock; a poll failure never cancels a running pipeline
- [x] Reconciliation marks ACKED exactly the STORED rows older than the poll start and absent from a complete listing, creating their acked deliveries through the same function the pipeline uses
- [x] Restart delays follow the backoff from initial to max on the virtual clock and reset after a successful trigger; both readiness rules of spec Sec 10 compute correctly
- [x] Stuck gauge refreshes at every poll completion
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`; every
new configuration knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every
new meter uses the names fixed in spec Sec 14.2; append a progress entry to
docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when
they disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and,
when a measurement forces it, docs/shuttle/.
