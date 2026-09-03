# 01: Skeleton: frozen surface, DSL, validation rules, boundary gates

**What to build:** A developer can build the new `shuttle` module inside the parent reactor and every later ticket
codes against final signatures: the five seams, every value type, the route event and fetcher,
the transfer and delivery states, the seven hook points, the Kotlin DSL producing an immutable
configuration, all twenty-five validation rules reporting by number, and the metric names.
ArchUnit fences the packages from day one. Nothing behaves yet.

**Blocked by:** None (can start immediately)

**Nature:** scaffolding, plus the rule tests

**Status:** ready-for-agent

- [ ] Maven module `shuttle` builds in the parent reactor; the core package depends only on kotlin-stdlib, coroutines, micrometer-core, jboss-logging and Jackson databind
- [ ] Every type of plan Sec 2.2 exists with the signatures of spec Sec 3.4, 5, 6.1, 6.2, 7.1, 8.2, 9.2, 9.3 and 9.6; the five seams are the only seams
- [ ] The Kotlin DSL of spec Sec 13.2 builds an immutable configuration; every rule of spec Sec 13.3 has a `rule<n>_` test that rejects a violating configuration and reports its number; `I14` asserts all rules together
- [ ] Defaults of spec Sec 9.3 and Sec 10 asserted; the spec Sec 14.2 metric-name set asserted verbatim
- [ ] ArchUnit tests state every sentence of plan Sec 2.2, including that no context object carries a logger
- [ ] `docs/shuttle/progress.md` exists in the sibling format with this ticket's entry

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`; every
new configuration knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every
new meter uses the names fixed in spec Sec 14.2; append a progress entry to
docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when
they disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and,
when a measurement forces it, docs/shuttle/.
