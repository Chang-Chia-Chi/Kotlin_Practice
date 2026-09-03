# 02: YAML loader and the validate function

**What to build:** The spec's YAML document becomes the same immutable configuration the Kotlin DSL builds, with
environment references resolved, durations and status ranges parsed, and every violation
reported with its rule number in one report. A pure validate function exists for the host to
wrap later.

**Blocked by:** 01 (Skeleton)

**Nature:** adapter work

**Status:** ready-for-agent

- [ ] The spec Sec 13.1 document loads, passes all 25 rules, and equals the spec Sec 13.2 DSL build for the vendor-drop route
- [ ] Rule 9 counts the poll, fetch and target roles of every route on a store, and a route without `parallelism` counts as 1
- [ ] `${VAR}` references resolve from an injected environment map; a literal secret fails rule 25
- [ ] S25: a file with five violations reports five rule numbers in one report and opens no connection
- [ ] An unknown key is an error naming its YAML path; durations like `30s`, `1h`, byte sizes like `1g`, `10g` and ranges like `[200-299]` parse
- [ ] `staging: { dir, minFree }`, route `recheckFinished` and `unzip: { maxEntries, maxBytes }` load with the spec's defaults when omitted (v0.4)
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`; every
new configuration knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every
new meter uses the names fixed in spec Sec 14.2; append a progress entry to
docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when
they disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and,
when a measurement forces it, docs/shuttle/.
