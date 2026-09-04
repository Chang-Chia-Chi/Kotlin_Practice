# 15: Milestone 1 acceptance run

**What to build:** The vendor-drop and mirror routes run end to end on real adapters: the embedded SSHD,
Testcontainers MinIO with versioning, Testcontainers Oracle, and a loopback HTTP server, every
scenario S1 to S26 named by id, the load scenario at ten times today's volume, and the open
items of spec Sec 17 re-checked with the spec amended wherever a measurement contradicts it.

**Blocked by:** 14 (Quarkus host)

**Nature:** diagnosis work

**Status:** done

- [x] One suite covers S1 to S26 end to end, each named by id
- [x] S13 at 5,000 files of 10 MB: all DONE, in-flight never above parallelism, staging bounded, no skipped poll at the next tick (measured at a scaled-down 200 files x 64 KiB; full-scale 50 GB does not fit this disk, extrapolation recorded as an open item)
- [x] Spec Sec 17 items 1 to 8 and 11 re-checked; each closed or left open with what is missing
- [x] Every behaviour that differs from the spec is a recorded deviation with a decision entry (D43)
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
