# 06: Transfer pipeline, entry points and children

**What to build:** One source object goes all the way against the fakes: decide the entry point from the state
store, fetch, process, store every object of the final payload, STORED, ack, ACKED with done
deliveries or straight to DONE when the route notifies nobody. A final payload of several
objects becomes child rows; the parent is STORED when the last child is and acked only once.
Failures count attempts and become FAILED or REJECTED with the right nack flags.

**Blocked by:** 05 (Processing chain)

**Nature:** state machine work

**Status:** ready-for-agent

- [ ] `I1`, `I2`, `I7`, `I9`, `I10`, `I11`, `I16`, `I17` as named tests
- [ ] S1, S10, S11, S12 in both halves, S19, S33 on fakes; `I24` as a named test: a finished identity returning with a different digest gets a new revision through `supersede` and the old row is untouched
- [ ] Every row of the spec Sec 4.3 entry-point table has a test, including a false verify falling back to a full run, the re-ack counted as `reacked`, and two children of one parent on one key rejecting the transfer with both paths in the reason
- [ ] Store is called exactly once per object per successful run and verify exactly once per STORED entry
- [ ] D40: a DONE identity listed again inside `recheckFinished` of its `updated_at` is skipped with no fetch and no state write; listed outside the window it is fetched and digested (S12); `recheckFinished = 0s` rechecks on every poll
- [ ] D41: with the staging volume's usable space (a function injected at the filesystem boundary, no real disk fill) below `staging.minFree`, the object is nacked with redelivery before any fetch, `attempts` is unchanged, `shuttle_staging_deferred_total` increments and `shuttle_staging_free_bytes` reads the value; above the watermark the run proceeds
- [ ] D42: a child's STORED transition and the parent's last-child flip are one call on the state store seam that the in-memory store implements without a parent-wide lock; the seam method's contract is written so ticket 10 can implement it as one child update plus one conditional parent update
- [ ] Staging is empty after success and after every failure path, including files a processor created
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
