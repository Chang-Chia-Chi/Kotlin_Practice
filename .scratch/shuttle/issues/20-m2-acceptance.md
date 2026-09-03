# 20: Milestone 2 acceptance run

**What to build:** The image-sets route runs end to end on NATS, MinIO, the embedded SSHD as the partner server
and a loopback HTTP server, S27 to S30 named by id, with open items 9 and 10 of spec Sec 17
re-checked and the spec amended wherever a measurement contradicts it.

**Blocked by:** 15 (M1 acceptance), 16 (NATS channel), 17 (Expand), 18 (SFTP target), 19 (Notification moments and callback)

**Nature:** diagnosis work

**Status:** ready-for-agent

- [ ] One suite covers S27 to S30 end to end, each named by id
- [ ] Spec Sec 17 items 9 and 10 re-checked; each closed or left open with what is missing
- [ ] Every behaviour that differs from the spec is a recorded deviation with a decision entry
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
