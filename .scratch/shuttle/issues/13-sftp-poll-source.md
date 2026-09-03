# 13: SFTP poll source: the real trigger

**What to build:** A real SFTP directory feeds a route: the connector's watch is mapped onto route events with
ack and nack passed through, poll completion carries the listed identities and whether the
listing was truncated, poll failures and skips are counted, a terminated watch becomes route
down, the connector's download is the route's fetcher, and the move, delete and none ack
actions map onto the connector's actions.

**Blocked by:** 07 (Route runner); connector tickets 10 (poll, ack, nack) and 12 (watch) merged

**Nature:** adapter work

**Status:** ready-for-agent

- [ ] Against the connector testkit's embedded SSHD with the in-memory state store and target: the vendor-drop route moves a file to `temp/` only after the target holds it; the mirror route deletes after store
- [ ] A file removed between listing and fetch produces no transfer beyond SEEN and no error
- [ ] A wrong password ends the flow with route down; `idleCutoff` and readiness reach the connector's DSL
- [ ] Only the sftp package imports the connector
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
