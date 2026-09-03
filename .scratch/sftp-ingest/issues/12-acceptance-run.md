# 12: Acceptance run: the whole scenario table end to end

**What to build:** The full scenario table runs against the real adapters: embedded SSHD, Testcontainers MinIO
with versioning, Testcontainers Oracle, and a loopback HTTP server, each scenario named by its
id, plus the load scenario at ten times today's volume with its three measurements. Every open
item of spec Sec 16 is re-checked and the spec amended with a decision entry wherever a
measurement contradicts it.

**Blocked by:** 11 (Quarkus host)

**Nature:** diagnosis work

**Status:** ready-for-agent

- [ ] One suite covers S1 to S18 end to end, each named by id
- [ ] S13 at 5,000 files of 10 MB: all DONE, in-flight never above parallelism, staging never above parallelism times 10 MB, no skipped poll at the next tick
- [ ] Spec Sec 16 items re-checked; each closed, or left open with what is still missing
- [ ] Every behaviour that differs from the spec is a recorded deviation with a decision entry
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>` and scenario tests named by their `S<n>` id; every new configuration knob
lands in the DSL with build-time validation; every new meter uses the names fixed in spec Sec 13;
append a progress entry to docs/sftpingest/progress.md describing what was done and every
deviation. The spec is docs/sftpingest/spec.md and the plan is docs/sftpingest/plan.md; the
spec wins over this ticket when they disagree, unless the progress log records a deliberate
deviation. Modify only sftp-ingest/ and, when a measurement forces it, docs/sftpingest/.
