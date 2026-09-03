# 03: Per-file pipeline and ledger-driven entry points

**What to build:** One file goes all the way against the fakes: decide the entry point from the ledger, download,
quality check, upload with HEAD verification and version prune, ledger UPLOADED, ack, ledger
ACKED with one pending delivery per channel in one transaction. A quality failure rejects the
file without touching the store; a retryable error nacks with redelivery and counts an attempt;
the fifth failure marks the file FAILED and nacks without redelivery. A file seen again is
resumed from whatever the ledger says, including ack-only for an already uploaded file.

**Blocked by:** 02 (Test kit)

**Nature:** state machine work

**Status:** ready-for-agent

- [ ] Stages 1 to 4 of spec Sec 4.1 run in order and the staged file is deleted on success and on every failure path
- [ ] Every row of the spec Sec 4.3 entry-point table has a test, including HEAD-absent falling back to a full run and the re-ack of an ACKED or DONE file counted as `reacked`
- [ ] `I1`, `I2`, `I6`, `I7`, `I9`, `I10` and the ACKED half of `I11` as named tests
- [ ] S1, S10, S11, S12 against the fakes
- [ ] A quality Fail leaves the object store untouched, marks REJECTED and nacks with redeliver = false
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>` and scenario tests named by their `S<n>` id; every new configuration knob
lands in the DSL with build-time validation; every new meter uses the names fixed in spec Sec 13;
append a progress entry to docs/sftpingest/progress.md describing what was done and every
deviation. The spec is docs/sftpingest/spec.md and the plan is docs/sftpingest/plan.md; the
spec wins over this ticket when they disagree, unless the progress log records a deliberate
deviation. Modify only sftp-ingest/ and, when a measurement forces it, docs/sftpingest/.
