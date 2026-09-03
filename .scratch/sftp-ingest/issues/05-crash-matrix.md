# 05: Crash matrix replay

**What to build:** A restart at any point converges. For each hook point of spec Sec 4.4 the pipeline is
cancelled there, a second poll runs from the same in-memory ledger and target, and the
end state matches the table: at most one extra upload, at most one extra delivery per channel,
never a lost file. Any fix the replay forces in the pipeline or consumer lands here and is
recorded.

**Blocked by:** 04 (Route consumer)

**Nature:** state machine reasoning

**Status:** ready-for-agent

- [ ] `I8` as one named test per spec Sec 4.4 row, each asserting the end state, the upload count and the delivery count the row promises
- [ ] S2, S3, S4, S5, S6 by id
- [ ] A crash after the move and before the ACKED write is repaired by reconciliation on the second poll, not by the pipeline
- [ ] Every deviation the replay forced is in the progress entry
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>` and scenario tests named by their `S<n>` id; every new configuration knob
lands in the DSL with build-time validation; every new meter uses the names fixed in spec Sec 13;
append a progress entry to docs/sftpingest/progress.md describing what was done and every
deviation. The spec is docs/sftpingest/spec.md and the plan is docs/sftpingest/plan.md; the
spec wins over this ticket when they disagree, unless the progress log records a deliberate
deviation. Modify only sftp-ingest/ and, when a measurement forces it, docs/sftpingest/.
