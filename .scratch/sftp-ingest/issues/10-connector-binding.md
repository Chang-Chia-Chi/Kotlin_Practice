# 10: Connector binding: the real source

**What to build:** A real SFTP directory feeds the pipeline: the connector's watch is mapped onto the ingest
event flow with ack and nack passed through, poll completion carries the listed identities and
whether the listing was truncated, poll failures and skips are counted, a terminated watch
becomes route down, and the connector's download is the pipeline's downloader. The route
configuration hands the temp-folder move and the readiness checks to the connector's DSL.

**Blocked by:** 04 (Route consumer); connector tickets 10 (poll, ack, nack) and 12 (watch) merged

**Nature:** adapter work

**Status:** ready-for-agent

- [ ] Against the connector testkit's embedded SSHD with the in-memory ledger and target: one poll moves a file to `temp/` only after the target holds it
- [ ] A file removed between listing and download produces no transfer beyond SEEN and no error
- [ ] A wrong password ends the flow with `RouteDown`
- [ ] Only the sftp package imports the connector
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>` and scenario tests named by their `S<n>` id; every new configuration knob
lands in the DSL with build-time validation; every new meter uses the names fixed in spec Sec 13;
append a progress entry to docs/sftpingest/progress.md describing what was done and every
deviation. The spec is docs/sftpingest/spec.md and the plan is docs/sftpingest/plan.md; the
spec wins over this ticket when they disagree, unless the progress log records a deliberate
deviation. Modify only sftp-ingest/ and, when a measurement forces it, docs/sftpingest/.
