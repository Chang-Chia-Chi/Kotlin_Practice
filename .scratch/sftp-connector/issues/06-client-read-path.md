# 06: Client read path: list, stat, exists, download with staging and digest

**What to build:** A caller can stream a directory listing without materializing it, stat a path, test existence,
and download a file to a local staging directory where it is written as a partial file, checked
against the listed size, renamed atomically, and returned with its digest. An abort deletes the
partial. Every operation takes a lease from the pool and maps errors through the error model.

**Blocked by:** 02 (Error model), 04 (Lease contract)

**Status:** ready-for-agent

- [ ] list returns a cold Flow fed by the transport's per-entry callback through a bounded channel; maxEntries stops the listing early; directories are skipped by default
- [ ] download writes name.part in the staging directory, verifies byte count against the listed size, renames atomically, returns LocalFile with digest (SHA-256 default, MD5 selectable)
- [ ] Abort or failure during download deletes the partial file (I13)
- [ ] Listing 100k entries with maxEntries 1000 stops after 1000 with flat memory (S11, against the embedded server)
- [ ] Meters sftp_op_seconds{op,result}
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`; every new configuration knob lands in the DSL block for its area with
build-time validation; every new meter uses the names fixed in spec Sec 13; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation.
