# 10: Poll with ack, nack, readiness and in-flight backpressure

**What to build:** The hourly use case works end to end on one listing: poll a directory, receive a FileSeen per
ready file with ack and nack, download, ack, and the file lands in the temp folder. Files still
being written are held back by the readiness checks, files that vanish between listing and
download become FileGone, in-flight files are never re-emitted, and the lister suspends when
maxInFlight unacked files exist.

**Blocked by:** 07 (Client write path)

**Status:** ready-for-agent

- [ ] poll returns a cold Flow of the sealed events PollStarted, FileSeen, FileGone, PollCompleted
- [ ] ack runs the ack action (Move with overwrite, Delete, Noop) and releases the slot; nack runs the nack action, releases the slot, and redelivers on a later poll unless redeliver = false
- [ ] Ack and nack each accepted once (I12); cancelling the collector releases every in-flight slot (I8); a file in the in-flight set is never emitted again (I7)
- [ ] Readiness interface plus SizeStable, MinAge, MarkerFile, AllOf; default SizeStable(2, 10s) + MinAge(1m); not-ready files counted in PollCompleted
- [ ] Action targets inside the watched directory are excluded from listing, also under recursive
- [ ] Scenarios S5 (FileGone), S7 (ack without download runs the move), S12 (same file listed while in flight emitted once) against the embedded server
- [ ] Meters sftp_poll_seconds, sftp_poll_files{state}, sftp_inflight, sftp_ack_total{outcome}
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`; every new configuration knob lands in the DSL block for its area with
build-time validation; every new meter uses the names fixed in spec Sec 13; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation.
