# 13: Graceful shutdown

**What to build:** Closing the connector stops new work, lets in-flight leases finish within a bound, cancels
what remains through the cancellation ladder, and leaves every entry closed and no partial file
on disk, all within drainTimeout plus cancelGrace.

**Blocked by:** 08 (Cancellation ladder), 12 (Watch)

**Status:** ready-for-agent

- [ ] close() is a suspend function with the phases Closing, cancel watchers, drain, force, stop housekeeper
- [ ] During Closing, acquire fails fast with PoolExhausted(closing = true)
- [ ] Unacked files at shutdown are treated as nacks with redelivery
- [ ] I9: close() returns within drainTimeout + cancelGrace and every entry ends Closed
- [ ] S9: shutdown during a download leaves no partial file and releases the lease
- [ ] sftp_pool_evicted_total{reason=shutdown} counts closed entries
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`; every new configuration knob lands in the DSL block for its area with
build-time validation; every new meter uses the names fixed in spec Sec 13; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation.
