# 08: Cancellation ladder: cooperative abort, socket timeout, forced disconnect

**What to build:** Cancelling a coroutine that is inside a blocking JSch call stops the call in a bounded time
and leaves the pool in a known state. A cancelled transfer or listing aborts cooperatively
through the progress monitor or list selector and its session stays usable; a server that stops
responding is cut off by the socket timeout and the session is poisoned; a call that neither
tier unblocks within the grace period is force-disconnected from the cancellation handler.

**Blocked by:** 06 (Client read path)

**Status:** ready-for-agent

- [ ] Cancelling a download mid-transfer against the embedded server returns within cancelGrace, the session is validated and returned to the pool, no partial file remains
- [ ] Cancelling a listing stops the selector; the session is reused for the next operation
- [ ] A server-side stall past socketTimeout (fault hook) raises SessionLost, poisons the lease, evicts the entry
- [ ] A call stuck past cancelGrace is force-disconnected; the blocked thread returns and the entry is Closed
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`; every new configuration knob lands in the DSL block for its area with
build-time validation; every new meter uses the names fixed in spec Sec 13; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation.
