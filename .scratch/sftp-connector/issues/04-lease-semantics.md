# 04: Lease contract, acquire timeout and poison eviction

**What to build:** A caller uses the pool through a lease: a use-block that releases exactly once on every exit
path including cancellation, a bare acquire for advanced use, a poison flag that makes the pool
close the entry on release instead of reusing it, and a bounded wait that fails with
PoolExhausted carrying active, idle and pending counts. The pool gauges are published through a
MeterRegistry seam that defaults to a simple registry.

**Blocked by:** 03 (Pool core)

**Status:** ready-for-agent

- [ ] acquire waits at most acquireTimeout then throws PoolExhausted with pool statistics in the message
- [ ] use-block releases in finally; a second release is logged as a bug and ignored
- [ ] A poisoned lease's entry transitions to Evicting on release and is closed outside the lock
- [ ] Cancellation during Connecting releases the permit and closes the half-open entry
- [ ] I3: a poisoned entry never returns to the idle deque; I4: every permit is released exactly once on every exit path
- [ ] Meters sftp_pool_active, sftp_pool_idle, sftp_pool_pending, sftp_pool_acquire_seconds, sftp_pool_acquire_timeout_total, sftp_pool_created_total
- [ ] Demo test against the embedded server: two concurrent leases hold two sessions, a third waits and times out
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`; every new configuration knob lands in the DSL block for its area with
build-time validation; every new meter uses the names fixed in spec Sec 13; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation.
