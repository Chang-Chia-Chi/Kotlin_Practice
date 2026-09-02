# 05: Housekeeper, lifetime jitter, keepalive and validation on borrow

**What to build:** A session that the server or proxy has silently killed is replaced without the caller
noticing, and the pool never holds a session past its lifetime or idle limit. The housekeeper
evicts expired idle entries under the no-I/O-in-lock rule, flags expired in-use entries for
eviction on release, tops up to minIdle, and each session sends keepalives. Borrow validates
with one round trip only when the entry was idle longer than the bypass window, and recreates
only when validation fails.

**Blocked by:** 04 (Lease contract)

**Status:** ready-for-agent

- [ ] maxLifetime with per-entry uniform jitter; idleTimeout honoured only above minIdle; minIdle top-up in the background
- [ ] Validation on borrow after validationBypass via realpath; failed validation closes the entry and acquire loops with the permit held
- [ ] Keepalive set on every session at the configured interval
- [ ] Leak detection logs the acquire stack trace once a lease exceeds leakDetectionThreshold and never forces release
- [ ] DSL validation rejects keepAlive >= idleCutoff and idleTimeout >= idleCutoff (I14)
- [ ] I6: an entry past maxLifetime is closed on release, never reused
- [ ] Demo against the embedded server: kill an idle session server-side, next acquire returns a working session, sftp_pool_evicted_total{reason=validation} is 1
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`; every new configuration knob lands in the DSL block for its area with
build-time validation; every new meter uses the names fixed in spec Sec 13; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation.
