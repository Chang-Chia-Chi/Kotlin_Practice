# 11: Resilience and transparent reconnect

**What to build:** A session that dies mid-operation, a proxy that refuses for a minute, or a server that stalls
never reaches the caller as an error unless the retry budget is spent or the failure is fatal.
Operations are wrapped in Resilience4j retry, circuit breaker, bulkhead and time limiter in the
agreed order, and each operation retries on a fresh lease with its own semantics so a lost
reply to a rename or delete is not reported as a phantom failure.

**Blocked by:** 07 (Client write path), 08 (Cancellation ladder)

**Status:** ready-for-agent

- [ ] Retry, CircuitBreaker, Bulkhead, TimeLimiter from resilience4j-kotlin wrap every client operation, outermost first in that order
- [ ] Only recoverable errors are retried and counted by the breaker; fatal errors short-circuit and are not counted
- [ ] Per-operation semantics: rename retried after a lost reply succeeds when the target exists with the expected size (I11); delete treats NoSuchFile after retry as success; mkdir treats AlreadyExists as success; download restarts into a fresh partial
- [ ] Breaker open makes acquire fail fast with CircuitOpen
- [ ] Scenarios S1 (session killed mid-download, one successful download), S2 (stall poisons and retries), S3 (breaker opens then half-open probe closes it), S10 (wrong password: no retry, breaker untouched)
- [ ] Meters sftp_retry_total{op}, sftp_breaker_state
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`; every new configuration knob lands in the DSL block for its area with
build-time validation; every new meter uses the names fixed in spec Sec 13; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation.
