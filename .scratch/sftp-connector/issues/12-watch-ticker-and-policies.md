# 12: Watch ticker, overlap policy and error policy

**What to build:** A pipeline can collect one long-lived flow that polls every interval and keeps going through
every recoverable failure, skipping ticks while the breaker is open or a previous tick is still
running, and terminates only on a fatal error. The consume helper acks when the block returns
and nacks when it throws.

**Blocked by:** 10 (Poll), 11 (Resilience)

**Status:** ready-for-agent

- [ ] watch(dir, every) repeats poll on a ticker driven by the injected clock
- [ ] OverlapPolicy SKIP emits PollSkipped(Overlap) while a tick runs (S8); PROCEED runs a second tick alongside
- [ ] Recoverable errors emit PollFailed and the flow continues; fatal errors terminate the flow with the error (I10); PoolExhausted emits PollFailed (S4); breaker open emits PollSkipped(BreakerOpen)
- [ ] A second watch on the same directory of the same connector is rejected at call time
- [ ] consume(dir, every) { } acks on normal return and nacks on exception
- [ ] Demo against the embedded server: the watch survives a server restart between ticks and terminates on a wrong password
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`; every new configuration knob lands in the DSL block for its area with
build-time validation; every new meter uses the names fixed in spec Sec 13; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation.
