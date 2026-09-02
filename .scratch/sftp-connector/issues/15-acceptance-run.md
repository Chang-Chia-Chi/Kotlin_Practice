# 15: Acceptance run: full scenario table and optional Toxiproxy tier

**What to build:** The whole scenario table S1 to S12 runs as one suite against the embedded server on every
build, and the network faults the embedded server cannot produce (half-open connections, proxy
stalls) run through Toxiproxy via Testcontainers when Docker is available. Any behaviour that
differs from the spec is recorded as a deviation.

**Blocked by:** 14 (Quarkus adapter)

**Status:** ready-for-agent

- [ ] One suite covers S1 to S12 with each scenario named by its ID
- [ ] Toxiproxy tier for half-open connection and proxy stall, skipped with a clear message when Docker is absent
- [ ] Spec Sec 16 open items re-checked; spec amended where a measurement contradicts it, with a decision entry
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`; every new configuration knob lands in the DSL block for its area with
build-time validation; every new meter uses the names fixed in spec Sec 13; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation.
