# 09: Startup sequence and probe

**What to build:** Starting a connector with a bad configuration or a server that cannot do what the pipeline
needs fails immediately with a clear error instead of an hour later at the first ack. Startup
validates the config, resolves each watched directory, creates action targets when
createActionTargets is on, renames a zero-byte marker into each action target and back, then
fills to minIdle in the background without blocking readiness.

**Blocked by:** 05 (Housekeeper), 07 (Client write path)

**Status:** ready-for-agent

- [ ] Configuration validation failures surface as ConfigurationError before any connection is opened
- [ ] Probe: realpath of each watched directory; mkdir of action targets when createActionTargets; marker rename into each target and back; startupProbe = false skips the marker rename
- [ ] A cross-filesystem action target (embedded server with a second root) fails startup with ConfigurationError (S6)
- [ ] minIdle fill runs in the background; the connector is usable before it completes
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`; every new configuration knob lands in the DSL block for its area with
build-time validation; every new meter uses the names fixed in spec Sec 13; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation.
