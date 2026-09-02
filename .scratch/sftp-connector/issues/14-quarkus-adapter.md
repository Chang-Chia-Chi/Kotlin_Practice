# 14: Quarkus adapter

**What to build:** A Quarkus host gets a configured connector injected from application properties, its
meters in the host's Micrometer registry, and a clean shutdown on the Quarkus shutdown event,
without the core module knowing Quarkus exists.

**Blocked by:** 09 (Startup probe), 13 (Graceful shutdown)

**Status:** ready-for-agent

- [ ] Separate module depending on core; CDI producer builds the connector through the DSL from mapped configuration
- [ ] Shutdown event calls close() under drainTimeout
- [ ] Host MeterRegistry is bound; the pool gauges appear in the host's metrics endpoint
- [ ] A Quarkus test boots against the embedded server, polls once, shuts down cleanly
- [ ] ArchUnit in core still passes: no Quarkus import in core
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`; every new configuration knob lands in the DSL block for its area with
build-time validation; every new meter uses the names fixed in spec Sec 13; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation.
