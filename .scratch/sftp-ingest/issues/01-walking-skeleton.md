# 01: Walking skeleton: frozen surface, DSL and boundary gates

**What to build:** A developer can build the new `sftp-ingest` module inside the parent reactor and every later
ticket codes against final signatures: the five seams (ledger, target, delivery channel,
quality check, hook), the value types, the sealed ingest event and the downloader function, the
transfer and delivery states, the eight hook points, the configuration DSL with every validation
rule, and the metric names. ArchUnit fences the packages from day one so the pipeline can never
import a technology. Nothing behaves yet; the pipeline, consumer and relay are shells.

**Blocked by:** None (can start immediately)

**Nature:** adapter and scaffolding work

**Status:** ready-for-agent

- [ ] Maven module `sftp-ingest` builds in the parent reactor with kotlin-stdlib, coroutines, micrometer-core, jboss-logging and Jackson databind as the pipeline package's only dependencies
- [ ] Every type of plan Sec 2.2 exists in the pipeline package with the signatures of spec Sec 5.2, 6.1, 7.1, 7.2 and 8; the DSL uses the source and target vocabulary of spec Sec 12.1; the five interfaces are the only interfaces in the module
- [ ] The DSL of spec Sec 12.1 builds an immutable config and rejects every rule listed there; `I14` rejects an API-call timeout or a channel timeout not below the drain timeout
- [ ] Delivery policy defaults match spec Sec 7.2 and are asserted by a test
- [ ] ArchUnit tests state every sentence of plan Sec 2.2, including that only the sftp and quarkus packages may import the connector
- [ ] A test asserts the spec Sec 13 metric-name set verbatim
- [ ] `docs/sftpingest/progress.md` exists in the sibling format with this ticket's entry

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>` and scenario tests named by their `S<n>` id; every new configuration knob
lands in the DSL with build-time validation; every new meter uses the names fixed in spec Sec 13;
append a progress entry to docs/sftpingest/progress.md describing what was done and every
deviation. The spec is docs/sftpingest/spec.md and the plan is docs/sftpingest/plan.md; the
spec wins over this ticket when they disagree, unless the progress log records a deliberate
deviation. Modify only sftp-ingest/ and, when a measurement forces it, docs/sftpingest/.
