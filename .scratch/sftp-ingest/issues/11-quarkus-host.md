# 11: Quarkus host: startup, readiness, admin, ordered shutdown

**What to build:** The application runs: producers in the documented order, properties mapped onto the DSL,
startup checks that fail loudly on a missing table or bucket, the staging directory wiped,
readiness true only once every check passes and every route is up, the five admin endpoints
under the admin role, and a shutdown from the Quarkus event that stops routes, drains the
connector, cancels the relay leaving rows PENDING, and closes the clients, all within the
drain timeout.

**Blocked by:** 05 (Crash matrix), 06 (Relay), 07 (Oracle ledger), 08 (S3 target), 09 (HTTP channel), 10 (Connector binding)

**Nature:** shutdown ordering and timing work

**Status:** ready-for-agent

- [ ] `I12`: close returns within the drain timeout with a delivery parked in a stalled loopback server, and every PENDING row stays PENDING
- [ ] S15 and S18
- [ ] A boot with a missing table fails naming the DDL; a boot with a missing bucket fails naming the bucket
- [ ] Readiness is false until spec Sec 11.1 steps 1 to 6 pass and false again when a route is down
- [ ] The five endpoints of spec Sec 12.3 answer under the admin role; both re-drive endpoints change the ledger and the delivery one wakes the relay; the deliveries endpoint lists channel, state, attempts, last status, reference and delivered time per transfer
- [ ] Blocking calls run on the module's bounded IO dispatcher; metrics appear in the host registry
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>` and scenario tests named by their `S<n>` id; every new configuration knob
lands in the DSL with build-time validation; every new meter uses the names fixed in spec Sec 13;
append a progress entry to docs/sftpingest/progress.md describing what was done and every
deviation. The spec is docs/sftpingest/spec.md and the plan is docs/sftpingest/plan.md; the
spec wins over this ticket when they disagree, unless the progress log records a deliberate
deviation. Modify only sftp-ingest/ and, when a measurement forces it, docs/sftpingest/.
