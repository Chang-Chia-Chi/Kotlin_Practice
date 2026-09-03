# 06: Relay: pending deliveries become channel calls

**What to build:** Pending delivery rows turn into channel calls through a cold flow with a bounded buffer and
parallel workers: delivered rows record their reference and flip the transfer to DONE when
every channel has delivered, retryable outcomes schedule the next attempt with exponential
backoff and jitter, rejected or exhausted deliveries become FAILED without touching the
transfer, an ack wakes the relay immediately and a sweep catches everything else. Cancelling
the relay leaves in-flight rows PENDING and its in-flight set empty.

**Blocked by:** 02 (Test kit)

**Nature:** concurrency work

**Status:** ready-for-agent

- [ ] `I3`, `I4`, `I5`, `I13` as named tests
- [ ] S7, S8, S9, S17 against the fakes
- [ ] A wake causes a select before the sweep interval elapses on the virtual clock
- [ ] Backoff follows spec Sec 7.2: exponential from the base, capped, jittered; `maxAttempts` and `giveUpAfter` both flip a delivery to FAILED with the `gave_up` outcome
- [ ] Cancellation mid-delivery leaves the row PENDING and the in-flight set empty
- [ ] Meters `sftp_ingest_delivery_total`, `sftp_ingest_delivery_seconds`, `sftp_ingest_outbox_pending`, `sftp_ingest_outbox_oldest_seconds`, `sftp_ingest_relay_inflight`
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>` and scenario tests named by their `S<n>` id; every new configuration knob
lands in the DSL with build-time validation; every new meter uses the names fixed in spec Sec 13;
append a progress entry to docs/sftpingest/progress.md describing what was done and every
deviation. The spec is docs/sftpingest/spec.md and the plan is docs/sftpingest/plan.md; the
spec wins over this ticket when they disagree, unless the progress log records a deliberate
deviation. Modify only sftp-ingest/ and, when a measurement forces it, docs/sftpingest/.
