# 09: Notifier: pending deliveries become channel calls

**What to build:** Pending delivery rows turn into channel calls through a cold flow with a bounded buffer and
parallel workers: delivered rows record their reference and flip the transfer to DONE when
every delivery is done, retryable outcomes schedule the next attempt with backoff and jitter,
rejected or exhausted deliveries become FAILED without touching the transfer, a transaction that
creates rows wakes the notifier, a sweep catches the rest, and cancelling leaves rows PENDING
with the in-flight set empty.

**Blocked by:** 03 (Test kit), 04 (Mapping renderer)

**Nature:** concurrency work

**Status:** done

- [x] `I3`, `I4`, `I5`, `I13` as named tests; S7, S8, S9, S17, S22 on fakes
- [x] A wake causes a select before the sweep interval elapses on the virtual clock
- [x] Backoff follows spec Sec 9.3; `maxAttempts` and `giveUpAfter` both flip a delivery to FAILED with the `gave_up` outcome
- [x] Bodies are rendered at send time through the mapping renderer; cancellation mid-delivery leaves the row PENDING and the set empty
- [x] Meters of spec Sec 14.2 for deliveries, outbox and notifier in-flight
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`; every
new configuration knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every
new meter uses the names fixed in spec Sec 14.2; append a progress entry to
docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when
they disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and,
when a measurement forces it, docs/shuttle/.
