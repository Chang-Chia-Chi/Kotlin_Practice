# 23: Fix: a finished identity is re-fetched at most once per `recheckFinished`

**What to build:** A file that stays in the polled directory under `onAck: none` after it is DONE is
downloaded and digested at most once per `recheckFinished`, measured from the row's `updated_at`,
as spec 4.3 (D40) says. Today a re-ack writes nothing, so `updated_at` never advances and, once the
row is older than the window, the file is downloaded on every poll for the rest of its life. The
existing `D40_` test checks 23 h and 25 h and never a third poll. Review finding Spec 2.

**Blocked by:** None (can start immediately)

**Nature:** state machine; a ledger write on the re-ack path through the StateStore seam

**Status:** done

- [x] A pipeline test on the fakes polls a DONE identity three times across the window (23 h, 25 h, 26 h): the second poll re-fetches, the third does not; red before the fix
- [x] The re-ack advances the row's `updated_at` in both stores under the shared contract, without creating outbox rows or changing the state (a `reacked` transition, or the existing ack transition made idempotent; keep the seam's surface minimal and record the choice)
- [x] The `reacked` outcome counter of spec 14.2 is unchanged in meaning
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
