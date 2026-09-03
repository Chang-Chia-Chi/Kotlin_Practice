# 08: Crash matrix replay

**What to build:** A restart at any point converges. For each hook point of spec Sec 4.4 the pipeline is
cancelled there, a second trigger runs from the same in-memory state store and target, and the
end state matches the table: at most one extra store, at most one extra delivery per channel per
event, never a lost object. Any fix the replay forces lands here and is recorded.

**Blocked by:** 07 (Route runner)

**Nature:** state machine reasoning

**Status:** done

- [x] `I8` as one named test per spec Sec 4.4 row, each asserting end state, store count and delivery count
- [x] S2, S3, S4, S5, S6 by id
- [x] A crash after the move and before ACKED is repaired by reconciliation on the second poll, not by the pipeline; the subscribe row of the table runs against the test kit's message source and is repaired by the redelivery's re-ack
- [x] Every deviation the replay forced is in the progress entry
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
