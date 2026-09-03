# 19: Notifications and callback acks

**What to build:** A route can tell a channel when an object was fetched or stored, not only when it is done,
each as an outbox row created in the transaction that defines the event; and a route can make a
channel call the commit action itself, synchronous and retried with the stage, so the transfer
is not acked until upstream has answered.

**Blocked by:** 09 (Notifier), 12 (HTTP channel)

**Nature:** state machine work

**Status:** done

- [x] `I20` for all three events: a delivery row exists if and only if its transition committed
- [x] S30: a callback ack returning 500 then 200 keeps the transfer STORED through the failure and ACKED after the 200, with one done delivery
- [x] A fetched delivery exists after a crash right after fetch and is delivered by the notifier
- [x] Rule 12 rejects a callback naming a channel without the notify role
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
