# 16: NATS channel: subscribe trigger and publish

**What to build:** A NATS subject can trigger a route and receive notifications: a JetStream subscription
mapped onto route events with ack, term and nak, a stable message identity, publish as a
delivery, and the message view the message-extraction processor reads.

**Blocked by:** 03 (Test kit)

**Nature:** adapter work

**Status:** ready-for-agent

- [ ] A message becomes one `Seen` with working ack and nak; a nak redelivers; term stops redelivery
- [ ] Identity per spec Sec 5.2 is stable across a redelivery
- [ ] A publish lands on the subject and returns the sequence as the reference; a broker outage ends with route down
- [ ] Tests tagged `nats` on Testcontainers; jnats appears only in the nats package
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`; every
new configuration knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every
new meter uses the names fixed in spec Sec 14.2; append a progress entry to
docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when
they disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and,
when a measurement forces it, docs/shuttle/.
