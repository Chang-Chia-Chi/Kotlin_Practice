# 03: Test kit: fakes, scripted source, hook driver

**What to build:** Every later ticket proves its behaviour with no socket, no container and no connector: an
in-memory state store with the real transaction semantics, an in-memory target keeping one copy
per key, a recording channel, a scripted source that plays objects and poll boundaries and
records every ack and nack, a scripted fetcher, a fake process context over a temp directory, a
hook driver that suspends a pipeline at any named point and cancels it there, and a clock fixture.

**Blocked by:** 01 (Skeleton)

**Nature:** concurrency work in the hook driver; the rest is scaffolding

**Status:** ready-for-agent

- [ ] In-memory state store implements every method of spec Sec 8.2 with the atomicity of the transitions that create delivery rows, records every call, and has its own test
- [ ] In-memory target returns a fresh reference per store, keeps exactly one copy per key, answers verify, and has its own test
- [ ] Recording channel returns scripted outcomes and records every event; scripted source emits a route-event flow from a script covering objects, poll completion with and without truncation, poll failure and route down, recording every ack and nack
- [ ] Fake process context allocates staged files in a temp directory and detects a processor writing into an input
- [ ] Hook driver demonstrably suspends, resumes and cancels a sample coroutine at a named point with no sleeps
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
