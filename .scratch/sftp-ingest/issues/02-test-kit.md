# 02: Test kit: fakes, scripted source, hook driver

**What to build:** Every later ticket can prove its behaviour with no socket, no container and no connector. The
kit provides an in-memory ledger with the same transaction semantics as the real one, a
in-memory target that keeps exactly one copy per key, a recording channel with scripted outcomes, a scripted event
source that plays files and poll boundaries and records every ack and nack, a scripted
downloader, a clock fixture, and a hook driver that can suspend a pipeline at any named point
and cancel it there, which is how every "crash after X" scenario is played.

**Blocked by:** 01 (Walking skeleton)

**Nature:** concurrency work in the hook driver; the rest is scaffolding

**Status:** ready-for-agent

- [ ] In-memory ledger implements every method of spec Sec 5.2 with the atomicity of the ACKED and DELIVERED transitions, records every call in order, and has its own test
- [ ] In-memory target returns a fresh reference per store, keeps exactly one copy per key, answers verify from it, and has its own test
- [ ] Recording channel returns scripted Delivered, Retry or Reject per call and records every event it received
- [ ] Scripted source emits an ingest-event flow from a script covering files, poll completion with and without truncation, poll failure and route down, and records every ack and nack with arguments
- [ ] Hook driver demonstrably suspends a sample coroutine at a named point, resumes it, or cancels it there, with no sleeps
- [ ] Clock fixture over `Clock.fixed` and `Clock.offset`
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>` and scenario tests named by their `S<n>` id; every new configuration knob
lands in the DSL with build-time validation; every new meter uses the names fixed in spec Sec 13;
append a progress entry to docs/sftpingest/progress.md describing what was done and every
deviation. The spec is docs/sftpingest/spec.md and the plan is docs/sftpingest/plan.md; the
spec wins over this ticket when they disagree, unless the progress log records a deliberate
deviation. Modify only sftp-ingest/ and, when a measurement forces it, docs/sftpingest/.
