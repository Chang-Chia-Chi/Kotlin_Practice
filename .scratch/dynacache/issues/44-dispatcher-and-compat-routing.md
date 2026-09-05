# 44: Command dispatcher, Redis-compat routing, RESP verbs

**What to build:** `CommandDispatcher`, a concrete class with the `CommandEngine` shape that
applies the three routing rules of CP spec 9.5 in order and submits to the AP router or the CP
engine; it routes and never translates. The ticket 13 parser is extended with every `CP.*` verb
of CP spec 6 as `Command.Cp` variants; the compat set on `cp:*` keys (`SET` with `NX`, `EX`,
`PX`, `GET`, `DEL`, `EXISTS`, the `INCR` family, `SETEX`, the TTL commands, `TYPE`) is handled
by the CP engine on the ordinary `Command` variants; `-NOTCP` for every rejection; the Netty
pipeline submits to the dispatcher; the CP error kinds of CP spec 6.8 are `Reply.Error` kinds.

**Blocked by:** 13 (Command parser and Netty server), 42 (Semaphore, latch, reference), 43 (gRPC CpService and RaftService)

**Nature:** routing, C16, C22, I22 (Opus)

**Status:** ready-for-agent

- [ ] CP spec 10.8 all five dispatcher tests
- [ ] `long_redis_compat_incr`, `I22_namespaces_never_cross`
- [ ] `C16_ap_engine_never_sees_cp_key` with the recording fake engine
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec-cp.md, the plan is docs/dynamiccache/plan.md and this ticket's
entry is docs/dynamiccache/plans/p5-cp-subsystem.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only DynaCache/ and,
when a measurement forces it, docs/dynamiccache/. DynaCache/ is its own git repository; commit
code there and docs in the parent.
