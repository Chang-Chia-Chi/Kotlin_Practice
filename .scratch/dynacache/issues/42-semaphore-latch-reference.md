# 42: Semaphore, CountDownLatch, AtomicReference

**What to build:** `SemaphoreStateMachine` (INIT, ACQUIRE, RELEASE, AVAILABLE, DRAIN; permits
owned per session and released by `SESSION_CLOSED`), `CountDownLatchStateMachine` (SET, DOWN,
GET, RESET only at zero) and `AtomicReferenceStateMachine` (SET, GET, CAS on byte equality,
TTL through log time).

**Blocked by:** 41 (Sessions)

**Nature:** three state machines with sequential specs, I21 (Opus)

**Status:** ready-for-agent

- [ ] CP spec 10.3 all six semaphore tests
- [ ] CP spec 10.4 all four latch tests
- [ ] CP spec 10.5 all three reference tests, `I21_concurrent_cas_exactly_one_wins`
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
when a measurement forces it, docs/dynamiccache/.
