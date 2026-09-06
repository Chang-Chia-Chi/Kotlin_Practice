# 59: Acceptance tests run on the injected clock

**What to build:** No test reads the wall clock. Today the P1 acceptance test and the CP
routing test build the AP engine on the system clock, and the P1, P2, P4 and P5 acceptance
tests busy-wait on real time; none has flaked yet, but they are the tests most likely to
flake under CI load and plan rule 1.5 forbids the pattern without a recorded deviation.
After this ticket those tests take the shared `MutableClock` from ticket 58, advance it
explicitly where they waited, and drive expiry by ticking. Where a wait is genuinely for
another thread (a Netty event loop or a Raft election) and cannot be replaced by a clock
tick, the bounded wait stays and the progress entry records it as a deviation, as the
existing CP-kit entries already do.

**Blocked by:** 58 (One MutableClock in an engine test-jar)

**Nature:** deterministic time in tests, plan rule 1.5 (Opus)

**Status:** ready-for-agent

- [ ] No test constructs `Clock.systemUTC()` or `Clock.systemDefaultZone()`
- [ ] The four acceptance tests' real-time spins are replaced by clock advances, or each
      remaining bounded wait is named in the progress entry with the thread it waits for
- [ ] Every acceptance test passes and total suite wall time does not grow
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200 to 600 lines including tests;
JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named
tests keep their names, constraint tests `C<n>_<description>`, invariant tests
`I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before green,
one slice at a time, `codebase-design` vocabulary for any new interface, and a `code-review`
self-pass before the commit; append a progress entry to docs/dynamiccache/progress.md
describing what was done and every deviation. The plan is docs/dynamiccache/plan.md and this
ticket's entry is docs/dynamiccache/plans/p6-review-fixes.md. Modify only DynaCache/ and, when
a measurement forces it, docs/dynamiccache/.
