# 58: One MutableClock in an engine test-jar; drop the two ModuleGraphTests

**What to build:** Every module's tests share one clock double. Today a `MutableClock` is
written four times (engine, engine persist, cluster, cp) plus a `RecordingClock`, and each
copy drifts. After this ticket the engine module publishes a test-jar carrying one
`MutableClock` (settable, tickable, recording what was read when a test needs that) and the
other three modules depend on it for tests only; the copies are deleted. The two
`ModuleGraphTest`s, which assert a module graph Maven already enforces, are deleted. This is
the prefactor for ticket 59, which needs the clock double from the server module's tests.

**Blocked by:** None (can start immediately)

**Nature:** test kit (Opus)

**Status:** done (DynaCache cab7126e, merged into misc/ai_gen)

- [x] The engine module produces a test-jar; cluster, cp and server declare a test-scoped
      dependency on it; the reactor builds offline from a clean state
- [x] Exactly one `MutableClock` class exists in DynaCache; `RecordingClock` is either folded
      into it or deleted
- [x] Both `ModuleGraphTest`s are deleted and the progress entry says why
- [x] Test count is unchanged apart from the two deleted tests; every test passes
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200 to 600 lines including tests;
JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named
tests keep their names, constraint tests `C<n>_<description>`, invariant tests
`I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before green,
one slice at a time, `codebase-design` vocabulary for any new interface, and a `code-review`
self-pass before the commit; append a progress entry to docs/dynamiccache/progress.md
describing what was done and every deviation. The plan is docs/dynamiccache/plan.md and this
ticket's entry is docs/dynamiccache/plans/p6-review-fixes.md. Modify only DynaCache/ and, when
a measurement forces it, docs/dynamiccache/.
