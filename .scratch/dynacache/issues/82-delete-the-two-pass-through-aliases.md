# 82: Delete the two pass-through aliases

**What to build:** Two methods that exist only to call another method under a different name
are gone, and their callers name the real one. The distributed snapshot's `initiate` forwards
to `start`; the timer wheel's `reschedule` forwards to `schedule` and is called only by tests.
Neither adds a check, a conversion or a promise, so each is a name a reader has to resolve for
nothing. From the standards review's Middle Man list.

**Blocked by:** None (can start immediately)

**Nature:** vocabulary, no behaviour change (Opus)

**Status:** ready-for-agent

- [ ] Neither alias exists; every caller, tests included, names the method it meant
- [ ] Test count and every test name are unchanged
- [ ] Progress entry appended

Size budget: a few lines. If either alias turns out to carry meaning the KDoc did not state (a
different intent at a call site, say), keep it, say which and why, and delete only the other.

Ground rules for every ticket: implement only this ticket; JUnit 5 + Mockito only, no AssertJ
or MockK; no sleeps, time is an injected Clock; spec-named tests keep their names, constraint
tests `C<n>_<description>`, invariant tests `I<n>_<description>`; a `code-review` self-pass
before the commit; append a progress entry to docs/dynamiccache/progress.md describing what was
done and every deviation. The glossary is DynaCache/CONTEXT.md, the plan is
docs/dynamiccache/plan.md and this ticket's entry is
docs/dynamiccache/plans/p6-review-fixes.md. Modify only DynaCache/.
