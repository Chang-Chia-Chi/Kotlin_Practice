# 60: CP.LONG.GETADD

**What to build:** `CP.LONG.GETADD K d` answers the counter's old value and adds `d`, in one
committed entry, as CP spec 3.2 and 6.2 define. It was deferred by T38 and never picked up:
no verb, no wire tag, no parser row exists. After this ticket the verb goes through the
parser, the wire, the CP engine and the AtomicLong state machine, answers `:old`, and takes
part in the linearizability checking the chaos test already does for the other counter verbs.

**Blocked by:** None (can start immediately)

**Nature:** command semantics, CP spec 3.2 (Opus)

**Status:** done (DynaCache f1e1d816, merged into misc/ai_gen)

- [x] `long_getadd_returns_old_value_and_adds`: on a missing key answers `:0` and leaves `d`;
      on an existing value answers it and adds
- [x] `long_getadd_concurrent_linearizable`: N clients each GETADD 1 concurrently; the
      returned old values are a permutation of 0..N-1 and the final value is N
- [x] The verb round-trips through the CP wire encoding and is rejected with `-NOTCP` on a
      non-`cp:` key like every other CP verb
- [x] A `-CAPACITY` style limit is not built; the progress entry notes the deferral stays
      recorded in ticket 62
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200 to 600 lines including tests;
JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named
tests keep their names, constraint tests `C<n>_<description>`, invariant tests
`I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before green,
one slice at a time, `codebase-design` vocabulary for any new interface, and a `code-review`
self-pass before the commit; append a progress entry to docs/dynamiccache/progress.md
describing what was done and every deviation. The spec is docs/dynamiccache/design-spec-cp.md,
the plan is docs/dynamiccache/plan.md and this ticket's entry is
docs/dynamiccache/plans/p6-review-fixes.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/.
