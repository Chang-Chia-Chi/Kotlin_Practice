# 71: One home for the cp: namespace rule

**What to build:** "Is this a CP key, which primitive kind owns it, and which Redis commands
may touch it" is answered in one place, beside the CP command hierarchy in the engine, and
read by the dispatcher, both CP engines and the AP engine's `-NOTCP` reply. Today that rule is
written in four modules with five separate `-NOTCP` literals, the reference prefix is known
only to the dispatcher, and the compat re-target undoes the parser (an `EXPIRE` becomes an
instant in the parser and a duration again in the dispatcher, from two clock reads). After
this ticket the parser emits the CP verb for a `cp:` key directly, the dispatcher only routes,
a kind mismatch (a counter verb on a lock key, a compat verb on a key of another kind)
answers `-WRONGTYPE` if CP spec 9.4 names it and nil as today otherwise, and the class of bug
fixed in ticket 54 cannot recur because there is one lookup.

**Blocked by:** 54 (TTL verbs on cp:ref: keys reach the reference), 61 (SET NX and SET XX on cp: keys)

**Nature:** routing, C16, C22, I22 (Opus)

**Status:** ready-for-agent

- [ ] One function from key to CP kind and one compat-set definition exist in DynaCache;
      `-NOTCP` is produced from one place
- [ ] `cp_kind_lookup_covers_every_prefix` and `compat_set_matches_cp_spec_9_5`
- [ ] The parser emits CP verbs for `cp:` keys; no instant-to-duration round trip remains
- [ ] `I22_namespaces_never_cross`, `C16_`, `C22_` and every dispatcher, compat, routing and P5
      acceptance test pass; the kind-mismatch reply is tested and its choice recorded
- [ ] Progress entry appended

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
