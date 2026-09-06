# 61: SET NX and SET XX on cp: keys

**What to build:** The Redis lock idiom works on the CP namespace: `SET cp:ref:x v NX PX 30000`
sets the reference only if it is absent, with a lease measured on log time, in one committed
entry; `SET ... XX` sets only if present. CP spec 1 names `SET NX PX` on `cp:*` as in scope
and 9.5 puts `SET` in the compat set, yet today the dispatcher answers `-NOTCP` to any `SET`
carrying `NX` or `XX` on a `cp:` key, the error reserved for commands outside the compat set.
Decision, taken in the 2026-09-06 review: the compat `SET NX`/`XX` re-targets to the
conditional form of the SET verb of whichever kind the key names (counter or reference), the
state machine applies the condition and the TTL atomically in that one entry, and the reply
is Redis's `+OK` or nil. Exposing the condition on the `CP.LONG.SET`/`CP.REF.SET` syntax is
free; the compat path is what is fixed.

**Blocked by:** None (can start immediately)

**Nature:** routing and conditional-set semantics, C16, I21 (Opus)

**Status:** ready-for-agent

- [ ] `compat_set_nx_on_ref_key_acquires_once`: N clients race `SET cp:ref:lock v NX`;
      exactly one gets `+OK`, the rest nil, and the reference holds the winner's bytes
- [ ] `compat_set_nx_px_expires_on_log_time`: the lease runs on log time and the key is
      absent after a tick past it, so a second `SET NX` then succeeds
- [ ] `compat_set_xx_on_missing_key_is_nil` and `compat_set_xx_on_present_key_replaces`
- [ ] The same four behaviours on a `cp:counter:` key with a numeric value
- [ ] `I22_namespaces_never_cross` and every existing dispatcher and compat test pass
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
