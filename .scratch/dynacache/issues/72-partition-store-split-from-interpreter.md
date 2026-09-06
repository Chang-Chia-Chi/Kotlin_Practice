# 72: The partition's store is split from its command interpreter

**What to build:** Memory accounting and eviction have a test surface of their own. Today the
partition is one 770-line unit holding the command interpreter, the entry store, expiry, the
timer wheel, byte accounting and the eviction policy; the accounting is re-run after every
command because aggregates mutate in place, and a drift in used bytes is observable only
through `INFO`. After this ticket a kind-agnostic store module owns entries, expiry, the
wheel, accounting and the policy behind a small interface (get, put, forget, expire-at,
account, evict-until), the interpreter calls it and never touches used bytes, and an
invariant test proves that used bytes equals the sum of entry sizes after any sequence of
commands. The interpreter's command `when` stays where it is.

**Blocked by:** None (can start immediately)

**Nature:** eviction and accounting, spec 2.7, 5.4, 5.5 (Opus)

**Status:** ready-for-agent

- [ ] `store_used_bytes_equals_sum_of_entries_after_any_sequence`: a seeded random sequence
      of writes, deletes, expiries and evictions over every value kind keeps the invariant
- [ ] `eviction_never_evicts_the_key_being_written` and the TinyLFU admission tests run
      against the store directly, not through the 1200-line engine test
- [ ] The interpreter has no reference to used bytes; every existing engine, eviction, expiry
      and P1 acceptance test passes unchanged
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket; this one may reach 800 lines
including tests because it moves a store out of a large unit, but no further; JUnit 5 +
Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named tests keep
their names, constraint tests `C<n>_<description>`, invariant tests `I<n>_<description>`;
Matt Pocock `tdd` at the seams the plan entry names, red before green, one slice at a time,
`codebase-design` vocabulary for any new interface, and a `code-review` self-pass before the
commit; append a progress entry to docs/dynamiccache/progress.md describing what was done and
every deviation. The spec is docs/dynamiccache/design-spec.md, the plan is
docs/dynamiccache/plan.md and this ticket's entry is docs/dynamiccache/plans/p6-review-fixes.md;
the spec wins over this ticket when they disagree, unless the progress log records a
deliberate deviation. Modify only DynaCache/ and, when a measurement forces it,
docs/dynamiccache/.
