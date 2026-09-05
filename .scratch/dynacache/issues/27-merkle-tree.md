# 27: Merkle tree per vnode range

**What to build:** `MerkleTree` built from the (key, value hash, DVV) triples of one vnode
range in key order with a fixed fan-out, root and level hashes, and `diff(other)` yielding
the divergent leaf ranges and their keys; deterministic across nodes with identical data.

**Blocked by:** 21 (Dotted Version Vectors)

**Nature:** deterministic structure, C6 (Opus)

**Status:** ready-for-agent

- [ ] `C6_identical_data_identical_root`, `merkle_one_changed_key_changes_root`
- [ ] `merkle_diff_names_only_divergent_ranges`, `merkle_empty_range_has_stable_root`
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p3-fault-tolerance.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only DynaCache/ and,
when a measurement forces it, docs/dynamiccache/.
