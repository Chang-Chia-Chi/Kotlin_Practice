# 57: Glossary renames

**What to build:** The code and tests use CONTEXT.md's words and none of its "Avoid" words.
A mechanical rename pass with no behaviour change: the MULTI/EXEC state in the connection
handler and its test are named for the **batch**, not "transaction"; the lock verbs carry a
**lease**, not a "ttl", and the lock's deadline is named for the lease; the latch's
description and tests do not say "barrier" (that is the engine's word for a parked
partition); the CP gRPC message types are **replies**, not "responses". Wire-visible names
(RESP command spellings, protobuf field names already on disk or on the wire) stay as they
are; the progress entry lists any name that could not change and why.

**Blocked by:** None (can start immediately)

**Nature:** vocabulary, no behaviour change (Opus)

**Status:** done (DynaCache 7dabeb41 plus merge fix 41d94892, merged into misc/ai_gen)

- [x] A search of DynaCache main and test sources for "transaction", "barrier" (outside the
      engine's parked-partition sense), "expiresAt" on the lock, "ttl" on the lock verbs, and
      "Response" on CP message types finds nothing
- [x] Every test passes unchanged in count and name except the renamed batch test
- [x] The progress entry lists each rename and any wire-visible name deliberately kept
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200 to 600 lines including tests;
JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named
tests keep their names, constraint tests `C<n>_<description>`, invariant tests
`I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before green,
one slice at a time, `codebase-design` vocabulary for any new interface, and a `code-review`
self-pass before the commit; append a progress entry to docs/dynamiccache/progress.md
describing what was done and every deviation. The glossary is DynaCache/CONTEXT.md, the plan
is docs/dynamiccache/plan.md and this ticket's entry is
docs/dynamiccache/plans/p6-review-fixes.md. Modify only DynaCache/ and, when a measurement
forces it, docs/dynamiccache/.
