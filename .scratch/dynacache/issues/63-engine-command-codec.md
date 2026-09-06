# 63: The engine's command codec encodes every keyed command

**What to build:** The engine owns one encoding of a command as bytes, total over every keyed
AP command, reads included, conditions and TTLs included, and it round-trips. Today the WAL
codec encodes only what it logs, the server has its own RESP re-encoding for forwards, and the
cluster test kit has a third partial copy that throws on verbs it lacks. This ticket is the
expand step of that consolidation: the WAL codec becomes the engine's command codec with
full coverage; the WAL's "what is logged is what changed" decision (skip errors and refused
conditionals, decided NX/XX, TTL as an absolute instant) becomes one named function from
(command, reply, now) to the command to log, used by the WAL exactly as before. Nothing else
changes yet; forwards move to bytes in ticket 64 and replicates in ticket 65.

**Blocked by:** 48 (WAL logs nothing for a refused conditional ZADD)

**Nature:** codec, C8 and C14 at the byte level (Opus)

**Status:** done (DynaCache 50fb069f, merged into misc/ai_gen)

- [x] `command_codec_round_trips_every_keyed_variant`: one test whose exhaustive `when` over
      the command hierarchy stops the build when a variant is added without a codec case
- [x] Reads, conditional writes with their condition, and TTLs as both duration and instant
      round-trip byte-exact
- [x] The what-changed function has its own tests, including the refused conditional `ZADD`
      of ticket 48 and the refused conditional `SET`
- [x] Every existing WAL, recovery and fsync test passes unchanged; `wal_reads_append_nothing`
      still holds
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200 to 600 lines including tests;
JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named
tests keep their names, constraint tests `C<n>_<description>`, invariant tests
`I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before green,
one slice at a time, `codebase-design` vocabulary for any new interface, and a `code-review`
self-pass before the commit; append a progress entry to docs/dynamiccache/progress.md
describing what was done and every deviation. The spec is docs/dynamiccache/design-spec.md,
the plan is docs/dynamiccache/plan.md and this ticket's entry is
docs/dynamiccache/plans/p6-review-fixes.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/.
