# 02: Engine walking skeleton and partition executors

**What to build:** GET and SET travel the real path: the engine owns a fixed number of
partitions (constructor argument), each with one JDK single-thread executor and its own store;
`Key` hashes (hash tag aware) to a partition; `submit(command)` returns a future completed on
the partition's thread; `Clock` is injected; `GET`, `SET` with `NX`, `XX`, `EX`, `PX` (expiry
kept as an absolute instant and checked lazily on access), single-key `DEL`, `EXISTS`, `TYPE`,
`PING`; `close()` shuts the executors down. The cluster module gets a one-line `await()` bridge
from the future to coroutines. See ADR 0001.

**Blocked by:** 01 (Skeleton)

**Nature:** concurrency and invariant work (Fable)

**Status:** done (DynaCache 97f5336, merged into misc/ai_gen)

- [x] `string_set_get_roundtrip`, `string_set_nx_rejects_existing`, `string_set_xx_rejects_missing`, `string_set_ex_expires` (clock advanced, never slept)
- [x] `C1_one_command_at_a_time_per_partition`: two commands on one partition never overlap and commands on two partitions may (Lincheck or an interleaving test with a blocking command)
- [x] `keys_with_same_hash_tag_share_a_partition`
- [x] `DEL`, `EXISTS`, `TYPE`, `PING` reply with Redis shapes
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p1-data-engine.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/. DynaCache/ is its own git repository; commit code
there and docs in the parent.
