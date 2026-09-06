# 15: Lua scripting

**What to build:** `EVAL` on LuaJ with `os`, `io`, `math.random`, `math.randomseed`,
`require`, `load` and `dofile` removed from the globals; `redis.call` raising and
`redis.pcall` returning an error table; `KEYS` and `ARGV`; the type conversion rules of spec
5.7 in both directions; the script runs inside `atomically` over its `KEYS`; no state
survives between calls. `EVALSHA` is out of scope.

**Blocked by:** 14 (MULTI, EXEC, DISCARD)

**Nature:** sandbox and bridge, C11, C12, I10 (Opus)

**Status:** done (DynaCache 03d3708, merged into misc/ai_gen)

- [x] `lua_redis_call`, `lua_keys_argv`, `lua_cross_partition_rejected`, `lua_no_side_effects`
- [x] `lua_deterministic`: two engines with the same state give the same result
- [x] `C11_clock_and_random_unavailable`, `lua_type_conversion_table`
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
measurement forces it, docs/dynamiccache/.
