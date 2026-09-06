# 14: MULTI, EXEC, DISCARD and `atomically`

**What to build:** `CommandEngine.atomically(keys) { ctx -> }` runs its block on the declared
keys' partition executor, rejects keys on different partitions before running, and answers
`ctx.execute` on an undeclared key with an error reply while the batch continues (CONTEXT.md
"batch"); per-connection queue state where `MULTI` buffers, a parse error while queued makes
`EXEC` reply `-EXECABORT`, `EXEC` runs the buffer in order inside `atomically` and replies an
array with per-command errors in place without rolling back, and `DISCARD` clears.

**Blocked by:** 13 (Command parser and Netty server)

**Nature:** batch semantics, C12 and I11 (Opus)

**Status:** done (DynaCache 0033f64, merged into misc/ai_gen)

- [x] `multi_exec_atomic`: a reader on the same partition sees the pre-batch or the post-batch state, never between
- [x] `multi_exec_cross_partition_rejected`, `multi_exec_hash_tags_allow_two_keys`, `discard_clears_buffer`
- [x] `I11_failing_command_does_not_undo_neighbours`
- [x] `C12_atomically_rejects_span_before_running`, `C12_undeclared_key_inside_batch_is_an_error`
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
