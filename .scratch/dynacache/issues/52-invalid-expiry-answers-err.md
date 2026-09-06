# 52: An invalid expiry answers -ERR, never drops the connection

**What to build:** A client that sends a zero, negative or out-of-range expiry gets the error
reply Redis gives and keeps its connection. Today the parser's time arithmetic can throw on
an overflowing `EXPIRE`, `EXPIREAT`, `SET EX/PX` or `SETEX`, the pipeline treats the throwable
as fatal and closes the socket, and a zero or negative TTL is accepted as if it were valid.
After this ticket every expiry-taking command validates its argument before any arithmetic
and answers `-ERR invalid expire time in '<command>' command` for a non-positive or
unrepresentable value; nothing in the parser can throw past the reply.

**Blocked by:** None (can start immediately)

**Nature:** command semantics, C8 (Opus)

**Status:** done (DynaCache 2e27b490, merged into misc/ai_gen; EXPIRE family accepts zero and negative per Redis, see progress)

- [x] `C8_invalid_expire_answers_err_not_disconnect`: for `EXPIRE`, `PEXPIRE`, `EXPIREAT`,
      `PEXPIREAT`, `SET EX`, `SET PX`, `SETEX`, each of zero, a negative number and
      `Long.MAX_VALUE` answers the Redis error and the next command on the same connection
      still works
- [x] A property-style loop over random large and negative arguments never sees an exception
      escape the parser
- [x] The existing RESP fuzz test is extended with expiry-taking commands
- [x] Progress entry appended

A red test for this exists in the review worktree `kp-wt/review` under the server module's
test tree (`BugHuntParserTest`); reuse it if present, otherwise rewrite it from the first
criterion. Ground rules for every ticket: implement only this ticket; 200 to 600 lines
including tests; JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected
Clock; spec-named tests keep their names, constraint tests `C<n>_<description>`, invariant
tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before
green, one slice at a time, `codebase-design` vocabulary for any new interface, and a
`code-review` self-pass before the commit; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's
entry is docs/dynamiccache/plans/p6-review-fixes.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only DynaCache/ and,
when a measurement forces it, docs/dynamiccache/.
