# 13: Command parser and Netty server

**What to build:** A token-to-`Command` parser for every command of tickets 02 to 09
(case-insensitive names, arity and unknown-command errors in Redis wording), and the Netty
pipeline: decoder, `engine.submit`, encoder on future completion; reply order per connection
preserved under pipelining; a `main` taking port and partition count; a test-kit RESP client
over a plain socket. The server never serializes or splits anything: it submits. If the budget
runs out, land the parser first and the pipeline as a follow-up ticket.

**Blocked by:** 09 (TTL commands), 12 (RESP codec)

**Nature:** adapter and wiring (Opus)

**Status:** done (DynaCache e30a6ef, merged into misc/ai_gen)

- [x] `server_ping_pong` over a real socket
- [x] `server_pipelined_replies_in_order` with 100 pipelined commands
- [x] `server_unknown_command_error`, `server_arity_error`
- [x] `parser_maps_every_command`: one row per command name of tickets 02 to 09
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
