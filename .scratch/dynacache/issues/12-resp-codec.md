# 12: RESP2 codec

**What to build:** An incremental RESP2 decoder (partial frames resume) for arrays of bulk
strings and the inline form, an encoder from `Reply` to bytes, the Redis error prefix
conventions (`-ERR`, `-WRONGTYPE`, `-EXECABORT`), and a seeded fuzz test. No socket yet;
ticket 13 puts Netty in front.

**Blocked by:** 01 (Skeleton)

**Nature:** codec, C8 at the byte level (Opus)

**Status:** ready-for-agent

- [ ] `resp_encode_decode_roundtrip`, `resp_bulk_string_nil`, `resp_error_format`, `resp_inline_command`
- [ ] `resp_fuzz_no_crash`: 10,000 seeded byte sequences, every one an error or a valid parse
- [ ] `C8_reply_bytes_match_redis`: a golden table of reply bytes from real Redis for every reply type
- [ ] Progress entry appended

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
