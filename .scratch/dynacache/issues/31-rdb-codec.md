# 31: RDB codec

**What to build:** A streaming writer and reader for `[header][entry]*[checksum]` with
entries `[key_len][key][type][dvv][ttl_abs][value_bytes]` for String, Hash, List and Sorted
Set; the DVV encoding shared with ticket 21; expired entries skipped at write time against the
injected clock; the checksum verified at read.

**Blocked by:** 07 (Sorted Set), 09 (TTL commands), 21 (Dotted Version Vectors)

**Nature:** codec, spec 2.8 RDB format (Opus)

**Status:** ready-for-agent

- [ ] `rdb_save_restore_roundtrip` at codec level: every type, TTLs and DVVs intact
- [ ] `rdb_excludes_expired`, `rdb_bad_checksum_rejected`, `rdb_truncated_file_rejected`
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p4-persistence.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/.
