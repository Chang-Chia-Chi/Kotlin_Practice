# 27: Fix: ledger timestamps do not depend on the JVM's default zone

**What to build:** Every instant the Oracle state store writes (`updated_at`, `created_at`,
`next_attempt_at`, `source_mtime`, `stored_mtime`, `delivered_at`) reads back as the same instant
whatever the process's default time zone, including during a DST fall-back hour. Today
`Timestamp.from` is bound through the default zone into a TIMESTAMP without zone, so with
Europe/Berlin as default a row updated at 00:30Z on 2026-10-25 reads back one hour off; reconciliation
(`unlisted`), the stuck gauge, the re-check window and mtime identity matching all read those
columns. Review finding B6.

**Blocked by:** 26 (Fix: two parents may expand a child with the same name, size and mtime), because both change the Oracle store

**Nature:** adapter correctness at the JDBI edge

**Status:** ready-for-agent

- [ ] `JdbiStateStoreTest` (Oracle): with the default zone set to Europe/Berlin for the test, a row STORED at 2026-10-25T00:30Z is found by `unlisted(olderThan = 01:30Z)` and reads back exactly; red before the fix; the default zone is restored after the test
- [ ] Binding and reading go through UTC explicitly (a UTC `Calendar` on every bind and read, or `TIMESTAMP WITH TIME ZONE` columns; the first needs no DDL change and is preferred); the choice is a decision entry
- [ ] The in-memory store is unaffected, and `StateStoreContract` stays green on both
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>`; every new configuration knob lands in the YAML
grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in spec Sec 14.2; append
a progress entry to docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement forces it,
docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim. Run the Oracle
class with `-DexcludedGroups=none`.
