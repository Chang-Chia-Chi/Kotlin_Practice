# 07: Oracle ledger adapter over JDBI

**What to build:** The ledger runs on a real Oracle: the DDL of spec Sec 5.1 as reference text, every ledger
method as one transaction, the due-deliveries select bounded and skip-locked and excluding the
given ids, and the unlisted query as one statement. The same contract test class that proves
the in-memory ledger proves this one.

**Blocked by:** 01 (Walking skeleton)

**Nature:** adapter work

**Status:** ready-for-agent

- [ ] `LedgerSchema.DDL` matches spec Sec 5.1 verbatim
- [ ] The shared ledger contract test class passes against both the in-memory ledger and the JDBI ledger on Testcontainers Oracle, tagged `oracle` and excluded by a pom property like the sibling modules
- [ ] `I11` on Oracle: a failing delivery insert rolls back the ACKED update
- [ ] A unique-identity violation on `seen` returns the existing row instead of failing
- [ ] `due` excludes the given ids, honours the limit, and uses a skip-locked select
- [ ] JDBI and `java.sql` appear only in the jdbi package
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>` and scenario tests named by their `S<n>` id; every new configuration knob
lands in the DSL with build-time validation; every new meter uses the names fixed in spec Sec 13;
append a progress entry to docs/sftpingest/progress.md describing what was done and every
deviation. The spec is docs/sftpingest/spec.md and the plan is docs/sftpingest/plan.md; the
spec wins over this ticket when they disagree, unless the progress log records a deliberate
deviation. Modify only sftp-ingest/ and, when a measurement forces it, docs/sftpingest/.
