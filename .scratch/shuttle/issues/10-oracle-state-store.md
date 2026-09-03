# 10: Oracle state store over JDBI

**What to build:** The state store runs on a real Oracle: the DDL of spec Sec 8.1 as reference text, every seam
method as one transaction, the due-deliveries select bounded and skip-locked and excluding
given ids, children and parent transitions, and the unlisted query as one statement. The same
contract test class that proves the in-memory store proves this one.

**Blocked by:** 01 (Skeleton)

**Nature:** adapter work

**Status:** ready-for-agent

- [ ] The DDL text matches spec Sec 8.1 verbatim
- [ ] The shared state-store contract test class passes against the in-memory store and the JDBI store on Testcontainers Oracle, tagged `oracle` and excluded by a pom property
- [ ] `I11` and `I20` on Oracle: a failing delivery insert rolls back the transition that created it
- [ ] A unique-identity violation on `seen` returns the existing row; `due` excludes given ids, honours the limit and uses a skip-locked select
- [ ] JDBI and `java.sql` appear only in the jdbi package
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`; every
new configuration knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every
new meter uses the names fixed in spec Sec 14.2; append a progress entry to
docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when
they disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and,
when a measurement forces it, docs/shuttle/.
