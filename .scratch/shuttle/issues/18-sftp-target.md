# 18: SFTP target

**What to build:** Copies land on a partner SFTP server through the connector: upload to a partial name, rename
over the final name with the connector's overwrite policy so exactly one copy exists at the
key, verify by stat, probe through the connector's startup probe.

**Blocked by:** 13 (SFTP poll source); connector ticket 07 (write path) merged

**Nature:** adapter work

**Status:** ready-for-agent

- [ ] The shared target contract test class passes against the SFTP target on the embedded SSHD
- [ ] `I6` on SFTP: a crash between upload and rename is repaired by the next store
- [ ] Verify of a removed file is false; probe fails on a missing directory
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
