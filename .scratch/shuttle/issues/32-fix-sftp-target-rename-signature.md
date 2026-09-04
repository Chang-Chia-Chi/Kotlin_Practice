# 32: Fix: the SFTP target renames with the connector's new signature and keeps its lost-reply compensation

**What to build:** `shuttle` compiles and its SFTP target still lands exactly one copy at the key. The
connector's T17 failure-semantics batch (merge 75cecb2, connector decision D46: a REPLACE rename
discriminates the old file at the destination by size and mtime) changed `SftpClient.rename` to
`rename(from, to, overwrite, listed: RemoteFile? = null)`, where `listed` is the `RemoteFile` of the
source path (or null). `shuttle/src/main/kotlin/infra/shuttle/sftp/SftpTarget.kt:79` still calls
`client.rename(partial, remote, Overwrite.REPLACE, expectedSize = size)` and no longer compiles.
Design decision (already made by the orchestrator): pass the stat of the partial (`client.stat(partial)`,
or the `RemoteFile` the upload already returned if it does) so the connector's lost-reply compensation
can recognise its own landed file; do not give the compensation up.

**Blocked by:** none

**Nature:** adapter follow-up to a connector signature change

**Status:** done

- [x] `shuttle` compiles against the reactor's connector
- [x] `SftpTargetTest` stays green (6 tests, embedded SSHD) and the contract test `ObjectStoreTargetContract` still passes on the SFTP target
- [x] One new test in `SftpTargetTest` proves the compensation: a rename whose reply is lost (use the connector's test kit hook if it offers one, otherwise a rename that is retried after the destination already holds the partial's bytes) ends with exactly one copy at the key and `store` answering success; if no such hook exists at the shuttle seam, say so in the progress entry and keep only the two above - **no such hook exists at this seam** (`LoopbackConnectProxy.onNextClientRequest` fires on the next client packet, which from outside `store` is the upload's, never the rename's); recorded in the progress entry, items 1 and 2 kept
- [x] Progress entry appended; decision recorded in spec 16 as D51 only if behaviour changed beyond the signature - it did not, so no decision
- [x] Checklist ticked, Status done

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>`; every new configuration knob lands in the YAML
grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in spec Sec 14.2; append
a progress entry to docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement forces it,
docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
