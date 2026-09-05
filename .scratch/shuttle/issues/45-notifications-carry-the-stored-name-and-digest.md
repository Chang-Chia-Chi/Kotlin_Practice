# 45: A notification carries the stored object's name and digest, not the source's

**What to build:** D43 recorded that the ledger's `stored_name`, `digest` and `stored_mtime` are the
fetched source object's, so a notification's `STORED_NAME` and `DIGEST` render source values even after a
rename or zip; the processed name and digest live only on the target object's metadata. Downstream is told
the wrong name. After this ticket the `stored` transition carries the processed summary (name, size,
digest, algorithm, mtime) into the ledger, the notifier renders those, and `SOURCE_NAME` and
`SOURCE_DIGEST` keep the source's values as spec 9.6 lists them. D43 is amended in place to say the fix
landed, and the acceptance suites' body assertions (M1's S20 rename-then-zip, M2's S27) are updated to
expect stored values. Review finding from ticket 15's handoff (deferred D43).

**Blocked by:** None (can start immediately); must not touch `HttpChannel.kt`, `Delivery.kt`, `Rules.kt`,
`S3Target.kt` (ticket 44) or `Commands.kt`, `YamlLoader.kt` (ticket 43) beyond what the compiler forces

**Nature:** seam change at `StateStore.stored`; state machine

**Status:** ready-for-agent

- [ ] `TransferPipelineTest`: a rename-then-zip route's `stored` delivery renders `STORED_NAME` and `DIGEST` as the archive's name and digest and `SOURCE_NAME`/`SOURCE_DIGEST` as the source's; red before the fix
- [ ] `StateStoreContract`: `stored` persists the processed summary in both adapters (in-memory and Oracle) and `byId` reads it back; the 8.1 columns already exist for it (verify against the DDL block; do not edit the block)
- [ ] `Ledger.stored` and the pipeline's call carry the summary; the crash matrix (spec 4.4 rows "store, before ledger" and "ledger STORED") is unchanged and `CrashMatrixTest` stays green with its ids
- [ ] Spec 9.6's field table and D43 say what each field now carries
- [ ] Both acceptance classes run green (`-DexcludedGroups=none -Dtest=M1AcceptanceTest,M2AcceptanceTest`)
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
