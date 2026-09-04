# 25: Fix: the pipeline rejects an empty final payload and a key that leaves the target directory

**What to build:** Two payload shapes the processing chain can produce today reach the target when they
must not. (a) A final payload with zero objects (an archive of directories only through `unzip`, or a
custom processor answering `Continue` with nothing) is treated as "same as before", no child is
created, and the row goes PROCESSED to ACKED: the source is moved away with no copy anywhere (I8).
(b) A resolved key containing a `..` segment (an unzip entry named `../../escaped.txt`, or a name
pattern fed such an attribute) is stored outside the target directory; on the SFTP target it lands at
the server root and `store` answers Success. Rule 13 judges the pattern, never the value. Review
findings B4 and B5.

**Blocked by:** 23 (Fix: a finished identity is re-fetched at most once per `recheckFinished`), because both change the pipeline

**Nature:** payload validation at the process-to-store step; a defensive check in the SFTP target

**Status:** done

- [x] `TransferPipelineTest`: a processor returning an empty payload ends the transfer REJECTED with a reason naming the step, nothing stored, the source not acked, and re-drive re-runs from fetch; red before the fix
- [x] `TransferPipelineTest`: a zip entry `../../escaped.txt` through the real `UnzipProcessor` ends REJECTED with both the key and the reason, nothing stored; red before the fix. Same for a parent whose expanded child key resolves with `..`
- [x] `SftpTargetTest`: `store` with a key containing a `..` segment refuses before any upload; `S3Target` needs no change (a key is opaque there) unless a test shows otherwise
- [x] Spec 6.1 or 7.1 gets one sentence for each rule, and the failure model table in spec 11 lists them as Reject
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>`; every new configuration knob lands in the YAML
grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in spec Sec 14.2; append
a progress entry to docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement forces it,
docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
