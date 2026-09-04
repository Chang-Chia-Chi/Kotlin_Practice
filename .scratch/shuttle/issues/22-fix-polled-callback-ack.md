# 22: Fix: a polled route with `onAck: callback` runs

**What to build:** A polled SFTP route whose ack action is `callback: <channel>` starts, transfers a file,
calls the channel before the ledger writes ACKED, and leaves the file where it is (the `none` behaviour,
D40 bounding the re-checks), exactly as spec 5.3 and ticket 19's hand-off note describe. Today
`sftpConnectorConfig`'s post-action mapping throws for `Callback`, so the host turns every start into
`RouteDown` and the route restart-loops for ever; rule 12 accepts the configuration, so validate mode
says nothing. Review finding Spec 1.

**Blocked by:** 21 (Fix: the poll source's give-back never hands one file over twice), because both change the poll source

**Nature:** adapter mapping plus the pipeline's ack order on a polled route

**Status:** done

- [x] A test on the embedded SSHD runs a polled route with `onAck: callback` through `RouteRunner` and a `RecordingChannel`: the callback is called once before the ACKED ledger write, the file stays in the drop directory, the transfer ends ACKED then DONE, and the route never goes down; red before the fix
- [x] The connector's post-action for `Callback` is what `none` does; `Seen.ack` under a callback calls no channel and only releases the connector's in-flight entry
- [x] The connector config test names the mapping (`the_ack_vocabulary_maps_onto_the_connectors_post_actions` extended or a sibling)
- [x] Progress entry appended, including whether spec 5.3 needed a sentence

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
