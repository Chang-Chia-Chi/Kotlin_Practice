# 21: Fix: the poll source's give-back never hands one file over twice

**What to build:** Under a backlog (pipelines slower than `every`, parallelism 1 is enough), every polled
file that reaches DONE in the ledger has actually been moved or deleted on the server, and the
connector's in-flight slot for it is released. Today the D40 give-back (`releaseAbandoned`) nacks a
file whose pipeline has only just started, the next tick re-lists it, the in-flight map keyed by
remote path is overwritten with the new hand-over, the old pipeline fetches on the new slot and acks
the old one ("already settled", nothing moved), and `answering` removes the new entry, which is never
settled: the ledger says DONE, the file stays in the drop directory and is never listed again until
restart, and after `maxInFlight` such files the route hands over nothing. Review finding B1; the
`ponytail:` record on `releaseAbandoned` saying "nothing is lost" is wrong for this interleaving.

**Blocked by:** None (can start immediately)

**Nature:** concurrency and state, the in-flight map's identity

**Status:** ready-for-agent

- [ ] A test on the embedded SSHD reproduces the interleaving (three files, `every` 200 ms, parallelism 1, a state store whose `find` delays longer than `every`, `RouteRunner.run` over `source.events()`), asserting every DONE row's file is gone from the drop directory and a fresh watch lists nothing; it is red before the fix and green after
- [ ] A pipeline's fetch and ack act on the hand-over that launched it, never on a later hand-over of the same path (key the map by the hand-over, or refuse a second entry for a path still in flight, or fail the old fetch when its own entry is gone; choose the shape that keeps the connector's slot accounting exact)
- [ ] The existing `SftpPollSourceTest` cases stay green, in particular the D40 give-back and the run-ends-gives-back-everything cases
- [ ] The `ponytail:` comment and progress 13's deviation 4 are corrected to what is now true
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>`; every new configuration knob lands in the YAML
grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in spec Sec 14.2; append
a progress entry to docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement forces it,
docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
