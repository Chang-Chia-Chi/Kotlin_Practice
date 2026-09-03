# 17: Expand, fetch and parent completion on fakes

**What to build:** The image-sets route runs end to end against the test kit: a message names a metadata file,
the route fetches it from a store, expand reads the listed paths and fetches each child through
the context, children are stored in parallel, the parent is UPLOADED when the last child is,
the message is acked once, and downstream is told once.

**Blocked by:** 08 (Crash matrix), 11 (S3 target), 16 (NATS channel)

**Nature:** state machine work

**Status:** ready-for-agent

- [ ] S27, S28, S29 on fakes with the scripted fetcher; `I16` as a named test
- [ ] Expand reads paths from a metadata file and from the message; `extract` with `from: message` sets attributes from the message
- [ ] A child failing five times fails the parent and the message is not acked; a re-drive of the parent replaces its children
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
