# 05: Processing chain and built-in processors

**What to build:** A payload runs through a chain of processors under the four re-run rules: inputs immutable, no
side effects outside staging, digests recomputed by the pipeline, cardinality deciding rows.
Quality, rename, zip, unzip, extraction from the file name, the source path and JSON content, and digest verification exist as
built-ins; a custom processor resolves by name; attributes freeze at the end of the chain and
every notified channel's mapping is checked against them before the store.

**Blocked by:** 03 (Test kit), 04 (Mapping renderer)

**Nature:** state machine work

**Status:** ready-for-agent

- [ ] `I15` and `I18` as named tests; S20 on fakes; S26
- [ ] Every built-in of spec Sec 6.3 except expand and extraction from a message has a test; unzip yields one object per entry; zip yields one archive created through the context
- [ ] `unzip` past `maxEntries` or past `maxBytes` uncompressed is Reject with the limit and the offending count or size in the reason, and stops reading the archive there (D41): a small archive declaring more entries than the limit is rejected without extracting them all
- [ ] A processor writing into its input is detected; a processor throwing is a retryable stage error
- [ ] Attribute limits of rule 22 enforced; `SOURCE_DIGEST` and `DIGEST` differ after zip
- [ ] The mapping check runs at attribute freeze and a missing required attribute fails the transfer before any store
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
