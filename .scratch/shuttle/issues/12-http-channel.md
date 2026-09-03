# 12: HTTP channel

**What to build:** A downstream endpoint declared in the channels block is driven by the JDK HTTP client: method,
URL, headers, auth from an environment secret, timeout, the body from the mapping renderer, a
response section naming success and retry statuses and a pointer to the request id. Every
attempt logs its transfer id, event, channel, attempt, status and reference.

**Blocked by:** 04 (Mapping renderer)

**Nature:** adapter work

**Status:** done

- [x] Against a loopback JDK `HttpServer`: 200 with the pointer resolving yields Delivered with the reference; 200 without it yields Delivered with a null reference and a WARN; 503 and 429 yield Retry; 400 yields Reject; connection refused and a stall past the timeout yield Retry
- [x] Auth modes bearer, basic and header work; `CancellationException` is never converted into an outcome
- [x] `java.net.http` appears only in the http package
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`; every
new configuration knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every
new meter uses the names fixed in spec Sec 14.2; append a progress entry to
docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when
they disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and,
when a measurement forces it, docs/shuttle/.
