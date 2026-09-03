# 09: Declarative HTTP channel

**What to build:** A downstream endpoint is described in the DSL and driven by the JDK HTTP client: method, URL,
headers, auth from an environment secret, timeout, a body built from the fixed event
vocabulary as a Jackson tree, a response section naming success and retry statuses and a JSON
pointer to the request id. Every attempt logs its file id, channel, attempt, status and
reference; the outcome carries the reference for the ledger.

**Blocked by:** 01 (Walking skeleton)

**Nature:** adapter work

**Status:** ready-for-agent

- [ ] Against a loopback JDK `HttpServer`: 200 with the pointer resolving yields Delivered with the reference; 200 without it yields Delivered with a null reference and a WARN; 503 and 429 yield Retry; 400 yields Reject; connection refused and a stall past the timeout yield Retry
- [ ] A file name containing quotes and backslashes is escaped correctly in the body
- [ ] `CancellationException` is never converted into an outcome
- [ ] Every DSL knob of spec Sec 7.5 validates at build time; a secret is read from the environment, never from the config value
- [ ] `java.net.http` and Jackson serialization appear only in the http package
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>` and scenario tests named by their `S<n>` id; every new configuration knob
lands in the DSL with build-time validation; every new meter uses the names fixed in spec Sec 13;
append a progress entry to docs/sftpingest/progress.md describing what was done and every
deviation. The spec is docs/sftpingest/spec.md and the plan is docs/sftpingest/plan.md; the
spec wins over this ticket when they disagree, unless the progress log records a deliberate
deviation. Modify only sftp-ingest/ and, when a measurement forces it, docs/sftpingest/.
