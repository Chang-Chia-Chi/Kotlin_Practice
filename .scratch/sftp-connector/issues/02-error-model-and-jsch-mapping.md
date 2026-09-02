# 02: Error model and JSch message mapping

**What to build:** Every failure the transport can raise arrives at callers as one of the sealed error classes
from spec Sec 10, with endpoint, operation, path and attempt in the message. Status-code errors
map by code; free-text JSch errors map by a maintained table; anything unmapped becomes an
Unknown error that keeps the raw text, is recoverable and poisons, logs at WARN and increments
the unmapped counter, so a new wording is visible in production the first time it occurs.

**Blocked by:** 01 (Walking skeleton)

**Status:** ready-for-agent

- [ ] Sealed hierarchy exactly as spec Sec 10.1, each class carrying a poisons flag where applicable
- [ ] Mapper is one class; a table entry exists for at least: auth fail, unknown host key, connect timeout, socket timeout, session down, proxy failure, channel not opened
- [ ] One embedded-server test per table row triggers the real condition (wrong password, stalled response, killed session, closed proxy port) and asserts the class
- [ ] Unmapped message maps to Unknown with the raw message preserved, WARN logged, sftp_error_unmapped_total incremented
- [ ] CancellationException is never wrapped
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`; every new configuration knob lands in the DSL block for its area with
build-time validation; every new meter uses the names fixed in spec Sec 13; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation.
