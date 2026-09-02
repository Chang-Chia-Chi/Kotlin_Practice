# 01: Walking skeleton: one session through the transport seam

**What to build:** A developer can build the new connector module, point it at an SFTP server through an HTTP
CONNECT proxy setting, open one session, resolve a path and close, all through the transport
interface rather than JSch types. The embedded MINA SSHD testkit exists from day one so every
later ticket has a real server to test against, and the ArchUnit boundary exists so nothing
above the transport package can ever import JSch and nothing in core can import Quarkus.
Includes the minimal DSL: endpoint with optional proxy, password auth, host key policy with
the accept-all warning at startup.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Maven module builds inside the parent reactor with the pinned mwiede JSch, coroutines, resilience4j-kotlin, micrometer-core and slf4j-api as core dependencies
- [ ] Transport interface with connect, realpath and close; JSch adapter is the only implementation and the only package importing JSch
- [ ] Testkit module starts an embedded Apache MINA SSHD on loopback with a temp-directory filesystem and password auth
- [ ] A test opens a session against the embedded server, runs realpath, closes; the session's reader thread is gone afterwards
- [ ] ArchUnit tests: core never imports Quarkus; only the JSch transport package imports com.jcraft
- [ ] DSL builds an immutable config; AcceptAll host key policy logs a warning at build time and is not the default
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`; every new configuration knob lands in the DSL block for its area with
build-time validation; every new meter uses the names fixed in spec Sec 13; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation.
