# 07: Client write path: upload, rename with overwrite, delete, mkdir, withSession

**What to build:** A caller can upload a local file, rename with an overwrite policy that works on servers with
and without the POSIX rename extension, delete, create directories with parents, and run several
operations on one lease through withSession. This completes the operation set the source and
the startup probe need.

**Blocked by:** 06 (Client read path)

**Status:** ready-for-agent

- [ ] upload streams a local file to the remote path with overwrite flag
- [ ] rename with overwrite = true tries the rename, and on failure deletes the target then renames again; embedded-server test covers a pre-existing target
- [ ] delete, mkdir with parents, exists round-trip against the embedded server
- [ ] withSession runs the block on one lease and releases it on every exit path
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`; every new configuration knob lands in the DSL block for its area with
build-time validation; every new meter uses the names fixed in spec Sec 13; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation.
