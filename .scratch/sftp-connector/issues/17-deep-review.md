# 17: Deep review: six perspectives by a session that wrote none of it

**What to build:** Not a code-review pass. A multi-perspective read of the whole connector by a
fresh context, after ticket 16's harness has shaken it. Each perspective is a separate subagent
with one lens, reporting findings with file:line, severity, and a failing test or reproduction;
the ticket owner adjudicates, fixes every Critical and High in its own commit with the test
that found it, and reconciles the open-seams table so nothing correctly deferred is forgotten.
Operational-realism reviews keep finding critical issues that correctness passes miss, which is
why there are six lenses and not one.

**Blocked by:** 16 (Pressure)

**Status:** done

- [x] **Concurrency**: every `Mutex`/`synchronized`, every `NonCancellable`, every `StateFlow`
      write (the undispatched-collector hazard on the seams table), every `catch (Throwable)` vs
      `Exception`, every entry to the bounded dispatcher without a pool place held
- [x] **Resource lifecycle**: sockets, JSch reader threads, `.part` files, caller `InputStream`s
      (`writeFrom` leaves them open by design - is that documented at every call site?),
      coroutine scopes; for each: who creates, who closes, on which exit paths
- [x] **Security**: the traversal class generally - any server-supplied string reaching a `Path`,
      a log format string, or a shell; password and host key material in logs and exception
      messages; `AcceptAll`; whether `Attempt` messages can carry credentials
- [x] **Operational readability**: every log line and exception message read cold at 3am - does it
      name endpoint, operation, path, attempt and remedy in its own words; every meter - is
      absent-versus-zero documented for the lazily registered counters
- [x] **Failure semantics**: every `catch` - which classes, what disposition, is a
      `CancellationException` ever wrapped or swallowed; every retry site against spec Sec 6.1's
      per-operation table; the `NoSuchFile`-before-retry rule for S5
- [x] **Spec conformance**: `mattpocock-skills:code-review` spec axis over the whole module against
      spec plus the recorded deviations - every D-number has code behind it; every seam on the
      table is closed or still honestly open with an owner
- [x] Six reports in `docs/sftpconnector/review/`, each finding with file:line, severity and a
      reproduction; Critical/High fixed in their own commits with tests; Medium fixed or recorded
      as a seam with an owner; Low listed
- [x] Spec Sec 16 open items re-checked; measurements that contradict the spec recorded and raised
      to the coordinator with a proposed decision entry
- [x] Final progress entry: what the build is, what it is not, and what the maintainer must decide
      before production - `Fingerprint` pinning, the `reason=poisoned` label covering cut sessions,
      ack-wait, and anything this review adds to that list

Ground rules for every ticket: implement only this ticket; fixes are scoped to what a finding
names and carry the test that found it; no Thread.sleep; invariant tests named
`I<n>_<description>`; never weaken an earlier ticket's test - a finding that a test is wrong is
reported, not silently corrected; append a progress entry describing what was done and every
deviation. The spec is docs/sftpconnector/spec.md and it wins over this ticket when they
disagree, unless the progress log records a deliberate deviation.
