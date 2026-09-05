# 20: `FileGone` goes; `consume` knows whose verdict an exception is

**What to build:** Two findings from the T17-follow-up spec sweep, both in the source's contract
with its consumer.

`FileGone` is emitted only when the slot is already settled GONE at the instant `emit(FileSeen)`
returns (`SftpSource.kt:313-314`). Under `poll` that is after the consumer's block ran. Under
`watch` the tick's flow is collected into a channel, so `emit` returns when the receiver takes the
element and before the consumer downloads; the check loses the race every time. Both FileGone
tests use `poll`. The one consumer ignores the event and reads `download()` returning null
instead, which is the signal every path already has. Delete the event rather than fix it.

`consume` nacks on any exception out of the block (`SftpSource.kt:188-193`). When `download()`
itself throws `PoolExhausted`, `CircuitOpen` or `SessionLost`, the file is filed under `onNack`
- moved to `failed/` in the usual layout - although nobody said it failed; the spec's own reasoning
for cancellation applies.

**Blocked by:** 19

**Model:** Fable 5.1 - settlement paths under `NonCancellable`

**Status:** done

**Spec changes this ticket applies first:**

- 7.1: remove `FileGone` from the event list. State the contract in its place: a file listed and
  then absent at download time is answered by `download()` returning null, its place in the
  in-flight set already given back, and no event follows.
- 8.2: the sentence "yields `FileGone`, not an error" becomes "the download answers null, not an
  error".
- 7.2: which exceptions are the consumer's verdict. An exception the connector itself raised - any
  `SftpException` - out of the block is not a nack: the file's slot is released with redeliver so
  the next tick lists it again, the ack action does not run, and the exception still goes to the
  error policy as today. Any other exception out of the block is the consumer's verdict and nacks
  as today.

- [x] `SftpEvent.FileGone` deleted, with the `emit` at `SftpSource.kt:314` and every `is FileGone`
      branch; the `consume` log line "ignored, given back as gone" for a null download goes with it,
      because a null download is the normal answer, not something to warn about
- [x] The two FileGone tests are rewritten to assert the contract that replaces it: `download()`
      answers null and `sftp_inflight` returns to zero. Their names change; their scenario ids stay
- [x] `consume`: an `SftpException` out of the block releases the slot with redeliver, runs no
      action, rethrows to the error policy; test with the fake transport refusing the download
      with `PoolExhausted` and `SessionLost`, then asserting the file is listed again next tick and
      nothing moved to the nack target
- [x] `consume`: a consumer's own exception still nacks; existing test unmodified
- [x] Shuttle compiles: the one `is SftpEvent.FileGone -> Unit` branch in
      `shuttle/src/main/kotlin/infra/shuttle/sftp/SftpPollSource.kt` is removed. That is the only
      shuttle edit; the Shuttle orchestrator has been told. Then run shuttle's default tier in the
      worktree (`mvn -B -o -q -pl shuttle test`, about 90 s) and put the counts in the progress
      entry; `ShuttleQuarkusTest` (port 8081) and `ShuttleHostTest`'s two readiness cases may
      flake under parallel builds - rerun those alone and say so
- [x] Progress entry appended; the open-seams row "`FileGone` is an event of the live poll only"
      struck through as closed by deletion

Ground rules for every ticket: implement only this ticket; fixes are scoped to what a finding
names and carry the test that found it; no Thread.sleep; `@Test fun x() = runBlocking<Unit>`;
invariant tests named `I<n>_<description>`; never weaken an earlier ticket's test - a finding that
a test is wrong is reported, not silently corrected; comments and messages carry reasons, never
spec section numbers; append a progress entry describing what was done and every deviation. The
spec is docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation. Work in an isolated worktree branched from
`misc/ai_gen`; modify only `sftpconnector/`, `docs/sftpconnector/`, and the one shuttle line named.
