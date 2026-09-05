# 18: `PollCompleted` says what is still out and whether the listing was cut short

**What to build:** The source's one consumer keeps a private map of every `FileSeen` it was handed,
because `PollCompleted` tells it neither which files are still in flight nor whether the listing
stopped at `maxFilesPerPoll`; it reads the cap out of the config and compares `seen` against it
to guess. That is a second ledger of the connector's own in-flight set (spec D14 and shuttle's D1
both say two ledgers are two truths). This ticket makes the event answer both questions. (How the cap ends the listing is T17's
lens-1 H1 fix, owned by the other session; do not touch `take()` or the listing hand-off here.)

**Blocked by:** T17 lens 3 committed on `misc/ai_gen` (it touches `SftpEvent.kt`)

**Model:** Fable 5.1 - the in-flight set's lock and a tick's end are on the escalation list

**Status:** done

**Spec changes this ticket applies first** (edit `docs/sftpconnector/spec.md` before code):

- 7.1: `PollCompleted` carries `inFlight: List<RemoteFile>` - every file the in-flight set holds
  unsettled at the instant the tick ends, this tick's and earlier ticks' alike, in listing order -
  and `truncated: Boolean`, true when the listing stopped at `maxFilesPerPoll` and the directory
  may hold more. Say in the spec's own words why a consumer needs each: a downstream ledger
  reconciles against the first, and "the drop is complete" is only claimable from the second.

- [x] `SftpEvent.PollCompleted` gains `inFlight` and `truncated`; `InFlightSet` answers "what is
      unsettled right now" under its own lock, in one call, so the tick never iterates the set
- [x] Test: a directory of N > `maxFilesPerPoll` files gives `truncated = true` and exactly the cap
      emitted; a directory of exactly the cap gives `truncated = false`
- [x] Test: files handed over on tick 1 and unacked are in tick 2's `PollCompleted.inFlight`; an
      acked file is not; a file nacked with redeliver is not
- [x] `InFlightSetLincheckTest` still passes unmodified; every earlier test unmodified
- [x] Progress entry appended, with the open-seams row "A file the consumer holds from a tick
      that has already completed stays in flight across `close()`" annotated: it is now
      observable through `inFlight`, and T19 closes it

Ground rules for every ticket: implement only this ticket; fixes are scoped to what a finding
names and carry the test that found it; no Thread.sleep; `@Test fun x() = runBlocking<Unit>`;
invariant tests named `I<n>_<description>`; never weaken an earlier ticket's test - a finding that
a test is wrong is reported, not silently corrected; comments and messages carry reasons, never
spec section numbers; append a progress entry describing what was done and every deviation. The
spec is docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation. Work in an isolated worktree branched from
`misc/ai_gen`; modify only `sftpconnector/` and `docs/sftpconnector/`.
