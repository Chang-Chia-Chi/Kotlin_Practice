# 19: One file in flight per path, and a watch that ends gives back everything it handed over

**What to build:** Two things the consumer currently does for itself with its private map. First,
the in-flight set keys on path plus size plus mtime, so a file re-uploaded under a new size while
the first copy is still being worked is a second in-flight entry at the same path; the consumer
refuses it and nacks it back by hand. Second, a `watch` that is cancelled withdraws only the
files of the tick that was running; files handed over by earlier ticks stay in the set for the
life of the process and are never listed again, so the consumer nacks everything it holds in a
`finally`. Both belong to the set that owns the files.

**Blocked by:** 18, and T17's failure-semantics batch (it changes `SftpSource.watch`'s cancellation catch)

**Model:** Fable 5.1 - the in-flight set's lock and coroutine lifecycles under cancellation

**Status:** done

**Spec changes this ticket applies first:**

- 7.3: separate *identity* from *exclusivity*. Identity - what makes an ack idempotent and what a
  nack-for-good remembers - stays path plus size plus mtime. Exclusivity is the path alone: while
  any file at a path is in flight, a listing of that path admits nothing, whatever its size or
  mtime; it is handed over on a later poll once the first settles. Give the reason: a consumer
  working a file must never be racing itself on a second copy at the same name, and the second
  copy is not lost, only later.
- 7.6 and 11.2: a `watch` that ends - its collector left, it was cancelled, the connector closed -
  releases every file it handed over that is still unsettled, with redeliver, so the next watch
  or the next process lists them again. Only a tick's own cancellation used to do this, and only
  for that tick.
- 15: one decision entry for the identity/exclusivity split, naming shuttle's D2 as the
  consumer that asked. Its D-number is given in the dispatch message, not chosen here: T17
  is appending entries in parallel, so a number picked from the file races.

- [x] `InFlightSet.admit` refuses a file whose path is in flight regardless of size or mtime; the
      tick counts it neither as emitted nor as not-ready, and logs at debug that a newer file at
      the path waits for the one being worked
- [x] Test: file A at `x` handed over and unacked; a listing that shows `x` with a new size emits
      nothing for `x`; after A is acked, the next listing emits the new file
- [x] Test: identity is unchanged - acking A twice is still "already settled", and a
      nacked-for-good A is still remembered by path plus size plus mtime
- [x] A `watch` releases every unsettled slot it ever handed over when it ends, on every exit
      path: collector left, cancelled, connector closed. Redeliver, never for good
- [x] Test: two ticks hand over two files, neither acked; the watch is cancelled; the next watch's
      first tick lists both; `sftp_inflight` reads zero between
- [x] Test: an ack that arrives after the watch ended is "already settled" and moves nothing; the
      WARN says the watch had already given the file back
- [x] `InFlightSetLincheckTest` passes unmodified, and gains one operation for the path-exclusive
      admit if its model needs it to keep exploring the lock
- [x] Progress entry appended; the open-seams row "A file the consumer holds from a tick that has
      already completed stays in flight across `close()`" struck through as closed by this ticket

Ground rules for every ticket: implement only this ticket; fixes are scoped to what a finding
names and carry the test that found it; no Thread.sleep; `@Test fun x() = runBlocking<Unit>`;
invariant tests named `I<n>_<description>`; never weaken an earlier ticket's test - a finding that
a test is wrong is reported, not silently corrected; comments and messages carry reasons, never
spec section numbers; append a progress entry describing what was done and every deviation. The
spec is docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation. Work in an isolated worktree branched from
`misc/ai_gen`; modify only `sftpconnector/` and `docs/sftpconnector/`.
