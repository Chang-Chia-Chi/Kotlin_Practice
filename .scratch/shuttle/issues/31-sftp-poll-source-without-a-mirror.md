# 31: `SftpPollSource` keeps no ledger of its own

**What to build:** `SftpPollSource` holds a `ConcurrentHashMap<String, InFlight>` mirroring the
connector's in-flight set, reads `config.polling.maxFilesPerPoll` to guess whether a listing was
truncated, refuses a second file at a path by hand, and nacks everything it holds in a `finally`
when the route ends - two `ponytail:` ceilings and a `config` constructor parameter. Spec 4.6
("three pieces of state, each with one owner: the connector's in-flight set...") and D1 ("two
ledgers are two truths") already say this state is the connector's. Connector tickets 18 and 19
make the connector answer every question the map answered; this ticket deletes the map.

**Blocked by:** connector tickets 18, 19 and 21 merged into `misc/ai_gen` (the connector's
`PollCompleted` carries `inFlight` and `truncated`; the in-flight set is path-exclusive; a watch
that ends releases what it handed over; `ShuttleHost.sized()` is already gone)

**Nature:** deleting a mirror; the connector's contract does the work

**Status:** done

**Spec changes this ticket applies first:**

- D2: the connector's in-flight *identity* is still store, directory, name, size, mtime; its
  *exclusivity* is now the path, so a re-drop with a new mtime while the first is being worked is
  handed over on a later poll by the connector, not refused by this module.
- 4.6 `PollCompleted`: `listed` is the connector's `inFlight` plus this tick's emitted files;
  `truncated` is the connector's flag.

- [x] `SftpPollSource` has no map, no `config` parameter, no `releaseEverythingHeld`, no
      `StillInFlight`, no `Abandoned`; the `finally` and both `ponytail:` comments go
      (all gone but one path-to-`FileSeen` handle table the fetcher downloads through; nothing is
      decided from it - progress 31 deviation 1, approved by the coordinator)
- [x] `RouteEvent.PollCompleted.listed` and `.truncated` are computed from
      `SftpEvent.PollCompleted.inFlight` and `.truncated`; `emitted` per tick stays as the only
      state, reset on `PollStarted`
- [x] `SftpPollSourceTest` no longer builds a pool by hand for the tests that only needed the map;
      tests that asserted the hand-made refusal or the route-end nack are rewritten to
      assert what the connector now guarantees, through the connector's own events
      (no test built a pool for the map's sake; the wrong-password case still builds one to skip
      the start-up probe - progress 31 deviation 2)
- [x] Every `S<n>` test that drove the old behaviour still passes with its id unchanged
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>`; every new configuration knob lands in the YAML
grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in spec Sec 14.2; append
a progress entry to docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement forces it,
docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
