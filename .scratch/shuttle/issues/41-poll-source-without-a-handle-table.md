# 41: `SftpPollSource` keeps no handle table either

**What to build:** ticket 31 left one map standing in the poll source - a path-to-`FileSeen` handle
table - because the connector could not answer "which listed file is in flight at this path" and
answering it by `stat`-then-`download` would have lost the size check against the listing (progress
31 deviation 1, approved by the coordinator). Connector ticket 25 answers it:
`SftpSource.inFlightAt(path: String): SftpEvent.FileSeen?` returns the exact `FileSeen` instance the
watch emitted for the file in flight at that path, or null when nothing is in flight there -
including the gap between a tick admitting a file and handing it over, and after a watch's end has
given the file back. Acking through the looked-up handle and through the emitted one is one action
plus one ignored second answer. This ticket deletes the table: the `ConcurrentHashMap` and its
import, the `put` on hand-over, `answering`'s `finally`, and `fetch` becomes
`source.inFlightAt(path) ?: throw IOException(...)` naming the path - the stage error the pipeline
charges an attempt for.

**Blocked by:** connector ticket 25 merged into `misc/ai_gen` (`SftpSource.inFlightAt`, spec 7.1 and
7.3, D50). Its progress entry's "for the next ticket" paragraph names exactly this work.

**Nature:** deletion; the connector's contract does the work

**Spec changes this ticket applies first:** none. Spec 4.6 and D1 already say the in-flight set is
the connector's one truth; connector spec 7.3 already carries `inFlightAt`. This ticket only stops
shuttle keeping a second copy of it.

- [x] `SftpPollSourceTest` proves a fetch after the handle was looked up downloads with the
      connector's size check intact - a file re-dropped with a different size between listing and
      fetch is refused, not stored under the old identity - and that a fetch for a path with nothing
      in flight is an `IOException`
- [x] No map and no `ConcurrentHashMap` import in `SftpPollSource`; `emitted` per tick is the only
      state left
- [x] Every `S<n>`, `SPEC1` and `B1` test keeps its id and passes
- [x] The KDoc's "handle table" paragraph and progress 31's deviation 1 are superseded in this
      ticket's progress entry
- [x] Default tier green; `M1AcceptanceTest` green
- [x] Progress entry appended

**Status:** done

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>`; every new configuration knob lands in the YAML
grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in spec Sec 14.2; append
a progress entry to docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement forces it,
docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
