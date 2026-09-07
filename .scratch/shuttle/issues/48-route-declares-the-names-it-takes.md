# 48: A polled route declares the names it takes

**What to build:** A route's `poll:` block declares the file names it takes and the names it will
not, in the YAML grammar and in the Kotlin DSL, and passes them to the connector, which does the
filtering inside its own listing (sftp-connector ticket 26). Shuttle declares; the connector
enforces. It cannot be the other way round: a filter applied after the source hands an event over
has already paid for the listing place, the readiness stats, the in-flight slot and possibly the
download, which is the entire cost the filter exists to avoid.

The two patterns go on `Source.Poll`, not on the SFTP store. Filtering by name is what any polled
listing wants, and the config vocabulary already admits a poll whose store is S3 (rule 12 branches
on it). `SftpPollSource` is the only poll source that exists today, so it is the only one that
honours them; an S3 poll source, if one is ever built, owns honouring the same field.

**Blocked by:** sftp-connector ticket 26 - the connector's patterns have to exist before a route
can pass any down

**Nature:** a configuration knob, one rule, and a passthrough

**Status:** ready-for-agent

- [ ] `Source.Poll` carries the two patterns, both unset by default, so every existing document
      loads and behaves exactly as before
- [ ] Both land in the YAML grammar and in the Kotlin DSL, as every new knob must
- [ ] Rule 27 refuses a pattern that does not compile, naming the route and the pattern; the message
      says which of the two it is
- [ ] `RulesTest`: `rule27_<description>`, red before the rule exists
- [ ] `SftpPollSource` passes both through to the connector's polling configuration and adds no
      filtering of its own - a second filter in shuttle would be the after-the-fact one this ticket
      exists to avoid
- [ ] A route declaring an include takes only the matching files end to end, through the existing
      poll tier rather than a new harness
- [ ] shuttle spec: the `poll:` block's grammar gains the two keys with their semantics stated once,
      pointing at the connector spec's 7.4 for the matching rules rather than keeping a second copy
      of them that can drift
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
